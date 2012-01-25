/** -*- C++ -*-
 * Copyright (C) 2011 Andy Thalmann
 *
 * This file is part of ht4c.
 *
 * ht4c is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifdef __cplusplus_cli
#error compile native
#endif

#include "stdafx.h"
#include "ThriftClient.h"
#include "ThriftNamespace.h"
#include "ThriftException.h"

namespace ht4c { namespace Thrift {

	Common::Client* ThriftClient::create( Hypertable::Thrift::ClientPtr client ) {
		HT4C_TRY {
			return new ThriftClient( client );
		}
		HT4C_THRIFT_RETHROW
	}

	ThriftClient::~ThriftClient( ) {
		HT4C_TRY {
			client = 0;
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftClient::createNamespace( const char* name, Common::Namespace* nsBase, bool createIntermediate, bool createIfNotExists ) {
		HT4C_TRY {
			std::string ns( getNamespace(name, nsBase) );
			ThriftClientLock sync( client.get() );
			if( createIfNotExists && client->namespace_exists(ns) ) {
				return;
			}
			if( createIntermediate ) {
				char* ns_tmp = strdup( ns.c_str() );
				try {
					int created = 0;
					char* ptr = ns_tmp;
					while( (ptr = strchr(ptr, '/')) != 0 ) {
						*ptr = 0;
						if( created > 0 || !client->namespace_exists(ns_tmp) ) {
							client->namespace_create( ns_tmp );
							++created;
						}
						*ptr++ = '/';
					}
					if( created > 0 || !client->namespace_exists(ns_tmp) ) {
						client->namespace_create( ns_tmp );
					}
					free( ns_tmp );
				}
				catch( ... ) {
					free( ns_tmp );
					throw;
				}
			}
			else {
				client->namespace_create( ns );
			}
		}
		HT4C_THRIFT_RETHROW
	}

	Common::Namespace* ThriftClient::openNamespace( const char* name, Common::Namespace* nsBase ) {
		HT4C_TRY {
			std::string _name( getNamespace(name, nsBase) );
			ThriftClientLock sync( client.get() );
			Hypertable::ThriftGen::Namespace ns( client->namespace_open(_name) );
			return ns ? ThriftNamespace::create( client, ns, _name ) : 0;
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftClient::dropNamespace( const char* name, Common::Namespace* nsBase, bool ifExists, bool dropTables, bool deep ) {
		HT4C_TRY {
			std::string _name = getNamespace( name, nsBase );
			ThriftClientLock sync( client.get() );
			if( dropTables || deep ) {
				if( ifExists && !client->namespace_exists(_name) ) {
					return;
				}
				Hypertable::ThriftGen::Namespace ns = client->namespace_open( _name );
				try {
					std::vector<Hypertable::ThriftGen::NamespaceListing> listing;
					client->namespace_get_listing( listing, ns );
					if( deep ) {
						drop( ns, _name, listing, ifExists, dropTables );
					}
					else {
						for( std::vector<Hypertable::ThriftGen::NamespaceListing>::const_iterator it = listing.begin(); it != listing.end(); ++it ) {
							if( !(*it).is_namespace ) {
								client->table_drop( ns, (*it).name, true );
							}
						}
					}
					client->namespace_close( ns );
				}
				catch( ... ) {
					client->namespace_close( ns );
					throw;
				}
			}
			return client->namespace_drop( _name, ifExists );
		}
		HT4C_THRIFT_RETHROW
	}

	bool ThriftClient::existsNamespace( const char* name, Common::Namespace* nsBase ) {
		HT4C_TRY {
			ThriftClientLock sync( client.get() );
			return client->namespace_exists( getNamespace(name, nsBase) );
		}
		HT4C_THRIFT_RETHROW
	}
	
	ThriftClient::ThriftClient( Hypertable::Thrift::ClientPtr _client )
	: client( _client )
	{
	}

	std::string ThriftClient::getNamespace( const char* name, Common::Namespace* nsBase ) {
		std::string ns;
		if( nsBase ) {
			ns = nsBase->getName();
		}
		if( name && *name ) {
			if( !ns.empty() ) {
				ns += "/";
			}
			ns += name;
		}
		Hypertable::Namespace::canonicalize( &ns );
		return ns;
	}

	void ThriftClient::drop( Hypertable::ThriftGen::Namespace ns, const std::string& nsName, const std::vector<Hypertable::ThriftGen::NamespaceListing>& listing, bool ifExists, bool dropTables ) {
		for( std::vector<Hypertable::ThriftGen::NamespaceListing>::const_iterator it = listing.begin(); it != listing.end(); ++it ) {
			if( (*it).is_namespace ) {
				std::string nsSubName = nsName + "/" + (*it).name;
				Hypertable::ThriftGen::Namespace nsSub = client->namespace_open( nsSubName );
				try {
					std::vector<Hypertable::ThriftGen::NamespaceListing> sub_entries;
					client->namespace_get_listing( sub_entries, nsSub ); //FIXME remove if the sub entries are available in thrift
					drop( nsSub, nsSubName, sub_entries, ifExists, dropTables ); //FIXME remove, use next line
					//drop( nsSub, nsSubName, (*it).sub_entries, ifExists, dropTables );
					client->namespace_close( nsSub );
				}
				catch( ... ) {
					client->namespace_close( nsSub );
					throw;
				}
				client->namespace_drop( nsSubName, ifExists );
			}
			else if( dropTables ) {
				client->table_drop( ns, (*it).name, true );
			}
		}
	}

} }