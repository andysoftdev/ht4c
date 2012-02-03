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
#include "HyperClient.h"
#include "HyperNamespace.h"

#include "ht4c.Common/Exception.h"


namespace ht4c { namespace Hyper {

	Common::Client* HyperClient::create( Hypertable::ConnectionManagerPtr connMngr, Hyperspace::SessionPtr session, Hypertable::PropertiesPtr properties ) {
		HT4C_TRY {
			return new HyperClient( connMngr, session, properties );
		}
		HT4C_RETHROW
	}

	HyperClient::~HyperClient( ) {
		HT4C_TRY {
			try {
				client = 0;
			}
			catch( Hypertable::Exception& exception ) {
				// ignore expired hyperspace session exception on close
				if( exception.code() != Hypertable::Error::HYPERSPACE_EXPIRED_SESSION ) {
					throw;
				}
			}
		}
		HT4C_RETHROW
	}

	void HyperClient::createNamespace( const char* name, Common::Namespace* nsBase, bool createIntermediate, bool createIfNotExists ) {
		HT4C_TRY {
			client->create_namespace( name, getNamespace(nsBase), createIntermediate, createIfNotExists );
		}
		HT4C_RETHROW
	}

	Common::Namespace* HyperClient::openNamespace( const char* name, Common::Namespace* nsBase ) {
		HT4C_TRY {
			Hypertable::NamespacePtr ns( client->open_namespace(name, getNamespace(nsBase)) );
			return ns ? HyperNamespace::create( client, ns ) : 0;
		}
		HT4C_RETHROW
	}

	void HyperClient::dropNamespace( const char* name, Common::Namespace* nsBase, bool ifExists, bool dropTables, bool deep ) {
		HT4C_TRY {
			Hypertable::Namespace* _nsBase = getNamespace( nsBase );
			if( dropTables || deep ) {
				if( ifExists && !client->exists_namespace(name, _nsBase) ) {
					return;
				}
				Hypertable::NamespacePtr ns = client->open_namespace( name, _nsBase );
				std::vector<Hypertable::NamespaceListing> listing;
				ns->get_listing( deep, listing );
				if( deep ) {
					drop( ns, listing, ifExists, dropTables );
				}
				else {
					for( std::vector<Hypertable::NamespaceListing>::const_iterator it = listing.begin(); it != listing.end(); ++it ) {
						if( !(*it).is_namespace ) {
							ns->drop_table( (*it).name.c_str(), true );
						}
					}
				}
			}
			return client->drop_namespace( name, _nsBase, ifExists );
		}
		HT4C_RETHROW
	}

	bool HyperClient::existsNamespace( const char* name, Common::Namespace* nsBase ) {
		HT4C_TRY {
			return client->exists_namespace( name, getNamespace(nsBase) );
		}
		HT4C_RETHROW
	}
	
	HyperClient::HyperClient( Hypertable::ConnectionManagerPtr connMngr, Hyperspace::SessionPtr session, Hypertable::PropertiesPtr properties )
	: client( )
	{
		HT4C_TRY {
			client = new Hypertable::Client( Hypertable::String(), connMngr, session, properties );
		}
		HT4C_RETHROW
	}

	Hypertable::Namespace* HyperClient::getNamespace( Common::Namespace* ns ) {
		if( ns ) {
			HyperNamespace* hns = dynamic_cast<HyperNamespace*>( ns );
			if( hns ) {
				return hns->get();
			}
		}
		return 0;
	}

	void HyperClient::drop( Hypertable::NamespacePtr ns, const std::vector<Hypertable::NamespaceListing>& listing, bool ifExists, bool dropTables ) {
		for( std::vector<Hypertable::NamespaceListing>::const_iterator it = listing.begin(); it != listing.end(); ++it ) {
			if( (*it).is_namespace ) {
				{
					Hypertable::NamespacePtr nsSub = client->open_namespace( (*it).name.c_str(), ns.get() );
					drop( nsSub, (*it).sub_entries, ifExists, dropTables );
				}
				client->drop_namespace( (*it).name.c_str(), ns.get(), ifExists );
			}
			else if( dropTables ) {
				ns->drop_table( (*it).name.c_str(), true );
			}
		}
	}

} }