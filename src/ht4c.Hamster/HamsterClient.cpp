/** -*- C++ -*-
 * Copyright (C) 2010-2013 Thalmann Software & Consulting, http://www.softdev.ch
 *
 * This file is part of ht4c.
 *
 * ht4c is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
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
#include "HamsterClient.h"
#include "HamsterNamespace.h"
#include "HamsterException.h"

namespace ht4c { namespace Hamster {

	Common::Client* HamsterClient::create( Db::ClientPtr client ) {
		HT4C_TRY {
			return new HamsterClient( client );
		}
		HT4C_HAMSTER_RETHROW
	}

	HamsterClient::~HamsterClient( ) {
		HT4C_TRY {
			client = 0;
		}
		HT4C_HAMSTER_RETHROW
	}

	void HamsterClient::createNamespace( const char* name, Common::Namespace* nsBase, bool createIntermediate, bool createIfNotExists ) {
		HT4C_TRY {
			std::string ns( getNamespace(name, nsBase) );
			HamsterEnvLock sync( client->getEnv() );
			if( createIfNotExists && client->namespaceExists(ns) ) {
				return;
			}
			client->createNamespace( ns, createIntermediate );
		}
		HT4C_HAMSTER_RETHROW
	}

	Common::Namespace* HamsterClient::openNamespace( const char* name, Common::Namespace* nsBase ) {
		HT4C_TRY {
			std::string _name( getNamespace(name, nsBase) );
			HamsterEnvLock sync( client->getEnv() );
			Db::NamespacePtr ns( client->openNamespace(_name) );
			return ns ? HamsterNamespace::create( ns, _name ) : 0;
		}
		HT4C_HAMSTER_RETHROW
	}

	void HamsterClient::dropNamespace( const char* name, Common::Namespace* nsBase, bool ifExists, bool dropTables, bool deep ) {
		HT4C_TRY {
			std::string _name( getNamespace(name, nsBase) );
			HamsterEnvLock sync( client->getEnv() );
			if( dropTables || deep ) {
				if( ifExists && !client->namespaceExists(_name) ) {
					return;
				}
				std::vector<Db::NamespaceListing> listing;
				Db::NamespacePtr ns = client->openNamespace( _name );
				ns->getNamespaceListing( deep, listing );
				if( deep ) {
					ns->drop( _name, listing, ifExists, dropTables );
				}
				else {
					for( std::vector<Db::NamespaceListing>::const_iterator it = listing.begin(); it != listing.end(); ++it ) {
						if( !(*it).isNamespace ) {
							ns->dropTable( (*it).name, true );
						}
					}
				}
			}
			return client->dropNamespace( _name, ifExists );
		}
		HT4C_HAMSTER_RETHROW
	}

	bool HamsterClient::existsNamespace( const char* name, Common::Namespace* nsBase ) {
		HT4C_TRY {
			HamsterEnvLock sync( client->getEnv() );
			return client->namespaceExists( getNamespace(name, nsBase) );
		}
		HT4C_HAMSTER_RETHROW
	}
	
	HamsterClient::HamsterClient( Db::ClientPtr _client )
	: client( _client )
	{
	}

	std::string HamsterClient::getNamespace( const char* name, Common::Namespace* nsBase ) {
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

} }