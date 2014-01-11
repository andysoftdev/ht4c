/** -*- C++ -*-
 * Copyright (C) 2010-2014 Thalmann Software & Consulting, http://www.softdev.ch
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
#include "SQLiteClient.h"
#include "SQLiteNamespace.h"
#include "SQLiteException.h"

namespace ht4c { namespace SQLite {

	Common::Client* SQLiteClient::create( Db::ClientPtr client ) {
		HT4C_TRY {
			return new SQLiteClient( client );
		}
		HT4C_SQLITE_RETHROW
	}

	SQLiteClient::~SQLiteClient( ) {
		HT4C_TRY {
			client = 0;
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteClient::createNamespace( const char* name, Common::Namespace* nsBase, bool createIntermediate, bool createIfNotExists ) {
		HT4C_TRY {
			std::string ns( getNamespace(name, nsBase) );
			SQLiteEnvLock sync( client->getEnv() );
			if( createIfNotExists && client->namespaceExists(ns) ) {
				return;
			}
			client->createNamespace( ns, createIntermediate );
		}
		HT4C_SQLITE_RETHROW
	}

	Common::Namespace* SQLiteClient::openNamespace( const char* name, Common::Namespace* nsBase ) {
		HT4C_TRY {
			std::string _name( getNamespace(name, nsBase) );
			SQLiteEnvLock sync( client->getEnv() );
			Db::NamespacePtr ns( client->openNamespace(_name) );
			return ns ? SQLiteNamespace::create( ns, _name ) : 0;
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteClient::dropNamespace( const char* name, Common::Namespace* nsBase, bool ifExists, bool dropTables, bool deep ) {
		HT4C_TRY {
			std::string _name( getNamespace(name, nsBase) );
			SQLiteEnvLock sync( client->getEnv() );
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
		HT4C_SQLITE_RETHROW
	}

	bool SQLiteClient::existsNamespace( const char* name, Common::Namespace* nsBase ) {
		HT4C_TRY {
			SQLiteEnvLock sync( client->getEnv() );
			return client->namespaceExists( getNamespace(name, nsBase) );
		}
		HT4C_SQLITE_RETHROW
	}
	
	SQLiteClient::SQLiteClient( Db::ClientPtr _client )
	: client( _client )
	{
	}

	std::string SQLiteClient::getNamespace( const char* name, Common::Namespace* nsBase ) {
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