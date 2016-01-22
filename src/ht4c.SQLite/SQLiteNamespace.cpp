/** -*- C++ -*-
 * Copyright (C) 2010-2016 Thalmann Software & Consulting, http://www.softdev.ch
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
#include "SQLiteNamespace.h"
#include "SQLiteTable.h"
#include "SQLiteException.h"

#include "ht4c.Common/Table.h"
#include "ht4c.Common/Cells.h"

namespace ht4c { namespace SQLite {

	Common::Namespace* SQLiteNamespace::create( Db::NamespacePtr ns, const std::string& name ) {
		HT4C_TRY {
			return new SQLiteNamespace( ns, name );
		}
		HT4C_SQLITE_RETHROW
	}

	SQLiteNamespace::~SQLiteNamespace( ) {
		HT4C_TRY {
			ns = 0;
		}
		HT4C_SQLITE_RETHROW
	}

	std::string SQLiteNamespace::getName( ) const {
		return name;
	}

	void SQLiteNamespace::createTable( const char* name, const char* schema ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			SQLiteEnvLock sync( ns->getEnv() );
			ns->createTable( name, schema );
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteNamespace::createTableLike( const char* name, const char* like ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			std::string schemaLike;
			SQLiteEnvLock sync( ns->getEnv() );
			ns->getTableSchema( like, true, schemaLike );
			ns->createTable( name, schemaLike );
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteNamespace::alterTable( const char* name, const char* schema ) {
		HT4C_TRY {
			SQLiteEnvLock sync( ns->getEnv() );
			ns->alterTable( name, schema );
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteNamespace::renameTable( const char* nameOld, const char* nameNew ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( nameOld );
			Common::Namespace::validateTableName( nameNew );
			SQLiteEnvLock sync( ns->getEnv() );
			ns->renameTable( nameOld, nameNew );
		}
		HT4C_SQLITE_RETHROW
	}

	Common::Table* SQLiteNamespace::openTable( const char* name, bool /*force*/ ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			SQLiteEnvLock sync( ns->getEnv() );
			if( !ns->tableExists(name) ) {
				using namespace Hypertable;
				HT_THROW(Error::TABLE_NOT_FOUND, name);
			}
			return SQLiteTable::create( ns->openTable(name) );
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteNamespace::dropTable( const char* name, bool ifExists ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			SQLiteEnvLock sync( ns->getEnv() );
			return ns->dropTable( name, ifExists );
		}
		HT4C_SQLITE_RETHROW
	}

	bool SQLiteNamespace::existsTable( const char* name ) {
		HT4C_TRY {
			SQLiteEnvLock sync( ns->getEnv() );
			return ns->tableExists( name );
		}
		HT4C_SQLITE_RETHROW
	}

	std::string SQLiteNamespace::getTableSchema( const char* name, bool withIds ) {
		HT4C_TRY {
			std::string schema;
			SQLiteEnvLock sync( ns->getEnv() );
			ns->getTableSchema( name, withIds, schema );
			return schema;
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteNamespace::getListing( bool deep, ht4c::Common::NamespaceListing& nsListing ) {
		nsListing = ht4c::Common::NamespaceListing( getName() );

		HT4C_TRY {
			std::vector<Db::NamespaceListing> listing;
			{
				SQLiteEnvLock sync( ns->getEnv() );
				ns->getNamespaceListing( deep, listing );
			}
			getListing( listing, nsListing );
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteNamespace::exec( const char* hql ) {
		HT4C_THROW_NOTIMPLEMENTED();
	}

	Common::Cells* SQLiteNamespace::query( const char* hql ) {
		HT4C_THROW_NOTIMPLEMENTED();
	}

	SQLiteNamespace::SQLiteNamespace( Db::NamespacePtr _ns, const std::string& _name )
	: ns( _ns )
	, name( _name )
	{
	}

	void SQLiteNamespace::getListing( const std::vector<Db::NamespaceListing>& listing, ht4c::Common::NamespaceListing& nsListing ) {
		for( std::vector<Db::NamespaceListing>::const_iterator it = listing.begin(); it != listing.end(); ++it ) {
			if( (*it).isNamespace ) {
				getListing( (*it).subEntries, nsListing.addNamespace(ht4c::Common::NamespaceListing((*it).name)));
			}
			else if( (*it).name.size() && (*it).name.front() != '^' ) {
				nsListing.addTable( (*it).name );
			}
		}
	}

} }