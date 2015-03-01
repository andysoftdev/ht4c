/** -*- C++ -*-
 * Copyright (C) 2010-2015 Thalmann Software & Consulting, http://www.softdev.ch
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
#include "OdbcNamespace.h"
#include "OdbcTable.h"
#include "OdbcException.h"

#include "ht4c.Common/Table.h"
#include "ht4c.Common/Cells.h"

namespace ht4c { namespace Odbc {

	Common::Namespace* OdbcNamespace::create( Db::NamespacePtr ns, const std::string& name ) {
		HT4C_TRY {
			return new OdbcNamespace( ns, name );
		}
		HT4C_ODBC_RETHROW
	}

	OdbcNamespace::~OdbcNamespace( ) {
		HT4C_TRY {
			ns = 0;
		}
		HT4C_ODBC_RETHROW
	}

	std::string OdbcNamespace::getName( ) const {
		return name;
	}

	void OdbcNamespace::createTable( const char* name, const char* schema ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			OdbcEnvLock sync( ns->getEnv() );
			ns->createTable( name, schema );
		}
		HT4C_ODBC_RETHROW
	}

	void OdbcNamespace::createTableLike( const char* name, const char* like ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			std::string schemaLike;
			OdbcEnvLock sync( ns->getEnv() );
			ns->getTableSchema( like, true, schemaLike );
			ns->createTable( name, schemaLike );
		}
		HT4C_ODBC_RETHROW
	}

	void OdbcNamespace::alterTable( const char* name, const char* schema ) {
		HT4C_TRY {
			OdbcEnvLock sync( ns->getEnv() );
			ns->alterTable( name, schema );
		}
		HT4C_ODBC_RETHROW
	}

	void OdbcNamespace::renameTable( const char* nameOld, const char* nameNew ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( nameOld );
			Common::Namespace::validateTableName( nameNew );
			OdbcEnvLock sync( ns->getEnv() );
			ns->renameTable( nameOld, nameNew );
		}
		HT4C_ODBC_RETHROW
	}

	Common::Table* OdbcNamespace::openTable( const char* name, bool /*force*/ ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			OdbcEnvLock sync( ns->getEnv() );
			if( !ns->tableExists(name) ) {
				using namespace Hypertable;
				HT_THROW(Error::TABLE_NOT_FOUND, name);
			}
			return OdbcTable::create( ns->openTable(name) );
		}
		HT4C_ODBC_RETHROW
	}

	void OdbcNamespace::dropTable( const char* name, bool ifExists ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			OdbcEnvLock sync( ns->getEnv() );
			return ns->dropTable( name, ifExists );
		}
		HT4C_ODBC_RETHROW
	}

	bool OdbcNamespace::existsTable( const char* name ) {
		HT4C_TRY {
			OdbcEnvLock sync( ns->getEnv() );
			return ns->tableExists( name );
		}
		HT4C_ODBC_RETHROW
	}

	std::string OdbcNamespace::getTableSchema( const char* name, bool withIds ) {
		HT4C_TRY {
			std::string schema;
			OdbcEnvLock sync( ns->getEnv() );
			ns->getTableSchema( name, withIds, schema );
			return schema;
		}
		HT4C_ODBC_RETHROW
	}

	void OdbcNamespace::getListing( bool deep, ht4c::Common::NamespaceListing& nsListing ) {
		nsListing = ht4c::Common::NamespaceListing( getName() );

		HT4C_TRY {
			std::vector<Db::NamespaceListing> listing;
			{
				OdbcEnvLock sync( ns->getEnv() );
				ns->getNamespaceListing( deep, listing );
			}
			getListing( listing, nsListing );
		}
		HT4C_ODBC_RETHROW
	}

	void OdbcNamespace::exec( const char* hql ) {
		HT4C_THROW_NOTIMPLEMENTED();
	}

	Common::Cells* OdbcNamespace::query( const char* hql ) {
		HT4C_THROW_NOTIMPLEMENTED();
	}

	OdbcNamespace::OdbcNamespace( Db::NamespacePtr _ns, const std::string& _name )
	: ns( _ns )
	, name( _name )
	{
	}

	void OdbcNamespace::getListing( const std::vector<Db::NamespaceListing>& listing, ht4c::Common::NamespaceListing& nsListing ) {
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