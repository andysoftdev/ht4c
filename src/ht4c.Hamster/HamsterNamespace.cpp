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
#include "HamsterNamespace.h"
#include "HamsterTable.h"
#include "HamsterException.h"

#include "ht4c.Common/Table.h"
#include "ht4c.Common/Cells.h"

namespace ht4c { namespace Hamster {

	Common::Namespace* HamsterNamespace::create( Db::NamespacePtr ns, const std::string& name ) {
		HT4C_TRY {
			return new HamsterNamespace( ns, name );
		}
		HT4C_HAMSTER_RETHROW
	}

	HamsterNamespace::~HamsterNamespace( ) {
		HT4C_TRY {
			ns = 0;
		}
		HT4C_HAMSTER_RETHROW
	}

	std::string HamsterNamespace::getName( ) const {
		return name;
	}

	void HamsterNamespace::createTable( const char* name, const char* schema ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			HamsterEnvLock sync( ns->getEnv() );
			ns->createTable( name, schema );
		}
		HT4C_HAMSTER_RETHROW
	}

	void HamsterNamespace::createTableLike( const char* name, const char* like ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			std::string schemaLike;
			HamsterEnvLock sync( ns->getEnv() );
			ns->getTableSchema( like, true, schemaLike );
			ns->createTable( name, schemaLike );
		}
		HT4C_HAMSTER_RETHROW
	}

	void HamsterNamespace::alterTable( const char* name, const char* schema ) {
		HT4C_TRY {
			HamsterEnvLock sync( ns->getEnv() );
			ns->alterTable( name, schema );
		}
		HT4C_HAMSTER_RETHROW
	}

	void HamsterNamespace::renameTable( const char* nameOld, const char* nameNew ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( nameOld );
			Common::Namespace::validateTableName( nameNew );
			HamsterEnvLock sync( ns->getEnv() );
			ns->renameTable( nameOld, nameNew );
		}
		HT4C_HAMSTER_RETHROW
	}

	Common::Table* HamsterNamespace::openTable( const char* name, bool /*force*/ ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			HamsterEnvLock sync( ns->getEnv() );
			if( !ns->tableExists(name) ) {
				using namespace Hypertable;
				HT_THROW(Error::TABLE_NOT_FOUND, name);
			}
			return HamsterTable::create( ns->openTable(name) );
		}
		HT4C_HAMSTER_RETHROW
	}

	void HamsterNamespace::dropTable( const char* name, bool ifExists ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			HamsterEnvLock sync( ns->getEnv() );
			return ns->dropTable( name, ifExists );
		}
		HT4C_HAMSTER_RETHROW
	}

	bool HamsterNamespace::existsTable( const char* name ) {
		HT4C_TRY {
			HamsterEnvLock sync( ns->getEnv() );
			return ns->tableExists( name );
		}
		HT4C_HAMSTER_RETHROW
	}

	std::string HamsterNamespace::getTableSchema( const char* name, bool withIds ) {
		HT4C_TRY {
			std::string schema;
			HamsterEnvLock sync( ns->getEnv() );
			ns->getTableSchema( name, withIds, schema );
			return schema;
		}
		HT4C_HAMSTER_RETHROW
	}

	void HamsterNamespace::getListing( bool deep, ht4c::Common::NamespaceListing& nsListing ) {
		nsListing = ht4c::Common::NamespaceListing( getName() );

		HT4C_TRY {
			std::vector<Db::NamespaceListing> listing;
			{
				HamsterEnvLock sync( ns->getEnv() );
				ns->getNamespaceListing( deep, listing );
			}
			getListing( listing, nsListing );
		}
		HT4C_HAMSTER_RETHROW
	}

	void HamsterNamespace::exec( const char* hql ) {
		HT4C_THROW_NOTIMPLEMENTED();
	}

	Common::Cells* HamsterNamespace::query( const char* hql ) {
		HT4C_THROW_NOTIMPLEMENTED();
	}

	HamsterNamespace::HamsterNamespace( Db::NamespacePtr _ns, const std::string& _name )
	: ns( _ns )
	, name( _name )
	{
	}

	void HamsterNamespace::getListing( const std::vector<Db::NamespaceListing>& listing, ht4c::Common::NamespaceListing& nsListing ) {
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