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
#include "HyperNamespace.h"
#include "HyperTable.h"

#include "ht4c.Common/Table.h"
#include "ht4c.Common/Cells.h"
#include "ht4c.Common/Exception.h"


namespace ht4c { namespace Hyper {

	Common::Namespace* HyperNamespace::create( Hypertable::ClientPtr client, Hypertable::NamespacePtr ns ) {
		HT4C_TRY {
			return new HyperNamespace( client, ns );
		}
		HT4C_RETHROW
	}

	HyperNamespace::~HyperNamespace( ) {
		HT4C_TRY {
			ns = 0;
		}
		HT4C_RETHROW
	}

	std::string HyperNamespace::getName( ) const {
		HT4C_TRY {
			return ns->get_name( );
		}
		HT4C_RETHROW
	}

	void HyperNamespace::createTable( const char* name, const char* schema ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			ns->create_table( name, schema );
		}
		HT4C_RETHROW
	}

	void HyperNamespace::createTableLike( const char* name, const char* like ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			Hypertable::String schemaLike = ns->get_schema_str( like, true );
			Hypertable::SchemaPtr schema( Hypertable::Schema::new_instance(schemaLike) );
			schema->update_generation( get_ts64() );
			schemaLike.clear();
			schemaLike = schema->render_xml( true );
			ns->create_table( name, schemaLike );
		}
		HT4C_RETHROW
	}

	void HyperNamespace::alterTable( const char* name, const char* _schema ) {
		HT4C_TRY {
			Hypertable::SchemaPtr schema = ns->get_schema( name );
			Hypertable::SchemaPtr newSchema( Hypertable::Schema::new_instance(_schema) );

			newSchema->set_generation( schema->get_generation() );
			for each( const ColumnFamilySpec* cf in schema->get_column_families() ) {
				ColumnFamilySpec* newCf = newSchema->get_column_family( cf->get_name() );
				if( newCf ) {
					newCf->set_id( cf->get_id() );
				}
			}

			ns->alter_table( name, newSchema, false );
			ns->refresh_table( name );
		}
		HT4C_RETHROW
	}

	void HyperNamespace::renameTable( const char* nameOld, const char* nameNew ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( nameOld );
			Common::Namespace::validateTableName( nameNew );
			ns->rename_table( nameOld, nameNew );
		}
		HT4C_RETHROW
	}

	Common::Table* HyperNamespace::openTable( const char* name, bool force ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			return HyperTable::create( ns->open_table(name, force) );
		}
		HT4C_RETHROW
	}

	void HyperNamespace::dropTable( const char* name, bool ifExists ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			return ns->drop_table( name, ifExists );
		}
		HT4C_RETHROW
	}

	bool HyperNamespace::existsTable( const char* name ) {
		HT4C_TRY {
			return ns->exists_table( name );
		}
		HT4C_RETHROW
	}

	std::string HyperNamespace::getTableSchema( const char* name, bool withIds ) {
		HT4C_TRY {
			return ns->get_schema_str( name, withIds );
		}
		HT4C_RETHROW
	}

	void HyperNamespace::getListing( bool deep, ht4c::Common::NamespaceListing& nsListing ) {
		nsListing = ht4c::Common::NamespaceListing( getName() );

		HT4C_TRY {
			std::vector<Hypertable::NamespaceListing> listing;
			ns->get_listing( deep, listing );
			getListing( listing, nsListing );
		}
		HT4C_RETHROW
	}

	void HyperNamespace::exec( const char* hql ) {
		HT4C_TRY {
			if( hql && *hql ) {
				Hypertable::HqlInterpreterPtr hqlInterpreter( client->create_hql_interpreter(true) );
				hqlInterpreter->set_namespace( getName() );
				hqlInterpreter->execute( hql );
			}
		}
		HT4C_RETHROW
	}

	Common::Cells* HyperNamespace::query( const char* hql ) {
		HT4C_TRY {
			Common::Cells* cells = 0;
			if( hql && *hql ) {
				Hypertable::HqlInterpreterPtr hqlInterpreter( client->create_hql_interpreter(true) );
				hqlInterpreter->set_namespace( getName() );
				cells = Common::Cells::create( 512 );
				std::vector< std::string > results;
				hqlInterpreter->execute( hql, cells->builder(), results );
			}
			return cells;
		}
		HT4C_RETHROW
	}

	HyperNamespace::HyperNamespace( Hypertable::ClientPtr _client, Hypertable::NamespacePtr _ns )
	: ns( _ns )
	, client( _client )
	{
	}

	void HyperNamespace::getListing( const std::vector<Hypertable::NamespaceListing>& listing, ht4c::Common::NamespaceListing& nsListing ) {
		for( std::vector<Hypertable::NamespaceListing>::const_iterator it = listing.begin(); it != listing.end(); ++it ) {
			if( (*it).is_namespace ) {
				getListing( (*it).sub_entries, nsListing.addNamespace(ht4c::Common::NamespaceListing((*it).name)));
			}
			else if( (*it).name.size() && (*it).name.front() != '^' ) {
				nsListing.addTable( (*it).name );
			}
		}
	}

} }