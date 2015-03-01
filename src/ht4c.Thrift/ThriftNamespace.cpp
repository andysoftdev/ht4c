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
#include "ThriftNamespace.h"
#include "ThriftTable.h"
#include "ThriftException.h"

#include "ht4c.Common/Table.h"
#include "ht4c.Common/Cells.h"

namespace ht4c { namespace Thrift {

	namespace {

		bool convert_column_family_options(const Hypertable::ColumnFamilyOptions &hoptions, ThriftGen::ColumnFamilyOptions &toptions) {
			bool ret = false;
			if (hoptions.is_set_max_versions()) {
				toptions.__set_max_versions(hoptions.get_max_versions());
				ret = true;
			}
			if (hoptions.is_set_ttl()) {
				toptions.__set_ttl(hoptions.get_ttl());
				ret = true;
			}
			if (hoptions.is_set_time_order_desc()) {
				toptions.__set_time_order_desc(hoptions.get_time_order_desc());
				ret = true;
			}
			if (hoptions.is_set_counter()) {
				toptions.__set_counter(hoptions.get_counter());
				ret = true;
			}
			return ret;
		}

		bool convert_access_group_options(const Hypertable::AccessGroupOptions &hoptions, ThriftGen::AccessGroupOptions &toptions) {
			bool ret = false;
			if (hoptions.is_set_in_memory()) {
				toptions.__set_in_memory(hoptions.get_in_memory());
				ret = true;
			}
			if (hoptions.is_set_replication()) {
				toptions.__set_replication(hoptions.get_replication());
				ret = true;
			}
			if (hoptions.is_set_blocksize()) {
				toptions.__set_blocksize(hoptions.get_blocksize());
				ret = true;
			}
			if (hoptions.is_set_compressor()) {
				toptions.__set_compressor(hoptions.get_compressor());
				ret = true;
			}
			if (hoptions.is_set_bloom_filter()) {
				toptions.__set_bloom_filter(hoptions.get_bloom_filter());
				ret = true;
			}
			return ret;
		}

		void convert_schema(const Hypertable::SchemaPtr &hschema, ThriftGen::Schema &tschema) {
			if (hschema->get_generation())
				tschema.__set_generation(hschema->get_generation());

			tschema.__set_version(hschema->get_version());

			if (hschema->get_group_commit_interval())
				tschema.__set_group_commit_interval(hschema->get_group_commit_interval());

			for (auto ag_spec : hschema->get_access_groups()) {
				ThriftGen::AccessGroupSpec tag;
				tag.name = ag_spec->get_name();
				tag.__set_generation(ag_spec->get_generation());
				if (convert_access_group_options(ag_spec->options(), tag.options))
					tag.__isset.options = true;
				if (convert_column_family_options(ag_spec->defaults(), tag.defaults))
					tag.__isset.defaults = true;
				tschema.access_groups[ag_spec->get_name()] = tag;
				tschema.__isset.access_groups = true;
			}

			for (auto cf_spec : hschema->get_column_families()) {
				ThriftGen::ColumnFamilySpec tcf;
				tcf.name = cf_spec->get_name();
				tcf.access_group = cf_spec->get_access_group();
				tcf.deleted = cf_spec->get_deleted();
				if (cf_spec->get_generation())
					tcf.__set_generation(cf_spec->get_generation());
				if (cf_spec->get_id())
					tcf.__set_id(cf_spec->get_id());
				tcf.value_index = cf_spec->get_value_index();
				tcf.qualifier_index = cf_spec->get_qualifier_index();
				if (convert_column_family_options(cf_spec->options(), tcf.options))
					tcf.__isset.options = true;
				tschema.column_families[cf_spec->get_name()] = tcf;
				tschema.__isset.column_families = true;
			}

			if (convert_access_group_options(hschema->access_group_defaults(),
				tschema.access_group_defaults))
				tschema.__isset.access_group_defaults = true;

			if (convert_column_family_options(hschema->column_family_defaults(),
				tschema.column_family_defaults))
				tschema.__isset.column_family_defaults = true;

		}
	}

	Common::Namespace* ThriftNamespace::create( Hypertable::Thrift::ThriftClientPtr client, const Hypertable::ThriftGen::Namespace& ns, const std::string& name ) {
		HT4C_TRY {
			return new ThriftNamespace( client, ns, name );
		}
		HT4C_THRIFT_RETHROW
	}

	ThriftNamespace::~ThriftNamespace( ) {
		HT4C_TRY {
			{
				ThriftClientLock sync( client.get() );
				HT4C_THRIFT_RETRY( client->namespace_close(ns) );
			}
			client = 0;
		}
		HT4C_THRIFT_RETHROW
	}

	std::string ThriftNamespace::getName( ) const {
		return name;
	}

	void ThriftNamespace::createTable( const char* name, const char* _schema ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );

			Hypertable::SchemaPtr schema = Hypertable::Schema::new_instance( _schema );
			ThriftGen::Schema ts;
			convert_schema( schema, ts );

			ThriftClientLock sync( client.get() );
			client->table_create( ns, name, ts );
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftNamespace::createTableLike( const char* name, const char* like ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			std::string schemaLike;
			ThriftClientLock sync( client.get() );
			client->table_get_schema_str_with_ids( schemaLike, ns, like );

			Hypertable::SchemaPtr schema = Hypertable::Schema::new_instance( schemaLike );
			ThriftGen::Schema ts;
			convert_schema( schema, ts );

			client->table_create( ns, name, ts );
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftNamespace::alterTable( const char* name, const char* _schema ) {
		HT4C_TRY {
			ThriftClientLock sync( client.get() );

			ThriftGen::Schema schema;
			client->get_schema( schema, ns, name );
			Hypertable::SchemaPtr newSchema = Hypertable::Schema::new_instance( _schema );
			newSchema->set_generation( schema.generation );

			ThriftGen::Schema ts;
			convert_schema( newSchema, ts );

			client->table_alter( ns, name, ts );
			client->refresh_table( ns, name );
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftNamespace::renameTable( const char* nameOld, const char* nameNew ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( nameOld );
			Common::Namespace::validateTableName( nameNew );
			ThriftClientLock sync( client.get() );
			client->table_rename( ns, nameOld, nameNew );
		}
		HT4C_THRIFT_RETHROW
	}

	Common::Table* ThriftNamespace::openTable( const char* name, bool /*force*/ ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			ThriftClientLock sync( client.get() );
			if( !client->table_exists(ns, name) ) {
				using namespace Hypertable;
				HT_THROW(Error::TABLE_NOT_FOUND, name);
			}
			return ThriftTable::create( client, this, name );
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftNamespace::dropTable( const char* name, bool ifExists ) {
		HT4C_TRY {
			Common::Namespace::validateTableName( name );
			ThriftClientLock sync( client.get() );
			return client->table_drop( ns, name, ifExists );
		}
		HT4C_THRIFT_RETHROW
	}

	bool ThriftNamespace::existsTable( const char* name ) {
		HT4C_TRY {
			ThriftClientLock sync( client.get() );
			return client->table_exists( ns, name );
		}
		HT4C_THRIFT_RETHROW
	}

	std::string ThriftNamespace::getTableSchema( const char* name, bool withIds ) {
		HT4C_TRY {
			std::string schema;
			ThriftClientLock sync( client.get() );
			if( withIds ) {
				client->table_get_schema_str_with_ids( schema, ns, name );
			}
			else {
				client->table_get_schema_str( schema, ns, name );
			}
			return schema;
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftNamespace::getListing( bool deep, ht4c::Common::NamespaceListing& nsListing ) {
		nsListing = ht4c::Common::NamespaceListing( getName() );

		HT4C_TRY {
			std::vector<Hypertable::ThriftGen::NamespaceListing> listing;
			{
				ThriftClientLock sync( client.get() );
				client->namespace_get_listing( listing, ns );
			}
			getListing( deep, getName(), listing, nsListing ); //FIXME remove deep+name parameter if the sub entries are available in thrift
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftNamespace::exec( const char* hql ) {
		HT4C_TRY {
			if( hql && *hql ) {
				ThriftClientLock sync( client.get() );
				Hypertable::ThriftGen::HqlResult result;
				client->hql_exec( result, ns, hql, false, true );
				if( result.__isset.scanner ) {
					client->scanner_close( result.scanner );
				}
			}
		}
		HT4C_THRIFT_RETHROW
	}

	Common::Cells* ThriftNamespace::query( const char* hql ) {
		HT4C_TRY {
			Common::Cells* cells = 0;
			if( hql && *hql ) {
				Hypertable::ThriftGen::HqlResult result;
				{
					ThriftClientLock sync( client.get() );
					client->hql_query( result, ns, hql );
				}
				cells = Common::Cells::create( result.cells.size() );
				foreach_ht( const Hypertable::ThriftGen::Cell& cell, result.cells ) {
					cells->add( cell.key.row.c_str(), cell.key.column_family.c_str(), cell.key.column_qualifier.c_str(), cell.key.timestamp, cell.value.data(), cell.value.size(), cell.key.flag );
				}
			}
			return cells;
		}
		HT4C_THRIFT_RETHROW
	}

	ThriftNamespace::ThriftNamespace( Hypertable::Thrift::ThriftClientPtr _client, const Hypertable::ThriftGen::Namespace& _ns, const std::string& _name )
	: client( )
	, ns( _ns )
	, name( _name )
	{
		HT4C_TRY {
			client = _client;
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftNamespace::getListing( bool deep, const std::string& nsName, const std::vector<Hypertable::ThriftGen::NamespaceListing>& listing, ht4c::Common::NamespaceListing& nsListing ) {
		for( std::vector<Hypertable::ThriftGen::NamespaceListing>::const_iterator it = listing.begin(); it != listing.end(); ++it ) {
			if( (*it).is_namespace ) {
				//FIXME remove complete block if the sub entries are available in thrift
				if( deep ) {
					std::string nsSubName = nsName + "/" + (*it).name;
					Hypertable::ThriftGen::Namespace nsSub = client->namespace_open( nsSubName );
					ThriftClientLock sync( client.get() );
					try {
						std::vector<Hypertable::ThriftGen::NamespaceListing> sub_entries;
						client->namespace_get_listing( sub_entries, nsSub );
						client->namespace_close( nsSub );
						getListing( deep, nsSubName, sub_entries, nsListing.addNamespace(ht4c::Common::NamespaceListing((*it).name)));
					}
					catch( ... ) {
						HT4C_THRIFT_RETRY( client->namespace_close(nsSub) );
						throw;
					}
				}
				else {
					nsListing.addNamespace( ht4c::Common::NamespaceListing((*it).name) );
				}
				//FIXME getListing( (*it).sub_entries, nsListing.addNamespace(ht4c::Common::NamespaceListing((*it).name)));
			}
			else if( (*it).name.size() && (*it).name.front() != '^' ) {
				nsListing.addTable( (*it).name );
			}
		}
	}

} }