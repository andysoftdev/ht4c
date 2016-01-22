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
#include "ThriftTable.h"
#include "ThriftNamespace.h"
#include "ThriftTableMutator.h"
#include "ThriftAsyncTableMutator.h"
#include "ThriftTableScanner.h"
#include "ThriftAsyncTableScanner.h"
#include "ThriftAsyncResult.h"
#include "ThriftBlockingAsyncResult.h"
#include "ThriftException.h"

#include "ht4c.Common/ScanSpec.h"

namespace ht4c { namespace Thrift {

	Common::Table* ThriftTable::create( Hypertable::Thrift::ThriftClientPtr client, const ThriftNamespace* ns, const std::string& name ) {
		HT4C_TRY {
			return new ThriftTable( client, ns, name );
		}
		HT4C_THRIFT_RETHROW
	}

	ThriftTable::~ThriftTable( ) {
		HT4C_TRY {
			client = 0;
		}
		HT4C_THRIFT_RETHROW
	}

	std::string ThriftTable::getName( ) const {
		return fullname;
	}

	Common::TableMutator* ThriftTable::createMutator( uint32_t /*timeoutMsec*/, uint32_t flags, uint32_t flushIntervalMsec ) {
		HT4C_TRY {
			ThriftClientLock sync( client.get() );
			return ThriftTableMutator::create( client, client->mutator_open(ns, name, flags, flushIntervalMsec) );
		}
		HT4C_THRIFT_RETHROW
	}

	Common::AsyncTableMutator* ThriftTable::createAsyncMutator( Common::AsyncResult& asyncResult, uint32_t /*timeoutMsec*/, uint32_t flags ) {
		HT4C_TRY {
			ThriftClientLock sync( client.get() );
			Hypertable::ThriftGen::Future future = typeid(asyncResult) != typeid(ThriftBlockingAsyncResult)
																					 ? static_cast<ThriftAsyncResult&>(asyncResult).get(client)
																					 : static_cast<ThriftBlockingAsyncResult&>(asyncResult).get(client);
			return ThriftAsyncTableMutator::create( client, client->async_mutator_open(ns, name, future, flags) );
		}
		HT4C_RETHROW
	}

	Common::TableScanner* ThriftTable::createScanner( Common::ScanSpec& scanSpec, uint32_t /*timeoutMsec*/, uint32_t /*flags*/ ) {
		HT4C_TRY {
			Hypertable::ThriftGen::ScanSpec _scanSpec;
			convertScanSpec( scanSpec, _scanSpec );
			ThriftClientLock sync( client.get() );
			return ThriftTableScanner::create( client, client->scanner_open(ns, name, _scanSpec) );
		}
		HT4C_THRIFT_RETHROW
	}

	Common::AsyncTableScanner* ThriftTable::createAsyncScanner( Common::ScanSpec& scanSpec, Common::AsyncResult& asyncResult, uint32_t /*timeoutMsec*/, uint32_t /*flags*/ ) {
		HT4C_TRY {
			Hypertable::ThriftGen::ScanSpec _scanSpec;
			convertScanSpec( scanSpec, _scanSpec );
			ThriftClientLock sync( client.get() );
			Hypertable::ThriftGen::Future future = typeid(asyncResult) != typeid(ThriftBlockingAsyncResult)
																					 ? static_cast<ThriftAsyncResult&>(asyncResult).get(client)
																					 : static_cast<ThriftBlockingAsyncResult&>(asyncResult).get(client);

			return ThriftAsyncTableScanner::create( client, client->async_scanner_open(ns, name, future, _scanSpec) );
		}
		HT4C_THRIFT_RETHROW
	}

	int64_t ThriftTable::createAsyncScannerId( Common::ScanSpec& scanSpec, Common::AsyncResult& asyncResult, uint32_t /*timeoutMsec*/, uint32_t /*flags*/ ) {
		HT4C_TRY {
			Hypertable::ThriftGen::ScanSpec _scanSpec;
			convertScanSpec( scanSpec, _scanSpec );
			ThriftClientLock sync( client.get() );
			Hypertable::ThriftGen::Future future = typeid(asyncResult) != typeid(ThriftBlockingAsyncResult)
																					 ? static_cast<ThriftAsyncResult&>(asyncResult).get(client)
																					 : static_cast<ThriftBlockingAsyncResult&>(asyncResult).get(client);

			return ThriftAsyncTableScanner::id( client->async_scanner_open(ns, name, future, _scanSpec) );
		}
		HT4C_THRIFT_RETHROW
	}

	std::string ThriftTable::getSchema( bool withIds ) {
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

	ThriftTable::ThriftTable( Hypertable::Thrift::ThriftClientPtr _client, const ThriftNamespace* _ns, const std::string& _name )
	: client( )
	, ns( _ns->get() )
	, name( _name )
	, fullname( _ns->getName() + "/" + _name )
	{
		HT4C_TRY {
			client = _client;
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftTable::convertScanSpec( Common::ScanSpec& scanSpec, Hypertable::ThriftGen::ScanSpec& tss ) {
		const Hypertable::ScanSpec& hss = scanSpec.get();

		if( hss.row_limit ) {
			tss.row_limit = hss.row_limit;
			tss.__isset.row_limit = true;
		}

		if( hss.cell_limit ) {
			tss.cell_limit = hss.cell_limit;
			tss.__isset.cell_limit = true;
		}

		if( hss.cell_limit_per_family ) {
			tss.cell_limit_per_family = hss.cell_limit_per_family;
			tss.__isset.cell_limit_per_family = true;
		}

		if( hss.max_versions ) {
			tss.versions = hss.max_versions;
			tss.__isset.versions = true;
		}

		if( hss.row_offset ) {
			tss.row_offset = hss.row_offset;
			tss.__isset.row_offset = true;
		}

		if( hss.cell_offset ) {
			tss.cell_offset = hss.cell_offset;
			tss.__isset.cell_offset = true;
		}

		if( hss.time_interval.first ) {
			tss.start_time = hss.time_interval.first;
			tss.__isset.start_time = true;
		}

		if( hss.time_interval.second ) {
			tss.end_time = hss.time_interval.second;
			tss.__isset.end_time = true;
		}

		if( hss.row_regexp && *hss.row_regexp ) {
			tss.row_regexp = hss.row_regexp;
			tss.__isset.row_regexp = true;
		}

		if( hss.value_regexp && *hss.value_regexp ) {
			tss.value_regexp = hss.value_regexp;
			tss.__isset.value_regexp = true;
		}

		tss.return_deletes = hss.return_deletes;
		tss.__isset.return_deletes = true;

		tss.keys_only = hss.keys_only;
		tss.__isset.keys_only = true;

		tss.do_not_cache = hss.do_not_cache;
		tss.__isset.do_not_cache = true;

		tss.scan_and_filter_rows = hss.scan_and_filter_rows;
		tss.__isset.scan_and_filter_rows = true;

		tss.and_column_predicates = hss.and_column_predicates;
		tss.__isset.and_column_predicates = true;

		if( hss.column_predicates.size() ) {
			tss.column_predicates.reserve( hss.column_predicates.size() );
			tss.__isset.column_predicates = true;

			Hypertable::ThriftGen::ColumnPredicate tcp;
			tcp.__isset.operation = true;

			for each( const Hypertable::ColumnPredicate& cp in hss.column_predicates ) {
				tcp.__isset.column_family = cp.column_family != 0;
				if( cp.column_family ) {
					tcp.column_family = cp.column_family;
				}
				tcp.__isset.column_qualifier = cp.column_qualifier != 0;
				if( cp.column_qualifier ) {
					tcp.column_qualifier = std::string(cp.column_qualifier, cp.column_qualifier_len);
				}
				tcp.operation = (Hypertable::ThriftGen::ColumnPredicateOperation::type)cp.operation;
				tcp.__isset.value = cp.value != 0;
				if( tcp.__isset.value ) {
					tcp.value = std::string( cp.value, cp.value_len );
				}

				tss.column_predicates.push_back( tcp );
			}
		}

		if( hss.row_intervals.size() ) {
			tss.row_intervals.reserve( hss.row_intervals.size() );
			tss.__isset.row_intervals = true;

			Hypertable::ThriftGen::RowInterval tri;
			tri.__isset.start_inclusive = true;
			tri.__isset.end_inclusive = true;

			for each( const Hypertable::RowInterval& ri in hss.row_intervals ) {
				tri.__isset.start_row = ri.start != 0;
				if( ri.start ) {
					tri.start_row = ri.start;
				}
				tri.start_inclusive = ri.start_inclusive;
				tri.__isset.end_row = ri.end != 0;
				if( ri.end ) {
					tri.end_row = ri.end;
				}
				tri.end_inclusive = ri.end_inclusive;

				tss.row_intervals.push_back( tri );
			}
		}

		if( hss.cell_intervals.size() ) {
			tss.cell_intervals.reserve( hss.cell_intervals.size() );
			tss.__isset.cell_intervals = true;

			Hypertable::ThriftGen::CellInterval tci;
			tci.__isset.start_column = true;
			tci.__isset.start_inclusive = true;
			tci.__isset.end_column = true;
			tci.__isset.end_inclusive = true;

			for each( const Hypertable::CellInterval& ci in hss.cell_intervals ) {
				tci.__isset.start_row = ci.start_row != 0;
				if( ci.start_row ) {
					tci.start_row = ci.start_row;
				}
				tci.start_column = ci.start_column;
				tci.start_inclusive = ci.start_inclusive;
				tci.__isset.end_row = ci.end_row != 0;
				if( ci.end_row ) {
					tci.end_row = ci.end_row;
				}
				tci.end_column = ci.end_column;
				tci.end_inclusive = ci.end_inclusive;

				tss.cell_intervals.push_back( tci );
			}
		}

		if( hss.columns.size() ) {
			tss.columns.reserve( hss.columns.size() );
			tss.__isset.columns = true;

			for each( const std::string& col in hss.columns ) {
				tss.columns.push_back( col );
			}
		}
	}

} }