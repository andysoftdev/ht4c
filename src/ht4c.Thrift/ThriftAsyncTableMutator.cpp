/** -*- C++ -*-
 * Copyright (C) 2010-2012 Thalmann Software & Consulting, http://www.softdev.ch
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
#include "ThriftAsyncTableMutator.h"
#include "ThriftException.h"

#include "ht4c.Common/KeyBuilder.h"

namespace ht4c { namespace Thrift {

	Common::AsyncTableMutator* ThriftAsyncTableMutator::create( Hypertable::Thrift::ClientPtr client, const Hypertable::ThriftGen::MutatorAsync& tableMutator ) {
		HT4C_TRY {
			return new ThriftAsyncTableMutator( client, tableMutator );
		}
		HT4C_THRIFT_RETHROW
	}

	int64_t ThriftAsyncTableMutator::id( const Hypertable::ThriftGen::MutatorAsync& tableMutator ) {
		return tableMutator;
	}

	ThriftAsyncTableMutator::~ThriftAsyncTableMutator( ) {
		HT4C_TRY {
			{
				ThriftClientLock sync( client.get() );
				HT4C_THRIFT_RETRY( client->async_mutator_close(tableMutator) );
			}
			client = 0;
		}
		HT4C_THRIFT_RETHROW
	}

	int64_t ThriftAsyncTableMutator::id( ) const {
		return id( tableMutator );
	}

	void ThriftAsyncTableMutator::set( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, uint8_t flag ) {
		HT4C_TRY {
			flag = FLAG( columnFamily, columnQualifier, flag );
			Hypertable::SerializedCellsWriter writer( flag == Hypertable::FLAG_INSERT ? 1024 : 128, true );
			writer.add( row, CF(columnFamily), columnQualifier, TIMESTAMP(timestamp, flag), value, valueLength, flag );
			writer.finalize( Hypertable::SerializedCellsFlag::EOS );
			ThriftClientLock sync( client.get() );
			client->async_mutator_set_cells_serialized( tableMutator, CellsSerializedNoCopy(reinterpret_cast<char*>(writer.get_buffer()), writer.get_buffer_length()), false );
			needFlush = true;
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftAsyncTableMutator::set( const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, std::string& row ) {
		row = Common::KeyBuilder();
		set( row.c_str(), CF(columnFamily), columnQualifier, timestamp, value, valueLength, Hypertable::FLAG_INSERT );
	}

	void ThriftAsyncTableMutator::set( const Common::Cells& cells ) {
		HT4C_TRY {
			const Hypertable::Cells& _cells = cells.get();
			Hypertable::SerializedCellsWriter writer( 8192, true );
			for( Hypertable::Cells::const_iterator it = _cells.begin(); it != _cells.end(); ++it ) {
				writer.add( (*it).row_key, (*it).column_family, (*it).column_qualifier, (*it).timestamp, (*it).value, (*it).value_len, (*it).flag );
			}
			writer.finalize( Hypertable::SerializedCellsFlag::EOS );
			ThriftClientLock sync( client.get() );
			client->async_mutator_set_cells_serialized( tableMutator, CellsSerializedNoCopy(reinterpret_cast<char*>(writer.get_buffer()), writer.get_buffer_length()), false );
			needFlush = true;
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftAsyncTableMutator::del( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp ) {
		HT4C_TRY {
			uint8_t flag = FLAG_DELETE( columnFamily, columnQualifier );
			Hypertable::SerializedCellsWriter writer( 128, true );
			writer.add( row, CF(columnFamily), columnQualifier, TIMESTAMP(timestamp, flag), 0, 0, flag );
			writer.finalize( Hypertable::SerializedCellsFlag::EOS );
			ThriftClientLock sync( client.get() );
			client->async_mutator_set_cells_serialized( tableMutator, CellsSerializedNoCopy(reinterpret_cast<char*>(writer.get_buffer()), writer.get_buffer_length()), false );
			needFlush = true;
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftAsyncTableMutator::flush() {
		HT4C_TRY {
			ThriftClientLock sync( client.get() );
			if( needFlush ) {
				client->async_mutator_flush( tableMutator );
				needFlush = false;
			}
		}
		HT4C_THRIFT_RETHROW
	}

	ThriftAsyncTableMutator::ThriftAsyncTableMutator( Hypertable::Thrift::ClientPtr _client, const Hypertable::ThriftGen::MutatorAsync& _tableMutator )
	: client( )
	, tableMutator( _tableMutator )
	, needFlush( false )
	{
		HT4C_TRY {
			client = _client;
		}
		HT4C_THRIFT_RETHROW
	}
	
} }