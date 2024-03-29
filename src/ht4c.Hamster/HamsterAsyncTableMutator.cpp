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
#include "HamsterAsyncTableMutator.h"
#include "HamsterException.h"

#include "ht4c.Common/KeyBuilder.h"

namespace ht4c { namespace Hamster {

	Common::AsyncTableMutator* HamsterAsyncTableMutator::create( Db::MutatorAsyncPtr tableMutator ) {
		HT4C_TRY {
			return new HamsterAsyncTableMutator( tableMutator );
		}
		HT4C_HAMSTER_RETHROW
	}

	int64_t HamsterAsyncTableMutator::id( const Db::MutatorAsyncPtr& tableMutator ) {
		return int64_t(tableMutator.get());
	}

	HamsterAsyncTableMutator::~HamsterAsyncTableMutator( ) throw(ht4c::Common::HypertableException) {
		HT4C_TRY {
			{
				HamsterEnvLock sync( tableMutator->getEnv() );
				tableMutator->flush( );
			}
			tableMutator = 0;
		}
		HT4C_HAMSTER_RETHROW
	}

	int64_t HamsterAsyncTableMutator::id( ) const {
		return id( tableMutator );
	}

	void HamsterAsyncTableMutator::set( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, uint8_t flag ) {
		/*HT4C_TRY {
			flag = FLAG( columnFamily, columnQualifier, flag );
			Hypertable::SerializedCellsWriter writer( flag == Hypertable::FLAG_INSERT ? 1024 : 128, true );
			writer.add( row, CF(columnFamily), columnQualifier, TIMESTAMP(timestamp, flag), value, valueLength, flag );
			writer.finalize( Hypertable::SerializedCellsFlag::EOS );
			HamsterEnvLock sync( env.get() );
			env->async_set_cells_serialized( tableMutator, Hypertable::HamsterGen::CellsSerialized(reinterpret_cast<char*>(writer.get_buffer()), writer.get_buffer_length()), false );
		}
		HT4C_HAMSTER_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	void HamsterAsyncTableMutator::set( const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, std::string& row ) {
		row = Common::KeyBuilder();
		set( row.c_str(), CF(columnFamily), columnQualifier, timestamp, value, valueLength, Hypertable::FLAG_INSERT );
	}

	void HamsterAsyncTableMutator::set( const Common::Cells& cells ) {
		/*HT4C_TRY {
			const Hypertable::Cells& _cells = cells.get();
			Hypertable::SerializedCellsWriter writer( 8192, true );
			for( Hypertable::Cells::const_iterator it = _cells.begin(); it != _cells.end(); ++it ) {
				writer.add( (*it).row_key, (*it).column_family, (*it).column_qualifier, (*it).timestamp, (*it).value, (*it).value_len, (*it).flag );
			}
			writer.finalize( Hypertable::SerializedCellsFlag::EOS );
			HamsterEnvLock sync( env.get() );
			env->async_set_cells_serialized( tableMutator, Hypertable::HamsterGen::CellsSerialized(reinterpret_cast<char*>(writer.get_buffer()), writer.get_buffer_length()), false );
		}
		HT4C_HAMSTER_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	void HamsterAsyncTableMutator::del( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp ) {
		/*HT4C_TRY {
			uint8_t flag = FLAG_DELETE( columnFamily, columnQualifier );
			Hypertable::SerializedCellsWriter writer( 128, true );
			writer.add( row, CF(columnFamily), columnQualifier, TIMESTAMP(timestamp, flag), 0, 0, flag );
			writer.finalize( Hypertable::SerializedCellsFlag::EOS );
			HamsterEnvLock sync( env.get() );
			env->async_set_cells_serialized( tableMutator, Hypertable::HamsterGen::CellsSerialized(reinterpret_cast<char*>(writer.get_buffer()), writer.get_buffer_length()), false );
		}
		HT4C_HAMSTER_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	void HamsterAsyncTableMutator::flush() {
		HT4C_TRY {
			HamsterEnvLock sync( tableMutator->getEnv() );
			tableMutator->flush( );
		}
		HT4C_HAMSTER_RETHROW
	}

	HamsterAsyncTableMutator::HamsterAsyncTableMutator( Db::MutatorAsyncPtr _tableMutator )
	: tableMutator( _tableMutator )
	{
	}
	
} }