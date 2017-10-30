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
#include "HyperAsyncTableMutator.h"

#include "ht4c.Common/KeyBuilder.h"
#include "ht4c.Common/Exception.h"

namespace ht4c { namespace Hyper {

	Common::AsyncTableMutator* HyperAsyncTableMutator::create( Hypertable::TableMutatorAsync* tableMutator ) {
		HT4C_TRY {
			return new HyperAsyncTableMutator( tableMutator );
		}
		HT4C_RETHROW
	}

	int64_t HyperAsyncTableMutator::id( Hypertable::TableMutatorAsync* tableMutator ) {
		return reinterpret_cast<int64_t>( tableMutator );
	}

	HyperAsyncTableMutator::~HyperAsyncTableMutator( ) throw(ht4c::Common::HypertableException) {
		HT4C_TRY {
			tableMutator = 0;
		}
		HT4C_RETHROW
	}

	int64_t HyperAsyncTableMutator::id( ) const {
		return id( tableMutator.get() );
	}

	void HyperAsyncTableMutator::set( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, uint8_t flag ) {
		HT4C_TRY {
			if( tableMutator->needs_flush() ) {
				tableMutator->flush();
			}
			flag = FLAG( columnFamily, columnQualifier, flag );
			Hypertable::KeySpec keySpec( row, CF(columnFamily), columnQualifier, TIMESTAMP(timestamp, flag), flag );
			tableMutator->set( keySpec, value, valueLength );
		}
		HT4C_RETHROW
	}

	void HyperAsyncTableMutator::set( const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, std::string& row ) {
		HT4C_TRY {
			if( tableMutator->needs_flush() ) {
				tableMutator->flush();
			}
			Common::KeyBuilder keyBuilder;
			Hypertable::KeySpec keySpec( keyBuilder, CF(columnFamily), columnQualifier, TIMESTAMP(timestamp, Hypertable::FLAG_INSERT) );
			tableMutator->set( keySpec, value, valueLength );
			row = keyBuilder;
		}
		HT4C_RETHROW
	}

	void HyperAsyncTableMutator::set( const Common::Cells& cells ) {
		HT4C_TRY {
			if( tableMutator->needs_flush() ) {
				tableMutator->flush();
			}
			tableMutator->set_cells( cells.get() );
		}
		HT4C_RETHROW
	}

	void HyperAsyncTableMutator::del( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp ) {
		HT4C_TRY {
			if( tableMutator->needs_flush() ) {
				tableMutator->flush();
			}
			uint8_t flag = FLAG_DELETE( columnFamily, columnQualifier );
			Hypertable::KeySpec keySpec( row, CF(columnFamily), columnQualifier, TIMESTAMP(timestamp, flag), flag );
			tableMutator->set_delete( keySpec );
		}
		HT4C_RETHROW
	}

	void HyperAsyncTableMutator::flush() {
		HT4C_TRY {
			tableMutator->flush();
		}
		HT4C_RETHROW
	}

	HyperAsyncTableMutator::HyperAsyncTableMutator( Hypertable::TableMutatorAsync* _tableMutator )
	: tableMutator( _tableMutator )
	{
	}

} }