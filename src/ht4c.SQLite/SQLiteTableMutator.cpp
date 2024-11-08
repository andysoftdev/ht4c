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
#include "SQLiteTableMutator.h"
#include "SQLiteException.h"

#include "ht4c.Common/KeyBuilder.h"

namespace ht4c { namespace SQLite {

	Common::TableMutator* SQLiteTableMutator::create( Db::MutatorPtr tableMutator ) {
		HT4C_TRY {
			return new SQLiteTableMutator( tableMutator );
		}
		HT4C_SQLITE_RETHROW
	}


	SQLiteTableMutator::~SQLiteTableMutator( ) {
		HT4C_TRY {
			{
				SQLiteEnvLock sync( tableMutator->getEnv() );
				tableMutator->flush( );
			}
			tableMutator = 0;
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteTableMutator::set( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, uint8_t flag ) {
		HT4C_TRY {
			flag = FLAG( columnFamily, columnQualifier, flag );
			Hypertable::KeySpec keySpec( row, CF(columnFamily), columnQualifier, TIMESTAMP(timestamp, flag), flag );
			SQLiteEnvLock sync( tableMutator->getEnv() );
			tableMutator->set( keySpec, value, valueLength );
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteTableMutator::set( const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, std::string& row ) {
		HT4C_TRY {
			row = Common::KeyBuilder();
			Hypertable::KeySpec keySpec( row.c_str(), CF(columnFamily), columnQualifier, TIMESTAMP(timestamp, Hypertable::FLAG_INSERT) );
			SQLiteEnvLock sync( tableMutator->getEnv() );
			tableMutator->set( keySpec, value, valueLength );
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteTableMutator::set( const Common::Cells& cells ) {
		HT4C_TRY {
			SQLiteEnvLock sync( tableMutator->getEnv() );
			tableMutator->set( cells.get() );
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteTableMutator::del( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp ) {
		HT4C_TRY {
			uint8_t flag = FLAG_DELETE( columnFamily, columnQualifier );
			Hypertable::KeySpec keySpec( row, CF(columnFamily), columnQualifier, TIMESTAMP(timestamp, flag), flag );
			SQLiteEnvLock sync( tableMutator->getEnv() );
			tableMutator->del( keySpec );
		}
		HT4C_RETHROW
	}

	void SQLiteTableMutator::flush() {
		HT4C_TRY {
			SQLiteEnvLock sync( tableMutator->getEnv() );
			tableMutator->flush( );
		}
		HT4C_SQLITE_RETHROW
	}

	SQLiteTableMutator::SQLiteTableMutator( Db::MutatorPtr _tableMutator )
	:tableMutator( _tableMutator )
	{
	}
	
} }