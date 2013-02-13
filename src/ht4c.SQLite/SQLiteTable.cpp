/** -*- C++ -*-
 * Copyright (C) 2010-2013 Thalmann Software & Consulting, http://www.softdev.ch
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
#include "SQLiteTable.h"
#include "SQLiteNamespace.h"
#include "SQLiteTableMutator.h"
#include "SQLiteAsyncTableMutator.h"
#include "SQLiteTableScanner.h"
#include "SQLiteAsyncTableScanner.h"
#include "SQLiteAsyncResult.h"
#include "SQLiteBlockingAsyncResult.h"
#include "SQLiteException.h"

#include "ht4c.Common/ScanSpec.h"

namespace ht4c { namespace SQLite {

	Common::Table* SQLiteTable::create( Db::TablePtr table ) {
		HT4C_TRY {
			return new SQLiteTable( table );
		}
		HT4C_SQLITE_RETHROW
	}

	SQLiteTable::~SQLiteTable( ) {
		HT4C_TRY {
			table = 0;
		}
		HT4C_SQLITE_RETHROW
	}

	std::string SQLiteTable::getName( ) const {
		return table->getFullName();
	}

	Common::TableMutator* SQLiteTable::createMutator( uint32_t /*timeoutMsec*/, uint32_t flags, uint32_t flushIntervalMsec ) {
		HT4C_TRY {
			SQLiteEnvLock sync( table->getEnv() );
			return SQLiteTableMutator::create( table->createMutator(flags, flushIntervalMsec) );
		}
		HT4C_SQLITE_RETHROW
	}

	Common::AsyncTableMutator* SQLiteTable::createAsyncMutator( Common::AsyncResult& asyncResult, uint32_t /*timeoutMsec*/, uint32_t flags ) {
		/*HT4C_TRY {
			SQLiteEnvLock sync( table->getEnv() );
			TODO Hypertable::SQLiteGen::Future future = typeid(asyncResult) != typeid(SQLiteBlockingAsyncResult)
																					 ? static_cast<SQLiteAsyncResult&>(asyncResult).get(env)
																					 : static_cast<SQLiteBlockingAsyncResult&>(asyncResult).get(env);
			return SQLiteAsyncTableMutator::create( table->async_createMutator(future, flags) );
		}
		HT4C_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	Common::TableScanner* SQLiteTable::createScanner( Common::ScanSpec& scanSpec, uint32_t /*timeoutMsec*/, uint32_t flags ) {
		HT4C_TRY {
			SQLiteEnvLock sync( table->getEnv() );
			return SQLiteTableScanner::create( table->createScanner(scanSpec.get(), flags) );
		}
		HT4C_SQLITE_RETHROW
	}

	Common::AsyncTableScanner* SQLiteTable::createAsyncScanner( Common::ScanSpec& scanSpec, Common::AsyncResult& asyncResult, uint32_t /*timeoutMsec*/, uint32_t /*flags*/ ) {
		/*HT4C_TRY {
			Hypertable::SQLiteGen::ScanSpec _scanSpec;
			convertScanSpec( scanSpec, _scanSpec );
			SQLiteEnvLock sync( table->getEnv() );
			Hypertable::SQLiteGen::Future future = typeid(asyncResult) != typeid(SQLiteBlockingAsyncResult)
																					 ? static_cast<SQLiteAsyncResult&>(asyncResult).get(env)
																					 : static_cast<SQLiteBlockingAsyncResult&>(asyncResult).get(env);

			return SQLiteAsyncTableScanner::create( table->async_createScanner(future, _scanSpec) );
		}
		HT4C_SQLITE_RETHROW */
		HT4C_THROW_NOTIMPLEMENTED();
	}

	int64_t SQLiteTable::createAsyncScannerId( Common::ScanSpec& scanSpec, Common::AsyncResult& asyncResult, uint32_t /*timeoutMsec*/, uint32_t /*flags*/ ) {
		/*HT4C_TRY {
			Hypertable::SQLiteGen::ScanSpec _scanSpec;
			convertScanSpec( scanSpec, _scanSpec );
			SQLiteEnvLock sync( table->getEnv() );
			Hypertable::SQLiteGen::Future future = typeid(asyncResult) != typeid(SQLiteBlockingAsyncResult)
																					 ? static_cast<SQLiteAsyncResult&>(asyncResult).get(env)
																					 : static_cast<SQLiteBlockingAsyncResult&>(asyncResult).get(env);

			return SQLiteAsyncTableScanner::id( env->async_createScanner(ns, name, future, _scanSpec) );
		}
		HT4C_SQLITE_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	std::string SQLiteTable::getSchema( bool withIds ) {
		HT4C_TRY {
			std::string schema;
			SQLiteEnvLock sync( table->getEnv() );
			table->getTableSchema( withIds, schema );
			return schema;
		}
		HT4C_SQLITE_RETHROW
	}

	SQLiteTable::SQLiteTable( Db::TablePtr _table )
	: table( _table )
	{
	}

} }