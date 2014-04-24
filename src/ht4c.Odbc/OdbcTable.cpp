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
#include "OdbcTable.h"
#include "OdbcNamespace.h"
#include "OdbcTableMutator.h"
#include "OdbcAsyncTableMutator.h"
#include "OdbcTableScanner.h"
#include "OdbcAsyncTableScanner.h"
#include "OdbcAsyncResult.h"
#include "OdbcBlockingAsyncResult.h"
#include "OdbcException.h"

#include "ht4c.Common/ScanSpec.h"

namespace ht4c { namespace Odbc {

	Common::Table* OdbcTable::create( Db::TablePtr table ) {
		HT4C_TRY {
			return new OdbcTable( table );
		}
		HT4C_ODBC_RETHROW
	}

	OdbcTable::~OdbcTable( ) {
		HT4C_TRY {
			table = 0;
		}
		HT4C_ODBC_RETHROW
	}

	std::string OdbcTable::getName( ) const {
		return table->getFullName();
	}

	Common::TableMutator* OdbcTable::createMutator( uint32_t /*timeoutMsec*/, uint32_t flags, uint32_t flushIntervalMsec ) {
		HT4C_TRY {
			OdbcEnvLock sync( table->getEnv() );
			return OdbcTableMutator::create( table->createMutator(flags, flushIntervalMsec) );
		}
		HT4C_ODBC_RETHROW
	}

	Common::AsyncTableMutator* OdbcTable::createAsyncMutator( Common::AsyncResult& asyncResult, uint32_t /*timeoutMsec*/, uint32_t flags ) {
		/*HT4C_TRY {
			OdbcEnvLock sync( table->getEnv() );
			TODO Hypertable::OdbcGen::Future future = typeid(asyncResult) != typeid(OdbcBlockingAsyncResult)
																					 ? static_cast<OdbcAsyncResult&>(asyncResult).get(env)
																					 : static_cast<OdbcBlockingAsyncResult&>(asyncResult).get(env);
			return OdbcAsyncTableMutator::create( table->async_createMutator(future, flags) );
		}
		HT4C_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	Common::TableScanner* OdbcTable::createScanner( Common::ScanSpec& scanSpec, uint32_t /*timeoutMsec*/, uint32_t flags ) {
		HT4C_TRY {
			OdbcEnvLock sync( table->getEnv() );
			return OdbcTableScanner::create( table->createScanner(scanSpec.get(), flags) );
		}
		HT4C_ODBC_RETHROW
	}

	Common::AsyncTableScanner* OdbcTable::createAsyncScanner( Common::ScanSpec& scanSpec, Common::AsyncResult& asyncResult, uint32_t /*timeoutMsec*/, uint32_t /*flags*/ ) {
		/*HT4C_TRY {
			Hypertable::OdbcGen::ScanSpec _scanSpec;
			convertScanSpec( scanSpec, _scanSpec );
			OdbcEnvLock sync( table->getEnv() );
			Hypertable::OdbcGen::Future future = typeid(asyncResult) != typeid(OdbcBlockingAsyncResult)
																					 ? static_cast<OdbcAsyncResult&>(asyncResult).get(env)
																					 : static_cast<OdbcBlockingAsyncResult&>(asyncResult).get(env);

			return OdbcAsyncTableScanner::create( table->async_createScanner(future, _scanSpec) );
		}
		HT4C_ODBC_RETHROW */
		HT4C_THROW_NOTIMPLEMENTED();
	}

	int64_t OdbcTable::createAsyncScannerId( Common::ScanSpec& scanSpec, Common::AsyncResult& asyncResult, uint32_t /*timeoutMsec*/, uint32_t /*flags*/ ) {
		/*HT4C_TRY {
			Hypertable::OdbcGen::ScanSpec _scanSpec;
			convertScanSpec( scanSpec, _scanSpec );
			OdbcEnvLock sync( table->getEnv() );
			Hypertable::OdbcGen::Future future = typeid(asyncResult) != typeid(OdbcBlockingAsyncResult)
																					 ? static_cast<OdbcAsyncResult&>(asyncResult).get(env)
																					 : static_cast<OdbcBlockingAsyncResult&>(asyncResult).get(env);

			return OdbcAsyncTableScanner::id( env->async_createScanner(ns, name, future, _scanSpec) );
		}
		HT4C_ODBC_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	std::string OdbcTable::getSchema( bool withIds ) {
		HT4C_TRY {
			std::string schema;
			OdbcEnvLock sync( table->getEnv() );
			table->getTableSchema( withIds, schema );
			return schema;
		}
		HT4C_ODBC_RETHROW
	}

	OdbcTable::OdbcTable( Db::TablePtr _table )
	: table( _table )
	{
	}

} }