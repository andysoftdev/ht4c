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
#include "HamsterTable.h"
#include "HamsterNamespace.h"
#include "HamsterTableMutator.h"
#include "HamsterAsyncTableMutator.h"
#include "HamsterTableScanner.h"
#include "HamsterAsyncTableScanner.h"
#include "HamsterAsyncResult.h"
#include "HamsterBlockingAsyncResult.h"
#include "HamsterException.h"

#include "ht4c.Common/ScanSpec.h"

namespace ht4c { namespace Hamster {

	Common::Table* HamsterTable::create( Db::TablePtr table ) {
		HT4C_TRY {
			return new HamsterTable( table );
		}
		HT4C_HAMSTER_RETHROW
	}

	HamsterTable::~HamsterTable( ) {
		HT4C_TRY {
			table = 0;
		}
		HT4C_HAMSTER_RETHROW
	}

	std::string HamsterTable::getName( ) const {
		return table->getFullName();
	}

	Common::TableMutator* HamsterTable::createMutator( uint32_t /*timeoutMsec*/, uint32_t flags, uint32_t flushIntervalMsec ) {
		HT4C_TRY {
			HamsterEnvLock sync( table->getEnv() );
			return HamsterTableMutator::create( table->createMutator(flags, flushIntervalMsec) );
		}
		HT4C_HAMSTER_RETHROW
	}

	Common::AsyncTableMutator* HamsterTable::createAsyncMutator( Common::AsyncResult& asyncResult, uint32_t /*timeoutMsec*/, uint32_t flags ) {
		/*HT4C_TRY {
			HamsterEnvLock sync( table->getEnv() );
			TODO Hypertable::HamsterGen::Future future = typeid(asyncResult) != typeid(HamsterBlockingAsyncResult)
																					 ? static_cast<HamsterAsyncResult&>(asyncResult).get(env)
																					 : static_cast<HamsterBlockingAsyncResult&>(asyncResult).get(env);
			return HamsterAsyncTableMutator::create( table->async_createMutator(future, flags) );
		}
		HT4C_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	Common::TableScanner* HamsterTable::createScanner( Common::ScanSpec& scanSpec, uint32_t /*timeoutMsec*/, uint32_t flags ) {
		HT4C_TRY {
			HamsterEnvLock sync( table->getEnv() );
			return HamsterTableScanner::create( table->createScanner(scanSpec.get(), flags) );
		}
		HT4C_HAMSTER_RETHROW
	}

	Common::AsyncTableScanner* HamsterTable::createAsyncScanner( Common::ScanSpec& scanSpec, Common::AsyncResult& asyncResult, uint32_t /*timeoutMsec*/, uint32_t /*flags*/ ) {
		/*HT4C_TRY {
			Hypertable::HamsterGen::ScanSpec _scanSpec;
			convertScanSpec( scanSpec, _scanSpec );
			HamsterEnvLock sync( table->getEnv() );
			Hypertable::HamsterGen::Future future = typeid(asyncResult) != typeid(HamsterBlockingAsyncResult)
																					 ? static_cast<HamsterAsyncResult&>(asyncResult).get(env)
																					 : static_cast<HamsterBlockingAsyncResult&>(asyncResult).get(env);

			return HamsterAsyncTableScanner::create( table->async_createScanner(future, _scanSpec) );
		}
		HT4C_HAMSTER_RETHROW */
		HT4C_THROW_NOTIMPLEMENTED();
	}

	int64_t HamsterTable::createAsyncScannerId( Common::ScanSpec& scanSpec, Common::AsyncResult& asyncResult, uint32_t /*timeoutMsec*/, uint32_t /*flags*/ ) {
		/*HT4C_TRY {
			Hypertable::HamsterGen::ScanSpec _scanSpec;
			convertScanSpec( scanSpec, _scanSpec );
			HamsterEnvLock sync( table->getEnv() );
			Hypertable::HamsterGen::Future future = typeid(asyncResult) != typeid(HamsterBlockingAsyncResult)
																					 ? static_cast<HamsterAsyncResult&>(asyncResult).get(env)
																					 : static_cast<HamsterBlockingAsyncResult&>(asyncResult).get(env);

			return HamsterAsyncTableScanner::id( env->async_createScanner(ns, name, future, _scanSpec) );
		}
		HT4C_HAMSTER_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	std::string HamsterTable::getSchema( bool withIds ) {
		HT4C_TRY {
			std::string schema;
			HamsterEnvLock sync( table->getEnv() );
			table->getTableSchema( withIds, schema );
			return schema;
		}
		HT4C_HAMSTER_RETHROW
	}

	HamsterTable::HamsterTable( Db::TablePtr _table )
	: table( _table )
	{
	}

} }