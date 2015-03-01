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
#include "HyperTable.h"
#include "HyperTableMutator.h"
#include "HyperAsyncTableMutator.h"
#include "HyperTableScanner.h"
#include "HyperAsyncTableScanner.h"
#include "HyperAsyncResult.h"

#include "ht4c.Common/ScanSpec.h"
#include "ht4c.Common/Exception.h"


namespace ht4c { namespace Hyper {

	Common::Table* HyperTable::create( Hypertable::TablePtr table ) {
		HT4C_TRY {
			return new HyperTable( table );
		}
		HT4C_RETHROW
	}


	HyperTable::~HyperTable( ) {
		HT4C_TRY {
			table = 0;
		}
		HT4C_RETHROW
	}

	std::string HyperTable::getName( ) const {
		HT4C_TRY {
			return table->get_name();
		}
		HT4C_RETHROW
	}

	Common::TableMutator* HyperTable::createMutator( uint32_t timeoutMsec, uint32_t flags, uint32_t flushIntervalMsec ) {
		HT4C_TRY {
			return HyperTableMutator::create( table->create_mutator(timeoutMsec, flags, flushIntervalMsec) );
		}
		HT4C_RETHROW
	}

	Common::AsyncTableMutator* HyperTable::createAsyncMutator( Common::AsyncResult& asyncResult, uint32_t timeoutMsec, uint32_t flags ) {
		HT4C_TRY {
			return HyperAsyncTableMutator::create( table->create_mutator_async(&static_cast<HyperAsyncResult&>(asyncResult).get(), timeoutMsec, flags) );
		}
		HT4C_RETHROW
	}

	Common::TableScanner* HyperTable::createScanner( Common::ScanSpec& scanSpec, uint32_t timeoutMsec, uint32_t flags ) {
		HT4C_TRY {
			return HyperTableScanner::create( table->create_scanner(scanSpec.get(), timeoutMsec, flags) );
		}
		HT4C_RETHROW
	}

	Common::AsyncTableScanner* HyperTable::createAsyncScanner( Common::ScanSpec& scanSpec, Common::AsyncResult& asyncResult, uint32_t timeoutMsec, uint32_t flags ) {
		HT4C_TRY {
			return HyperAsyncTableScanner::create( table->create_scanner_async( &static_cast<HyperAsyncResult&>(asyncResult).get(), scanSpec.get(), timeoutMsec, flags) );
		}
		HT4C_RETHROW
	}

	int64_t HyperTable::createAsyncScannerId( Common::ScanSpec& scanSpec, Common::AsyncResult& asyncResult, uint32_t timeoutMsec, uint32_t flags ) {
		HT4C_TRY {
			return HyperAsyncTableScanner::id( table->create_scanner_async( &static_cast<HyperAsyncResult&>(asyncResult).get(), scanSpec.get(), timeoutMsec, flags) );
		}
		HT4C_RETHROW
	}

	std::string HyperTable::getSchema( bool withIds ) {
		HT4C_TRY {
			Hypertable::String str;
			str = table->schema()->render_xml( withIds );
			return str;
		}
		HT4C_RETHROW
	}

	HyperTable::HyperTable( Hypertable::TablePtr _table )
	: table( _table )
	{
	}

} }