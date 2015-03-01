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
#include "HyperAsyncTableScanner.h"

#include "ht4c.Common/Exception.h"

namespace ht4c { namespace Hyper {

	Common::AsyncTableScanner* HyperAsyncTableScanner::create( Hypertable::TableScannerAsync* tableScanner ) {
		HT4C_TRY {
			return new HyperAsyncTableScanner( tableScanner );
		}
		HT4C_RETHROW
	}

	int64_t HyperAsyncTableScanner::id( Hypertable::TableScannerAsync* tableScanner ) {
		return reinterpret_cast<int64_t>( tableScanner );
	}

	HyperAsyncTableScanner::~HyperAsyncTableScanner( ) {
		HT4C_TRY {
			tableScanner = 0;
		}
		HT4C_RETHROW
	}

	int64_t HyperAsyncTableScanner::id( ) const {
		return id( tableScanner.get() );
	}

	HyperAsyncTableScanner::HyperAsyncTableScanner( Hypertable::TableScannerAsync* _tableScanner )
	: tableScanner( _tableScanner )
	{
	}

} }