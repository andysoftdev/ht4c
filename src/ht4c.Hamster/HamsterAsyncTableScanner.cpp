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
#include "HamsterAsyncTableScanner.h"
#include "HamsterException.h"

namespace ht4c { namespace Hamster {

	Common::AsyncTableScanner* HamsterAsyncTableScanner::create( Db::ScannerAsyncPtr tableScanner ) {
		HT4C_TRY {
			return new HamsterAsyncTableScanner( tableScanner );
		}
		HT4C_HAMSTER_RETHROW
	}

	int64_t HamsterAsyncTableScanner::id( const Db::ScannerAsyncPtr& tableScanner ) {
		return int64_t(tableScanner.get());
	}

	HamsterAsyncTableScanner::~HamsterAsyncTableScanner( ) {
		HT4C_TRY {
			tableScanner = 0;
		}
		HT4C_HAMSTER_RETHROW
	}

	int64_t HamsterAsyncTableScanner::id( ) const {
		return id( tableScanner );
	}

	HamsterAsyncTableScanner::HamsterAsyncTableScanner( Db::ScannerAsyncPtr _tableScanner )
	: tableScanner( _tableScanner )
	{
	}

} }