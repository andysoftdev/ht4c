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
#include "OdbcTableScanner.h"
#include "OdbcException.h"

namespace ht4c { namespace Odbc {

	Common::TableScanner* OdbcTableScanner::create( Db::ScannerPtr tableScanner ) {
		HT4C_TRY {
			return new OdbcTableScanner( tableScanner );
		}
		HT4C_ODBC_RETHROW
	}

	OdbcTableScanner::~OdbcTableScanner( ) {
		tableScanner = 0;
	}

	bool OdbcTableScanner::next( Common::Cell*& _cell ) {
		HT4C_TRY {
			if( tableScanner->nextCell(cell.get()) ) {
				_cell = &cell;
				return true;
			}
			_cell = 0;
			return false;
		}
		HT4C_ODBC_RETHROW
	}

	OdbcTableScanner::OdbcTableScanner( Db::ScannerPtr _tableScanner )
	: tableScanner( _tableScanner )
	{
	}

} }