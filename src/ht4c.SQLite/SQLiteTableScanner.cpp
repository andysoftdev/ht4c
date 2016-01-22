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
#include "SQLiteTableScanner.h"
#include "SQLiteException.h"

namespace ht4c { namespace SQLite {

	Common::TableScanner* SQLiteTableScanner::create( Db::ScannerPtr tableScanner ) {
		HT4C_TRY {
			return new SQLiteTableScanner( tableScanner );
		}
		HT4C_SQLITE_RETHROW
	}


	SQLiteTableScanner::~SQLiteTableScanner( ) {
		tableScanner = 0;
	}

	bool SQLiteTableScanner::next( Common::Cell*& _cell ) {
		HT4C_TRY {
			if( tableScanner->nextCell(cell.get()) ) {
				_cell = &cell;
				return true;
			}
			_cell = 0;
			return false;
		}
		HT4C_SQLITE_RETHROW
	}

	SQLiteTableScanner::SQLiteTableScanner( Db::ScannerPtr _tableScanner )
	: tableScanner( _tableScanner )
	{
	}

} }