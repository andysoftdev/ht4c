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
#include "HyperTableScanner.h"

namespace ht4c { namespace Hyper {

	Common::TableScanner* HyperTableScanner::create( Hypertable::TableScanner* tableScanner ) {
		HT4C_TRY {
			return new HyperTableScanner( tableScanner );
		}
		HT4C_RETHROW
	}


	HyperTableScanner::~HyperTableScanner( ) throw(ht4c::Common::HypertableException) {
		HT4C_TRY {
			tableScanner = 0;
		}
		HT4C_RETHROW
	}

	bool HyperTableScanner::next( Common::Cell*& _cell ) {
		HT4C_TRY {
			if( tableScanner->next(cell.get()) ) {
				_cell = &cell;
				return true;
			}
			_cell = 0;
			return false;
		}
		HT4C_RETHROW
	}

	HyperTableScanner::HyperTableScanner( Hypertable::TableScanner* _tableScanner )
	: tableScanner( _tableScanner )
	{
	}

} }