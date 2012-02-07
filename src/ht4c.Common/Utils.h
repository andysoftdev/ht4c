/** -*- C++ -*-
 * Copyright (C) 2010-2012 Thalmann Software & Consulting, http://www.softdev.ch
 *
 * This file is part of ht4n.
 *
 * ht4n is free software; you can redistribute it and/or
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

#pragma once

#ifdef __cplusplus_cli
#pragma managed( push, off )
#endif

#include "Types.h"

namespace ht4c { namespace Common {

	/// <summary>
	/// Returns the column family if not NULL or empty.
	/// </summary>
	inline const char* CF( const char* columnFamily ) {
		return  columnFamily && *columnFamily
					? columnFamily
					: 0;
	}

	#ifdef HYPERTABLE_KEYSPEC_H

	/// <summary>
	/// Returns the adjusted keyspec flag, according to the specified column family/qualifier.
	/// </summary>
	inline uint8_t FLAG( const char* columnFamily, const char* columnQualifier, uint8_t flag ) {
		if( flag != Hypertable::FLAG_INSERT ) {
			if( flag >= Hypertable::FLAG_DELETE_COLUMN_FAMILY ) {
				if( !CF(columnFamily) ) {
					return Hypertable::FLAG_DELETE_ROW;
				}
				else if( flag >= Hypertable::FLAG_DELETE_CELL && !columnQualifier ) {
					return Hypertable::FLAG_DELETE_COLUMN_FAMILY;
				}
			}
		}
		return flag;
	}

	/// <summary>
	/// Returns the appropriate keyspec DELETE flag, according to the specified column family/qualifier.
	/// </summary>
	inline uint8_t FLAG_DELETE( const char* columnFamily, const char* columnQualifier ) {
		return !CF(columnFamily)
					? Hypertable::FLAG_DELETE_ROW
					: columnQualifier
					? Hypertable::FLAG_DELETE_CELL
					: Hypertable::FLAG_DELETE_COLUMN_FAMILY;
	}

	/// <summary>
	/// Returns the adjusted timestamp, according to the specified timestamp and keyspec flag.
	/// </summary>
	inline uint64_t TIMESTAMP( uint64_t timestamp, uint8_t flag ) {
		return  timestamp == 0
					? Hypertable::AUTO_ASSIGN
					: flag == Hypertable::FLAG_INSERT || Hypertable::FLAG_DELETE_CELL_VERSION
					? timestamp
					: timestamp + 1;
	}

	#endif

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif