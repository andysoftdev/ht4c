/** -*- C++ -*-
 * Copyright (C) 2010-2012 Thalmann Software & Consulting, http://www.softdev.ch
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
#include "Cell.h"

namespace ht4c { namespace Common {

	Cell* Cell::create( ) {
		return new Cell();
	}

	Cell::Cell( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, uint8_t flag ) 
	: cell( row, CF(columnFamily), columnQualifier, timestamp, (uint64_t)Hypertable::AUTO_ASSIGN, (uint8_t*)value, valueLength, flag )
	{
	}

	const char* Cell::row( ) const {
		return cell.row_key;
	}

	const char* Cell::columnFamily() const {
		return cell.column_family;
	}

	const char* Cell::columnQualifier() const {
		return cell.column_qualifier;
	}

	uint64_t Cell::timestamp() const {
		return cell.timestamp;
	}

	const uint8_t* Cell::value() const {
		return cell.value;
	}

	size_t Cell::valueLength() const {
		return cell.value_len;
	}

	uint8_t Cell::flag() const {
		return cell.flag;
	}

} }