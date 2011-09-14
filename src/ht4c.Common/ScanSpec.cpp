/** -*- C++ -*-
 * Copyright (C) 2011 Andy Thalmann
 *
 * This file is part of ht4c.
 *
 * ht4c is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
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
#include "ScanSpec.h"
#include "Exception.h"

namespace ht4c { namespace Common {

	ScanSpec::ScanSpec( )
	: scanSpecBuilder( )
	{
	}

	ScanSpec::~ScanSpec( )
	{
	}

	ScanSpec* ScanSpec::create( ) {
		HT4C_TRY {
			return new ScanSpec( );
		}
		HT4C_RETHROW
	}

	void ScanSpec::maxRows( int maxRows ) {
		scanSpecBuilder.set_row_limit( maxRows );
	}

	void ScanSpec::maxVersions( int maxVersion ) {
		scanSpecBuilder.set_max_versions( maxVersion );
	}

	void ScanSpec::maxCells( int maxCells ) {
		scanSpecBuilder.set_cell_limit( maxCells );
	}

	void ScanSpec::keysOnly( bool keysOnly ) {
		scanSpecBuilder.set_keys_only( keysOnly );
	}

	void ScanSpec::scanAndFilter( bool scanAndFilter ) {
		scanSpecBuilder.set_scan_and_filter_rows( scanAndFilter );
	}

	void ScanSpec::startTimestamp( uint64_t start ) {
		scanSpecBuilder.set_start_time( start );
	}

	void ScanSpec::endTimestamp( uint64_t end ) {
		scanSpecBuilder.set_end_time( end );
	}

	void ScanSpec::rowRegex( const char* regex ) {
		scanSpecBuilder.set_row_regexp( regex );
	}

	void ScanSpec::valueRegex( const char* regex ) {
		scanSpecBuilder.set_value_regexp( regex );
	}

	void ScanSpec::reserveRows( int reserveRows ) {
		scanSpecBuilder.reserve_rows( reserveRows );
	}

	void ScanSpec::addRow( const char* row ) {
		if( row && *row ) {
			scanSpecBuilder.add_row( row );
		}
	}

	void ScanSpec::reserveColumns( int reserveColumns ) {
		scanSpecBuilder.reserve_columns( reserveColumns );
	}

	void ScanSpec::addColumn( const char* column ) {
		if( column && *column ) {
			scanSpecBuilder.add_column( column );
		}
	}

	void ScanSpec::reserveCells( int reserveCells ) {
		scanSpecBuilder.reserve_cells( reserveCells );
	}

	void ScanSpec::addCell( const char* row, const char* column ) {
		if( row && *row ) {
			if( column && *column ) {
				scanSpecBuilder.add_cell( row, column );
			}
			else {
				scanSpecBuilder.add_row( row );
			}
		}
	}

	void ScanSpec::addRowInterval( const char* startRow, bool includeStartRow, const char* endRow, bool includeEndRow ) {
		if(  startRow && *startRow
			&& endRow && *endRow ) {

			scanSpecBuilder.add_row_interval( startRow, includeStartRow, endRow, includeEndRow );
		}
	}

	void ScanSpec::addCellInterval( const char* startRow, const char* startColumn, bool includeStartRow, const char* endRow, const char* endColumn, bool includeEndRow ) {
		if(  startRow && *startRow
			&& startColumn && *startColumn
			&& endRow && *endRow
			&& endColumn && *endColumn ) {

			scanSpecBuilder.add_cell_interval( startRow, startColumn, includeStartRow, endRow, endColumn, includeEndRow );
		}
	}

	void ScanSpec::clear( ) {
		scanSpecBuilder.clear();
	}

} }