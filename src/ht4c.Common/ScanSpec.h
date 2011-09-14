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

#pragma once

#ifdef __cplusplus_cli
#pragma managed( push, off )
#endif

#include "Types.h"

namespace ht4c { namespace Common {

	/// <summary>
	/// Represents a Hypertable scan specification.
	/// </summary>
	/// <remarks>Links between native and C++/CLI.</remarks>
	class ScanSpec {

		public:

			/// <summary>
			/// Creates a new ScanSpec instance.
			/// </summary>
			/// <returns>New ScanSpec instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static ScanSpec* create( );

			#ifndef __cplusplus_cli

			/// <summary>
			/// Returns a reference to the underlying Hypertable scan specification.
			/// </summary>
			/// <returns>Reference to the underlying Hypertable scan specification</returns>
			/// <remarks>Pure native method.</remarks>
			inline Hypertable::ScanSpec& get() {
				return scanSpecBuilder.get();
			}

			#endif

			/// <summary>
			/// Destroys the ScanSpec instance.
			/// </summary>
			virtual ~ScanSpec( );

			/// <summary>
			/// Sets the maximum number of rows to return in the scan.
			/// </summary>
			/// <param name="maxRows">Maximum number of rows to return in the scan</param>
			void maxRows( int maxRows );

			/// <summary>
			/// Sets the maximum number of revisions of each cell to return in the scan.
			/// </summary>
			/// <param name="maxVersion">Maximum number of revisions of each cell to return in the scan</param>
			void maxVersions( int maxVersion );

			/// <summary>
			/// Sets the maximum number of cells to return per column family, per row.
			/// </summary>
			/// <param name="maxCells">Maximum number of cells to return per column family, per row</param>
			void maxCells( int maxCells );

			/// <summary>
			/// If set scans return only keys, no cell values.
			/// </summary>
			/// <param name="keysOnly">If true scans return onlt keys</param>
			void keysOnly( bool keysOnly );

			/// <summary>
			/// If set scans use scan and filter rows to get many individual rows.
			/// </summary>
			/// <param name="scanAndFilter">Use scan and filter rows</param>
			void scanAndFilter( bool scanAndFilter );

			/// <summary>
			/// Sets the start time of time interval for the scan.
			/// </summary>
			/// <param name="start">Start time of time interval</param>
			/// <remarks>Time values represent number of nanoseconds from 1970-01-00 00:00:00.0 UTC</remarks>
			void startTimestamp( uint64_t start );

			/// <summary>
			/// Sets the end time of time interval for the scan.
			/// </summary>
			/// <param name="end">End time of time interval</param>
			/// <remarks>Time values represent number of nanoseconds from 1970-01-00 00:00:00.0 UTC</remarks>
			void endTimestamp( uint64_t end );

			/// <summary>
			/// Sets the regexp to filter by row keys.
			/// </summary>
			/// <param name="regex">Row key regexp</param>
			void rowRegex( const char* regex );

			/// <summary>
			/// Sets the regexp to filter by cell values.
			/// </summary>
			/// <param name="regex">Cell value regexp</param>
			void valueRegex( const char* regex );

			/// <summary>
			/// Sets the capacity for a bunch of row keys.
			/// </summary>
			/// <param name="reserveRows">Capacity to reserve</param>
			void reserveRows( int reserveRows );

			/// <summary>
			/// Add row key to be included in the scan.
			/// </summary>
			/// <param name="row">Row key</param>
			void addRow( const char* row );

			/// <summary>
			/// Sets the capacity for a bunch of column.
			/// </summary>
			/// <param name="reserveColumns">Capacity to reserve</param>
			void reserveColumns( int reserveColumns );

			/// <summary>
			/// Add column to be included in the scan.
			/// </summary>
			/// <param name="column">Column</param>
			/// <remarks>Column is defined as column family[:column qualifier]</remarks>
			void addColumn( const char* column );

			/// <summary>
			/// Sets the capacity for a bunch of cells.
			/// </summary>
			/// <param name="reserveCells">Capacity to reserve</param>
			void reserveCells( int reserveCells );

			/// <summary>
			/// Add particular cell to be included in the scan.
			/// </summary>
			/// <param name="row">Row key</param>
			/// <param name="column">Column</param>
			/// <remarks>Column is defined as column family[:column qualifier]</remarks>
			void addCell( const char* row, const char* column );

			/// <summary>
			/// Add row interval to be included in the scan.
			/// </summary>
			/// <param name="startRow">Start row key</param>
			/// <param name="includeStartRow">If true the start row will be include</param>
			/// <param name="endRow">Start row key</param>
			/// <param name="includeEndRow">If true the start row will be include</param>
			void addRowInterval( const char* startRow, bool includeStartRow, const char* endRow, bool includeEndRow );

			/// <summary>
			/// Add cell interval to be included in the scan.
			/// </summary>
			/// <param name="startRow">Start row key</param>
			/// <param name="startColumn">Start column</param>
			/// <param name="includeStartRow">If true the start row will be include</param>
			/// <param name="endRow">Start row key</param>
			/// <param name="endColumn">End column</param>
			/// <param name="includeEndRow">If true the start row will be include</param>
			/// <remarks>Column is defined as column family[:column qualifier]</remarks>
			void addCellInterval( const char* startRow, const char* startColumn, bool includeStartRow, const char* endRow, const char* endColumn, bool includeEndRow );

			/// <summary>
			/// Reset scan definition.
			/// </summary>
			void clear( );

		private:

			ScanSpec( );
			ScanSpec( const ScanSpec& ) { }
			ScanSpec& operator = ( const ScanSpec& ) { return *this; }

			#ifndef __cplusplus_cli
	
			Hypertable::ScanSpecBuilder scanSpecBuilder;

			#endif
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif