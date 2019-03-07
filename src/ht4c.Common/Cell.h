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

#pragma once

#ifdef __cplusplus_cli
#pragma managed( push, off )
#endif

#include "Types.h"
#include "CellFlag.h" // satisfies /doc

namespace ht4c { namespace Common {

	/// <summary>
	/// Represents a Hypertable cell, provide accessors to the cell attributes.
	/// </summary>
	/// <remarks>Links between native and C++/CLI.</remarks>
	class Cell {

		public:

			/// <summary>
			/// Max cell size.
			/// </summary>
			static const uint32_t MaxSize = 2000000000;

			/// <summary>
			/// Creates a new Cell instance.
			/// </summary>
			/// <returns>New Cell instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Cell* create( );

			#ifndef __cplusplus_cli

			/// <summary>
			/// Initializes a new instance of the Cell class.
			/// </summary>
			/// <remarks>Pure native constructor.</remarks>
			Cell( ) { }

			/// <summary>
			/// Initializes a new instance of the Cell class, using the specified cell attributes.
			/// </summary>
			/// <param name="row">Row key, mandatory</param>
			/// <param name="columnFamily">Column family, mandatory</param>
			/// <param name="columnQualifier">Column qualifier, might be NULL</param>
			/// <param name="timestamp">Timestamp, auto-assigned if 0</param>
			/// <param name="value">Cell value, might be NULL</param>
			/// <param name="valueLength">Cell value length</param>
			/// <param name="flag">Cell flag</param>
			/// <remarks>Pure native constructor.</remarks>
			/// <seealso cref="ht4c::Common::CellFlag"/>
			Cell( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, uint8_t flag );

			/// <summary>
			/// Returns a reference to the underlying Hypertable cell.
			/// </summary>
			/// <returns>Reference to the underlying Hypertable cell</returns>
			/// <remarks>Pure native method.</remarks>
			inline Hypertable::Cell& get() {
				return cell;
			}

			/// <summary>
			/// Returns the underlying Hypertable cell.
			/// </summary>
			/// <returns>Underlying Hypertable cell</returns>
			/// <remarks>Pure native method.</remarks>
			inline const Hypertable::Cell& get() const {
				return cell;
			}

			#endif

			/// <summary>
			/// Destroys the Cell instance.
			/// </summary>
			virtual ~Cell( ) { }

			/// <summary>
			/// Returns the cell's row key.
			/// </summary>
			/// <returns>Cell's row key</returns>
			const char* row( ) const;

			/// <summary>
			/// Returns the cell's column family.
			/// </summary>
			/// <returns>Cell's column family</returns>
			const char* columnFamily( ) const;

			/// <summary>
			/// Returns the cell's column qualifier.
			/// </summary>
			/// <returns>Cell's column qualifier</returns>
			/// <remarks>Might be NULL</remarks>
			const char* columnQualifier( ) const;

			/// <summary>
			/// Returns the cell's timestamp.
			/// </summary>
			/// <returns>Cell's timestamp</returns>
			/// <remarks>Nanoseconds since 1970-01-01 00:00:00.0 UTC</remarks>
			uint64_t timestamp( ) const;

			/// <summary>
			/// Returns the cell's value.
			/// </summary>
			/// <returns>Cell's value</returns>
			/// <remarks>Might be NULL</remarks>
			const uint8_t* value( ) const;

			/// <summary>
			/// Returns the cell's value length.
			/// </summary>
			/// <returns>Cell's value length</returns>
			size_t valueLength( ) const;

			/// <summary>
			/// Returns the cell's flag.
			/// </summary>
			/// <returns>Cell's flag</returns>
			/// <seealso cref="ht4c::Common::CellFlag"/>
			uint8_t flag( ) const;

		private:

			Cell( const Cell& ) { }
			Cell& operator = ( const Cell& ) { return *this; }

			#ifndef __cplusplus_cli

			Hypertable::Cell cell;

			#endif
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif