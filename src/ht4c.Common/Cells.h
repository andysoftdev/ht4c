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

#include "Cell.h"

namespace ht4c { namespace Common {

	/// <summary>
	/// Represents a Hypertable cell collection.
	/// </summary>
	/// <remarks>Links between native and C++/CLI.</remarks>
	class Cells {

		public:

			/// <summary>
			/// Creates a new Cells instance.
			/// </summary>
			/// <param name="reserve">Initial capacity</param>
			/// <returns>New Cells instance</returns>
			/// <remarks>To free the created instance, the delete operator.</remarks>
			static Cells* create( int reserve );

			#ifndef __cplusplus_cli

			/// <summary>
			/// Initializes a new instance of the Cells class, using the specified cells collection.
			/// </summary>
			/// <param name="cells">Hypertable cells collection</param>
			/// <remarks>Pure native constructor.</remarks>
			explicit Cells( Hypertable::Cells* cells );

			/// <summary>
			/// Initializes a new instance of the Cells class with the specified initial capacity.
			/// </summary>
			/// <param name="reserve">Initial capacity</param>
			/// <remarks>Pure native constructor.</remarks>
			explicit Cells( int reserve );

			/// <summary>
			/// Returns a reference to the underlying Hypertable cells.
			/// </summary>
			/// <returns>Reference to the underlying Hypertable cells</returns>
			/// <remarks>Pure native method.</remarks>
			Hypertable::Cells& get() {
				return *cells;
			}

			/// <summary>
			/// Returns the underlying Hypertable cells.
			/// </summary>
			/// <returns>Underlying Hypertable cells</returns>
			/// <remarks>Pure native method.</remarks>
			inline const Hypertable::Cells& get() const {
				return *cells;
			}

			/// <summary>
			/// Returns the underlying Hypertable cells builder.
			/// </summary>
			/// <returns>Underlying Hypertable cells builder</returns>
			/// <remarks>Pure native method.</remarks>
			inline Hypertable::CellsBuilder& builder() {
				return *cellsBuilder;
			}

			/// <summary>
			/// Returns a Hypertable cell to the end of the collection.
			/// </summary>
			/// <param name="reserve">Cell to add</param>
			/// <remarks>Pure native method.</remarks>
			void add( const Hypertable::Cell& cell );

			#endif

			/// <summary>
			/// Destroys the Cells instance.
			/// </summary>
			virtual ~Cells( );

			/// <summary>
			/// Returns the number of cells actually contained in the collection.
			/// </summary>
			/// <returns>Number of cells actually contained in the collection</returns>
			size_t size( ) const;

			/// <summary>
			/// Returns a cell, constructed using the spoecified attributes, to the end of the collection.
			/// </summary>
			/// <param name="row">Row key, mandatory</param>
			/// <param name="columnFamily">Column family, mandatory</param>
			/// <param name="columnQualifier">Column qualifier, might be NULL</param>
			/// <param name="timestamp">Timestamp, auto-assigned if 0</param>
			/// <param name="value">Cell value, might be NULL</param>
			/// <param name="valueLength">Cell value length</param>
			/// <param name="flag">Cell flag</param>
			void add( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, uint8_t flag );

			/// <summary>
			/// Gets a cell at the specified index.
			/// </summary>
			/// <param name="n">Collection index</param>
			/// <param name="cell">Receives the cell at the specified index</param>
			/// <returns>true if succeeded</returns>
			bool get( size_t n, Cell* cell ) const;

			/// <summary>
			/// Gets a cell at the specified index, without validating input paramters nor index bounderies.
			/// </summary>
			/// <param name="n">Collection index</param>
			/// <param name="cell">Receives the cell at the specified index</param>
			/// <returns>true if succeeded</returns>
			void get_unchecked( size_t n, Cell* cell ) const;

			/// <summary>
			/// Removes all cells from the collection.
			/// </summary>
			void clear( );

		private:

			Cells( ) { }
			Cells( const Cells& ) { }
			Cells& operator = ( const Cells& ) { return *this; }

			#ifndef __cplusplus_cli

			Hypertable::CellsBuilderPtr cellsBuilder;
			Hypertable::Cells* cells;
			int initialCapacity;

			#endif
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif