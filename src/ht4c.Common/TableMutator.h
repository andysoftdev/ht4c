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

#pragma once

#ifdef __cplusplus_cli
#pragma managed( push, off )
#endif

#include "Types.h"

namespace ht4c { namespace Common {

	class Cells;

	/// <summary>
	/// Abstract class represents a table mutator.
	/// </summary>
	class TableMutator {

		public:

			/// <summary>
			/// Destroys the TableMutator instance.
			/// </summary>
			virtual ~TableMutator( ) { }

			/// <summary>
			/// Inserts a new cell into the table.
			/// </summary>
			/// <param name="row">Row key, mandatory</param>
			/// <param name="columnFamily">Column family, mandatory</param>
			/// <param name="columnQualifier">Column qualifier, might be NULL</param>
			/// <param name="timestamp">Timestamp, auto-assigned if 0</param>
			/// <param name="value">Cell value, might be NULL</param>
			/// <param name="valueLength">Cell value length</param>
			/// <param name="flag">Cell flag</param>
			virtual void set( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, uint8_t flag ) = 0;

			/// <summary>
			/// Inserts a new cell into the table, auto generate the row key.
			/// </summary>
			/// <param name="columnFamily">Column family, mandatory</param>
			/// <param name="columnQualifier">Column qualifier, might be NULL</param>
			/// <param name="timestamp">Timestamp, auto-assigned if 0</param>
			/// <param name="value">Cell value, might be NULL</param>
			/// <param name="valueLength">Cell value length</param>
			/// <param name="row">Receives the auto generated row key, base85 encode GUID</param>
			virtual void set( const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, std::string& row ) = 0;

			/// <summary>
			/// Inserts a cell collection into the table.
			/// </summary>
			/// <param name="cells">Cell collection</param>
			virtual void set( const Cells& cells ) = 0;

			/// <summary>
			/// Deletes an entire row, a column family in a particular row, or a specific cell within a row.
			/// </summary>
			/// <param name="row">Row key, mandatory</param>
			/// <param name="columnFamily">Column family, might be NULL</param>
			/// <param name="columnQualifier">Column qualifier, might be NULL</param>
			/// <param name="timestamp">Timestamp, if specified cells older or equal will be deleted</param>
			virtual void del( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp ) = 0;

			/// <summary>
			/// Flushes the accumulated mutations.
			/// </summary>
			virtual void flush( ) = 0;

		protected:

			/// <summary>
			/// Creates a new TableMutator instance.
			/// </summary>
			TableMutator( ) { }

		private:

			TableMutator( const TableMutator& ) { }
			TableMutator& operator = ( const TableMutator& ) { return *this; }
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif