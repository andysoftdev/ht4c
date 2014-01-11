/** -*- C++ -*-
 * Copyright (C) 2010-2014 Thalmann Software & Consulting, http://www.softdev.ch
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
#error compile native
#endif

#include "SQLiteClient.h"
#include "ht4c.Common/Types.h"
#include "ht4c.Common/Cells.h"
#include "ht4c.Common/AsyncTableMutator.h"

namespace ht4c { namespace SQLite {

	/// <summary>
	/// Represents a Hypertable asynchronous mutator, using the sqlite API.
	/// </summary>
	/// <seealso cref="ht4c::Common::TableMutator"/>
	class SQLiteAsyncTableMutator : public Common::AsyncTableMutator {

		public:

			/// <summary>
			/// Creates a new SQLiteAsyncTableMutator instance.
			/// </summary>
			/// <param name="tableMutator">SQLite mutator</param>
			/// <returns>New SQLiteAsyncTableMutator instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Common::AsyncTableMutator* create( Db::MutatorAsyncPtr tableMutator );

			/// <summary>
			/// Returns the mutator identifier for the specified asynchronous mutator.
			/// </summary>
			/// <param name="tableMutator">Hypertable mutator</param>
			/// <returns>Asynchronous mutator identifier</returns>
			static int64_t id( const Db::MutatorAsyncPtr& tableMutator );

			/// <summary>
			/// Destroys the SQLiteAsyncTableMutator instance.
			/// </summary>
			virtual ~SQLiteAsyncTableMutator( );

			#pragma region Common::AsyncTableMutator methods

			virtual int64_t id( ) const;
			virtual void set( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, uint8_t flag );
			virtual void set( const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, std::string& row );
			virtual void set( const Common::Cells& cells );
			virtual void del( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp );
			virtual void flush( );

			#pragma endregion

		private:

			SQLiteAsyncTableMutator( Db::MutatorAsyncPtr tableMutator );

			SQLiteAsyncTableMutator( ) { }
			SQLiteAsyncTableMutator( const SQLiteAsyncTableMutator& ) { }
			SQLiteAsyncTableMutator& operator = ( const SQLiteAsyncTableMutator& ) { return *this; }

			Db::MutatorAsyncPtr tableMutator;
	};

} }