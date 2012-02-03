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
#include "ContextKind.h"
#include "AsyncResultSink.h"
#include "MutatorFlags.h" // satisfies /doc
#include "ScannerFlags.h" // satisfies /doc

namespace ht4c { namespace Common {

	class TableMutator;
	class AsyncTableMutator;
	class TableScanner;
	class ScanSpec;
	class AsyncResult;

	/// <summary>
	/// Abstract class represents a Hypertable table.
	/// </summary>
	class Table {

		public:

			/// <summary>
			/// Destroys the Table instance.
			/// </summary>
			virtual ~Table( ) { }

			/// <summary>
			/// Returns the context kind.
			/// </summary>
			virtual Common::ContextKind getContextKind( ) const = 0;

			/// <summary>
			/// Returns the table name.
			/// </summary>
			/// <returns>Table name</returns>
			virtual std::string getName( ) const = 0;

			/// <summary>
			/// Creates a new table mutator on this table.
			/// </summary>
			/// <param name="timeoutMsec">Maximum time [ms] to allow mutator methods to execute before time out</param>
			/// <param name="flags">Mutator flags</param>
			/// <param name="flushIntervalMsec">Periodic flush interval [ms], if zero periodic flush is disabled.</param>
			/// <returns>Newly created table mutator</returns>
			/// <seealso cref="ht4c::Common::TableMutator"/>
			/// <seealso cref="ht4c::Common::MutatorFlags"/>
			virtual TableMutator* createMutator( uint32_t timeoutMsec = 0, uint32_t flags = 0, uint32_t flushIntervalMsec = 0 ) = 0;

			/// <summary>
			/// Creates a new asynchronous mutator on this table.
			/// </summary>
			/// <param name="asyncResult">Receives the results from the asynchronous mutator operations</param>
			/// <param name="timeoutMsec">Maximum time [ms] to allow mutator methods to execute before time out</param>
			/// <param name="flags">Mutator flags</param>
			/// <returns>Newly created asynchronous table mutator</returns>
			/// <seealso cref="ht4c::Common::TableMutator"/>
			/// <seealso cref="ht4c::Common::MutatorFlags"/>
			virtual AsyncTableMutator* createAsyncMutator( AsyncResult& asyncResult, uint32_t timeoutMsec = 0, uint32_t flags = 0 ) = 0;

			/// <summary>
			/// Creates a new synchronous scanner on this table.
			/// </summary>
			/// <param name="scanSpec">Scan specification</param>
			/// <param name="timeoutMsec">Maximum time [ms] to allow scanner methods to execute before time out</param>
			/// <param name="flags">Scanner flags</param>
			/// <returns>Newly created synchronous table scanner</returns>
			/// <seealso cref="ht4c::Common::TableScanner"/>
			/// <seealso cref="ht4c::Common::ScanSpec"/>
			/// <seealso cref="ht4c::Common::ScannerFlags"/>
			virtual TableScanner* createScanner( ScanSpec& scanSpec, uint32_t timeoutMsec = 0, uint32_t flags = 0 ) = 0;

			/// <summary>
			/// Creates a new asynchronous scanner on this table.
			/// </summary>
			/// <param name="scanSpec">Scan specification</param>
			/// <param name="asyncResult">Receives the results from the asynchronous scan operations</param>
			/// <param name="timeoutMsec">Maximum time [ms] to allow scanner methods to execute before time out</param>
			/// <param name="flags">Scanner flags</param>
			/// <returns>Newly created asynchronous table scanner</returns>
			/// <seealso cref="ht4c::Common::AsyncTableScanner"/>
			/// <seealso cref="ht4c::Common::ScanSpec"/>
			/// <seealso cref="ht4c::Common::AsyncResult"/>
			/// <seealso cref="ht4c::Common::ScannerFlags"/>
			virtual AsyncTableScanner* createAsyncScanner( ScanSpec& scanSpec, AsyncResult& asyncResult, uint32_t timeoutMsec = 0, uint32_t flags = 0 ) = 0;

			/// <summary>
			/// Creates a new asynchronous scanner on this table and returns the scanner identifier.
			/// </summary>
			/// <param name="scanSpec">Scan specification</param>
			/// <param name="asyncResult">Receives the results from the asynchronous scan operations</param>
			/// <param name="timeoutMsec">Maximum time [ms] to allow scanner methods to execute before time out</param>
			/// <param name="flags">Scanner flags</param>
			/// <returns>Newly created asynchronous table scanner identifier</returns>
			/// <seealso cref="ht4c::Common::AsyncTableScanner"/>
			/// <seealso cref="ht4c::Common::ScanSpec"/>
			/// <seealso cref="ht4c::Common::AsyncResult"/>
			/// <seealso cref="ht4c::Common::ScannerFlags"/>
			virtual int64_t createAsyncScannerId( ScanSpec& scanSpec, AsyncResult& asyncResult, uint32_t timeoutMsec = 0, uint32_t flags = 0 ) = 0;

			/// <summary>
			/// Returns the xml schema for this table.
			/// </summary>
			/// <param name="withIds">Include the id's in the schema</param>
			/// <returns>xml schema</returns>
			virtual std::string getSchema( bool withIds = false ) = 0;

		protected:

			/// <summary>
			/// Creates a new Table instance.
			/// </summary>
			Table( ) { }

		private:

			Table( const Table& ) { }
			Table& operator = ( const Table& ) { return *this; }
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif