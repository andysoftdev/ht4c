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

#include "OdbcClient.h"
#include "ht4c.Common/Table.h"

namespace ht4c { namespace Common {
	class TableMutator;
	class TableScanner;
	class ScanSpec;
	class AsyncResult;
} }

namespace ht4c { namespace Odbc {

	class OdbcNamespace;

	/// <summary>
	/// Represents a Hypertable table, using the Odbc API.
	/// </summary>
	/// <seealso cref="ht4c::Common::Table"/>
	class OdbcTable : public Common::Table {

		public:

			/// <summary>
			/// Creates a new OdbcTable instance.
			/// </summary>
			/// <param name="table">Table</param>
			/// <returns>New OdbcTable instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Common::Table* create( Db::TablePtr table );

			/// <summary>
			/// Destroys the OdbcTable instance.
			/// </summary>
			virtual ~OdbcTable( );

			#pragma region Common::Table methods

			virtual Common::ContextKind getContextKind( ) const {
				return Common::CK_ODBC;
			}
			virtual std::string getName( ) const;
			virtual Common::TableMutator* createMutator( uint32_t timeoutMsec = 0, uint32_t flags = 0, uint32_t flushIntervalMsec = 0 );
			virtual Common::AsyncTableMutator* createAsyncMutator( Common::AsyncResult& asyncResult, uint32_t timeoutMsec = 0, uint32_t flags = 0 );
			virtual Common::TableScanner* createScanner( Common::ScanSpec& scanSpec, uint32_t timeoutMsec = 0, uint32_t flags = 0 );
			virtual Common::AsyncTableScanner* createAsyncScanner( Common::ScanSpec& scanSpec, Common::AsyncResult& asyncResult, uint32_t timeoutMsec = 0, uint32_t flags = 0 );
			virtual int64_t createAsyncScannerId( Common::ScanSpec& scanSpec, Common::AsyncResult& asyncResult, uint32_t timeoutMsec = 0, uint32_t flags = 0 );
			virtual std::string getSchema( bool withIds = false );

			#pragma endregion

		private:

			OdbcTable( Db::TablePtr table );

			OdbcTable( ) { }
			OdbcTable( const OdbcTable& ) { }
			OdbcTable& operator = ( const OdbcTable& ) { return *this; }

			Db::TablePtr table;
	};

} }