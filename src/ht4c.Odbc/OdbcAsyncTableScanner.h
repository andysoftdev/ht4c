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
#error compile native
#endif

#include "OdbcClient.h"
#include "ht4c.Common/AsyncTableScanner.h"

namespace Hypertable {
	class SerializedCellsReader;
}

namespace ht4c { namespace Odbc {

	/// <summary>
	/// Represents a Hypertable asynchronous scanner, using the Odbc API.
	/// </summary>
	/// <seealso cref="ht4c::Common::AsyncTableScanner"/>
	class OdbcAsyncTableScanner : public Common::AsyncTableScanner {

		public:

			/// <summary>
			/// Creates a new OdbcAsyncTableScanner instance.
			/// </summary>
			/// <param name="tableScanner">Odbc scanner</param>
			/// <returns>New OdbcAsyncTableScanner instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Common::AsyncTableScanner* create( Db::ScannerAsyncPtr tableScanner );

			/// <summary>
			/// Returns the scanner identifier for the specified asynchronous scanner.
			/// </summary>
			/// <param name="tableScanner">Odbc scanner</param>
			/// <returns>Asynchronous scanner identifier</returns>
			static int64_t id( const Db::ScannerAsyncPtr& tableScanner );

			/// <summary>
			/// Destroys the OdbcAsyncTableScanner instance.
			/// </summary>
			virtual ~OdbcAsyncTableScanner( );

			#pragma region Common::AsyncTableScanner methods

			virtual int64_t id( ) const;

			#pragma endregion

		private:

			OdbcAsyncTableScanner( Db::ScannerAsyncPtr tableScanner );

			OdbcAsyncTableScanner( ) { }
			OdbcAsyncTableScanner( const OdbcAsyncTableScanner& ) { }
			OdbcAsyncTableScanner& operator = ( const OdbcAsyncTableScanner& ) { return *this; }

			Db::ScannerAsyncPtr tableScanner;
	};

} }