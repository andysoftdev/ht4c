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
#include "ht4c.Common/Types.h"
#include "ht4c.Common/Cell.h"
#include "ht4c.Common/TableScanner.h"

namespace Hypertable {
	class SerializedCellsReader;
}

namespace ht4c { namespace Odbc {

	/// <summary>
	/// Represents a Hypertable scanner, using the Odbc API.
	/// </summary>
	/// <seealso cref="ht4c::Common::TableScanner"/>
	class OdbcTableScanner : public Common::TableScanner {

		public:

			/// <summary>
			/// Creates a new OdbcTableScanner instance.
			/// </summary>
			/// <param name="tableScanner">Odbc scanner</param>
			/// <returns>New OdbcTableScanner instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Common::TableScanner* create( Db::ScannerPtr tableScanner );

			/// <summary>
			/// Destroys the OdbcTableScanner instance.
			/// </summary>
			virtual ~OdbcTableScanner( );

			#pragma region Common::TableScanner methods

			virtual bool next( Common::Cell*& cell );

			#pragma endregion

		private:

			OdbcTableScanner( Db::ScannerPtr tableScanner );

			OdbcTableScanner( ) { }
			OdbcTableScanner( const OdbcTableScanner& ) { }
			OdbcTableScanner& operator = ( const OdbcTableScanner& ) { return *this; }

			Db::ScannerPtr tableScanner;
			Common::Cell cell;
	};

} }