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
#error compile native
#endif

#include "SQLiteClient.h"
#include "ht4c.Common/Types.h"
#include "ht4c.Common/Cell.h"
#include "ht4c.Common/TableScanner.h"

namespace Hypertable {
	class SerializedCellsReader;
}

namespace ht4c { namespace SQLite {

	/// <summary>
	/// Represents a Hypertable scanner, using the sqlite API.
	/// </summary>
	/// <seealso cref="ht4c::Common::TableScanner"/>
	class SQLiteTableScanner : public Common::TableScanner {

		public:

			/// <summary>
			/// Creates a new SQLiteTableScanner instance.
			/// </summary>
			/// <param name="tableScanner">SQLite scanner</param>
			/// <returns>New SQLiteTableScanner instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Common::TableScanner* create( Db::ScannerPtr tableScanner );

			/// <summary>
			/// Destroys the SQLiteTableScanner instance.
			/// </summary>
			virtual ~SQLiteTableScanner( );

			#pragma region Common::TableScanner methods

			virtual bool next( Common::Cell*& cell );

			#pragma endregion

		private:

			SQLiteTableScanner( Db::ScannerPtr tableScanner );

			SQLiteTableScanner( ) { }
			SQLiteTableScanner( const SQLiteTableScanner& ) { }
			SQLiteTableScanner& operator = ( const SQLiteTableScanner& ) { return *this; }

			Db::ScannerPtr tableScanner;
			Common::Cell cell;
	};

} }