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
#error compile native
#endif

#include "HamsterClient.h"
#include "ht4c.Common/Types.h"
#include "ht4c.Common/Cell.h"
#include "ht4c.Common/TableScanner.h"

namespace Hypertable {
	class SerializedCellsReader;
}

namespace ht4c { namespace Hamster {

	/// <summary>
	/// Represents a Hypertable scanner, using the hamster API.
	/// </summary>
	/// <seealso cref="ht4c::Common::TableScanner"/>
	class HamsterTableScanner : public Common::TableScanner {

		public:

			/// <summary>
			/// Creates a new HamsterTableScanner instance.
			/// </summary>
			/// <param name="tableScanner">Hamster scanner</param>
			/// <returns>New HamsterTableScanner instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Common::TableScanner* create( Db::ScannerPtr tableScanner );

			/// <summary>
			/// Destroys the HamsterTableScanner instance.
			/// </summary>
			virtual ~HamsterTableScanner( );

			#pragma region Common::TableScanner methods

			virtual bool next( Common::Cell*& cell );

			#pragma endregion

		private:

			HamsterTableScanner( Db::ScannerPtr tableScanner );

			HamsterTableScanner( ) { }
			HamsterTableScanner( const HamsterTableScanner& ) { }
			HamsterTableScanner& operator = ( const HamsterTableScanner& ) { return *this; }

			Db::ScannerPtr tableScanner;
			Common::Cell cell;
	};

} }