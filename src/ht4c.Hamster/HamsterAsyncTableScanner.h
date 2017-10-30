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

#include "HamsterClient.h"
#include "ht4c.Common/AsyncTableScanner.h"

namespace Hypertable {
	class SerializedCellsReader;
}

namespace ht4c { namespace Hamster {

	/// <summary>
	/// Represents a Hypertable asynchronous scanner, using the hamster API.
	/// </summary>
	/// <seealso cref="ht4c::Common::AsyncTableScanner"/>
	class HamsterAsyncTableScanner : public Common::AsyncTableScanner {

		public:

			/// <summary>
			/// Creates a new HamsterAsyncTableScanner instance.
			/// </summary>
			/// <param name="tableScanner">Hamster scanner</param>
			/// <returns>New HamsterAsyncTableScanner instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Common::AsyncTableScanner* create( Db::ScannerAsyncPtr tableScanner );

			/// <summary>
			/// Returns the scanner identifier for the specified asynchronous scanner.
			/// </summary>
			/// <param name="tableScanner">Hamster scanner</param>
			/// <returns>Asynchronous scanner identifier</returns>
			static int64_t id( const Db::ScannerAsyncPtr& tableScanner );

			/// <summary>
			/// Destroys the HamsterAsyncTableScanner instance.
			/// </summary>
			virtual ~HamsterAsyncTableScanner( ) throw(ht4c::Common::HypertableException);

			#pragma region Common::AsyncTableScanner methods

			virtual int64_t id( ) const;

			#pragma endregion

		private:

			HamsterAsyncTableScanner( Db::ScannerAsyncPtr tableScanner );

			HamsterAsyncTableScanner( ) { }
			HamsterAsyncTableScanner( const HamsterAsyncTableScanner& ) { }
			HamsterAsyncTableScanner& operator = ( const HamsterAsyncTableScanner& ) { return *this; }

			Db::ScannerAsyncPtr tableScanner;
	};

} }