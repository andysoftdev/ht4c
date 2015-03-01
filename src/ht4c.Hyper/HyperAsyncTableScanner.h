/** -*- C++ -*-
 * Copyright (C) 2010-2015 Thalmann Software & Consulting, http://www.softdev.ch
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

#include "ht4c.Common/AsyncTableScanner.h"


namespace ht4c { namespace Hyper {

	/// <summary>
	/// Represents a Hypertable asynchronous scanner, using the native hypertable API.
	/// </summary>
	/// <seealso cref="ht4c::Common::AsyncTableScanner"/>
	class HyperAsyncTableScanner : public Common::AsyncTableScanner {

		public:

			/// <summary>
			/// Creates a new HyperAsyncTableScanner instance.
			/// </summary>
			/// <param name="tableScanner">Hypertable scanner</param>
			/// <returns>New HyperAsyncTableScanner instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Common::AsyncTableScanner* create( Hypertable::TableScannerAsync* tableScanner );

			/// <summary>
			/// Returns the scanner identifier for the specified asynchronous scanner.
			/// </summary>
			/// <param name="tableScanner">Hypertable scanner</param>
			/// <returns>Asynchronous scanner identifier</returns>
			static int64_t id( Hypertable::TableScannerAsync* tableScanner );

			/// <summary>
			/// Destroys the HyperAsyncTableScanner instance.
			/// </summary>
			virtual ~HyperAsyncTableScanner( );

			#pragma region Common::AsyncTableScanner methods

			virtual int64_t id( ) const;

			#pragma endregion

		private:

			HyperAsyncTableScanner( Hypertable::TableScannerAsync* tableScanner );

			HyperAsyncTableScanner( ) { }
			HyperAsyncTableScanner( const HyperAsyncTableScanner& ) { }
			HyperAsyncTableScanner& operator = ( const HyperAsyncTableScanner& ) { return *this; }

			Hypertable::TableScannerAsyncPtr tableScanner;
	};

} }