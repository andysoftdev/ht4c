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

#include "AsyncCallbackResult.h"

namespace ht4c { namespace Common {
	class Cells;
	class AsyncTableScanner;
	class HypertableException;

	/// <summary>
	/// Abstract class represents a callback for asynchronous table scan operations.
	/// </summary>
	class AsyncResultSink {

		public:

			/// <summary>
			/// Destroys the AsyncResultSink instance.
			/// </summary>
			virtual ~AsyncResultSink( ) { }

			/// <summary>
			/// Detaches an asynchronous table scanner.
			/// </summary>
			/// <param name="asyncScannerId">Asynchronous table scanner identifier</param>
			virtual void detachAsyncScanner( int64_t asyncScannerId ) = 0;

			/// <summary>
			/// Detaches an asynchronous table mutator.
			/// </summary>
			/// <param name="asyncMutatorId">Asynchronous table mutator identifier</param>
			virtual void detachAsyncMutator( int64_t asyncMutatorId ) = 0;

			/// <summary>
			/// Gets called if an asynchronous table scanner returns scanned cells.
			/// </summary>
			/// <param name="asyncScannerId">Asynchronous table scanner identifier</param>
			/// <param name="cells">Cells returned by the scanner</param>
			/// <returns>The asynchronous table scanner callback result</returns>
			virtual AsyncCallbackResult scannedCells( int64_t asyncScannerId, Cells& cells ) = 0;

			/// <summary>
			/// Gets called if an asynchronous operation fails.
			/// </summary>
			/// <param name="e">Exception</param>
			virtual void failure( Common::HypertableException& e ) = 0;
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif