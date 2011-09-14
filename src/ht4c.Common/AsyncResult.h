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

namespace ht4c { namespace Common {

	class AsyncTableScanner;

	/// <summary>
	/// Abstract class represents results from asynchronous table scan operations.
	/// </summary>
	class AsyncResult {

		public:

			/// <summary>
			/// Destroys the AsyncResult instance.
			/// </summary>
			virtual ~AsyncResult( ) { }

			/// <summary>
			/// Attaches an asynchronous table scanner.
			/// </summary>
			/// <param name="asyncScannerId">Asynchronous table scanner identifier</param>
			virtual void attachAsyncScanner( int64_t asyncScannerId ) = 0;

			/// <summary>
			/// Attaches an asynchronous table mutator.
			/// </summary>
			/// <param name="asyncMutatorId">Asynchronous table mutator identifier</param>
			virtual void attachAsyncMutator( int64_t asyncMutatorId ) = 0;

			/// <summary>
			/// Blocks the calling thread until the asynchronous operations have completed.
			/// </summary>
			virtual void join( ) = 0;

			/// <summary>
			/// Cancels any outstanding asynchronous operations.
			/// </summary>
			virtual void cancel( ) = 0;

			/// <summary>
			/// Cancels asynchronous table scanner.
			/// </summary>
			virtual void cancelAsyncScanner( int64_t asyncScannerId ) = 0;

			/// <summary>
			/// Cancels asynchronous table mutator.
			/// </summary>
			virtual void cancelAsyncMutator( int64_t asyncMutatorId ) = 0;

			/// <summary>
			/// Returns if all asynchronous operations have completed.
			/// </summary>
			/// <returns>true if all asynchronous operations have completed</returns>
			virtual bool isCompleted( ) const = 0;

			/// <summary>
			/// Returns if the asynchronous operations have cancelled.
			/// </summary>
			/// <returns>true if all asynchronous operations have cancelled</returns>
			virtual bool isCancelled( ) const = 0;

	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif