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

#include "ht4c.Common/AsyncResult.h"

namespace ht4c { namespace Common {
	class AsyncResultSink;
} }

namespace ht4c { namespace Hyper {

	/// <summary>
	/// Represents results from asynchronous table scan operations, using the native hypertable API.
	/// </summary>
	/// <remarks>Links between native and C++/CLI.</remarks>
	/// <seealso cref="ht4c::Common::AsyncResult"/>
	class HyperAsyncResult : public Common::AsyncResult {

		public:

			/// <summary>
			/// Creates a new HyperAsyncResult instance.
			/// </summary>
			/// <param name="asyncResultSink">Callback for asynchronous table scan operations</param>
			/// <returns>New HyperAsyncResult instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static HyperAsyncResult* create( Common::AsyncResultSink* asyncResultSink );

			#ifndef __cplusplus_cli

			/// <summary>
			/// Initializes a new instance of the HyperAsyncResult class.
			/// </summary>
			/// <param name="asyncResultSink">Callback for asynchronous table scan operations</param>
			/// <remarks>Pure native constructor.</remarks>
			explicit HyperAsyncResult( Common::AsyncResultSink* asyncResultSink );

			/// <summary>
			/// Returns a reference to the underlying Hypertable result callback.
			/// </summary>
			/// <returns>Reference to the underlying Hypertable result callback</returns>
			/// <remarks>Pure native method.</remarks>
			inline Hypertable::ResultCallback& get() {
				return *fcb;
			}

			#endif

			/// <summary>
			/// Destroys the HyperAsyncResult instance.
			/// </summary>
			virtual ~HyperAsyncResult( );

			#pragma region Common::AsyncResult methods

			virtual void attachAsyncScanner( int64_t asyncScannerId ) { }
			virtual void attachAsyncMutator( int64_t asyncMutatorId ) { }
			virtual void join( );
			virtual void cancel( );
			virtual void cancelAsyncScanner( int64_t asyncScannerId );
			virtual void cancelAsyncMutator( int64_t asyncMutatorId );
			virtual bool isCompleted( ) const;
			virtual bool isCancelled( ) const;

			#pragma endregion

		private:

			HyperAsyncResult( const HyperAsyncResult& ) { }
			HyperAsyncResult& operator = ( const HyperAsyncResult& ) { return *this; }

			#ifndef __cplusplus_cli

			Hypertable::FutureCallbackPtr fcb;

			#endif

	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif