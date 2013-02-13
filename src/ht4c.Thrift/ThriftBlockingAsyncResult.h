/** -*- C++ -*-
 * Copyright (C) 2010-2013 Thalmann Software & Consulting, http://www.softdev.ch
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

#include "ht4c.Common/BlockingAsyncResult.h"

namespace ht4c { namespace Thrift {

	/// <summary>
	/// Represents results from asynchronous table scan operations, using the thrift API.
	/// </summary>
	/// <remarks>Links between native and C++/CLI.</remarks>
	/// <seealso cref="ht4c::Common::AsyncResult"/>
	class ThriftBlockingAsyncResult : public Common::BlockingAsyncResult {

		public:

			/// <summary>
			/// Creates a new ThriftBlockingAsyncResult instance.
			/// </summary>
			/// <param name="capacity">Capacity in bytes of result queue. If zero then the queue capacity will be unbounded.</param>
			/// <returns>New ThriftBlockingAsyncResult instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static ThriftBlockingAsyncResult* create( size_t capacity = 0 );

			#ifndef __cplusplus_cli

			/// <summary>
			/// Initializes a new instance of the ThriftBlockingAsyncResult class.
			/// </summary>
			/// <param name="capacity">Capacity in bytes of result queue. If zero then the queue capacity will be unbounded.</param>
			/// <remarks>Pure native constructor.</remarks>
			explicit ThriftBlockingAsyncResult( size_t capacity );

			/// <summary>
			/// Returns the thrift future.
			/// </summary>
			/// <param name="client">Thrift client</param>
			/// <returns>Thrift future</returns>
			/// <remarks>Pure native method.</remarks>
			Hypertable::ThriftGen::Future get( Hypertable::Thrift::ClientPtr client );

			#endif

			/// <summary>
			/// Destroys the ThriftBlockingAsyncResult instance.
			/// </summary>
			virtual ~ThriftBlockingAsyncResult( );

			#pragma region Common::BlockingAsyncResult methods

			virtual void attachAsyncScanner( int64_t asyncScannerId );
			virtual void attachAsyncMutator( int64_t asyncMutatorId );
			virtual void join( );
			virtual void cancel( );
			virtual void cancelAsyncScanner( int64_t asyncScannerId );
			virtual void cancelAsyncMutator( int64_t asyncMutatorId );
			virtual bool isCompleted( ) const;
			virtual bool isCancelled( ) const;

			virtual bool isEmpty( ) const;
			virtual bool getCells( Common::AsyncResultSink* asyncResultSink );
			virtual bool getCells( Common::AsyncResultSink* asyncResultSink, uint32_t timeoutMsec, bool& timedOut );

			#pragma endregion

		private:

			#ifndef __cplusplus_cli

			enum {
				queryFutureResultTimeoutMs = 500
			};

			Hypertable::Thrift::ClientPtr client;
			Hypertable::ThriftGen::Future future;
			size_t capacity;
			mutable bool cancelled;
			mutable Hypertable::RecMutex mutex;

			typedef std::set<int64_t> set_t;
			set_t asyncTableScanners;

			#endif
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif