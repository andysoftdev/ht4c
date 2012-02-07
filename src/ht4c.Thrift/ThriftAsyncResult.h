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

#include "ht4c.Common/AsyncResult.h"

#ifndef __cplusplus_cli
#include <boost/thread/condition.hpp>
#endif

namespace ht4c { namespace Common {
	class AsyncResultSink;
} }

namespace ht4c { namespace Thrift {

	/// <summary>
	/// Represents results from asynchronous table scan operations, using the thrift API.
	/// </summary>
	/// <remarks>Links between native and C++/CLI.</remarks>
	/// <seealso cref="ht4c::Common::AsyncResult"/>
	class ThriftAsyncResult : public Common::AsyncResult {

		public:

			/// <summary>
			/// Creates a new ThriftAsyncResult instance.
			/// </summary>
			/// <param name="asyncResultSink">Callback for asynchronous table scan operations</param>
			/// <param name="capacity">Capacity in bytes of result queue. If zero then the queue capacity will be unbounded.</param>
			/// <returns>New ThriftAsyncResult instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static ThriftAsyncResult* create( Common::AsyncResultSink* asyncResultSink, size_t capacity = 0 );

			#ifndef __cplusplus_cli

			/// <summary>
			/// Initializes a new instance of the ThriftAsyncResult class.
			/// </summary>
			/// <param name="capacity">Capacity in bytes of result queue. If zero then the queue capacity will be unbounded.</param>
			/// <remarks>Pure native constructor.</remarks>
			explicit ThriftAsyncResult( size_t capacity );

			/// <summary>
			/// Initializes a new instance of the ThriftAsyncResult class.
			/// </summary>
			/// <param name="asyncResultSink">Callback for asynchronous table scan operations</param>
			/// <param name="capacity">Capacity in bytes of result queue. If zero then the queue capacity will be unbounded.</param>
			/// <remarks>Pure native constructor.</remarks>
			ThriftAsyncResult( Common::AsyncResultSink* asyncResultSink, size_t capacity );

			/// <summary>
			/// Returns the thrift future.
			/// </summary>
			/// <param name="client">Thrift client</param>
			/// <returns>Thrift future</returns>
			/// <remarks>Pure native method.</remarks>
			Hypertable::ThriftGen::Future get( Hypertable::Thrift::ClientPtr client );

			/// <summary>
			/// Publish received results.
			/// </summary>
			/// <param name="result">Thrift serialized result</param>
			/// <param name="asyncResult">Async result</param>
			/// <param name="asyncResultSink">Callback for asynchronous table scan operations</param>
			/// <param name="raiseException">If true the method raise an exception on error</param>
			/// <returns>true if succeeded</returns>
			/// <remarks>Pure native method.</remarks>
			static bool publishResult( Hypertable::ThriftGen::ResultSerialized& result, Common::AsyncResult* asyncResult, Common::AsyncResultSink* asyncResultSink, bool raiseException );

			#endif

			/// <summary>
			/// Destroys the ThriftAsyncResult instance.
			/// </summary>
			virtual ~ThriftAsyncResult( );

			#pragma region Common::AsyncResult methods

			virtual void attachAsyncScanner( int64_t asyncScannerId );
			virtual void attachAsyncMutator( int64_t asyncMutatorId );
			virtual void join( );
			virtual void cancel( );
			virtual void cancelAsyncScanner( int64_t asyncScannerId );
			virtual void cancelAsyncMutator( int64_t asyncMutatorId );
			virtual bool isCompleted( ) const;
			virtual bool isCancelled( ) const;

			#pragma endregion

		private:

			ThriftAsyncResult( const ThriftAsyncResult& ) { }
			ThriftAsyncResult& operator = ( const ThriftAsyncResult& ) { return *this; }

			#ifndef __cplusplus_cli

			enum {
				queryFutureResultTimeoutMs = 100
			};

			void readAndPublishResult( );

			static DWORD WINAPI threadProc( void* param );

			Hypertable::Thrift::ClientPtr client;
			Hypertable::ThriftGen::Future future;
			Common::AsyncResultSink* asyncResultSink;
			size_t capacity;
			mutable bool cancelled;
			int outstanding;
			HANDLE thread;
			bool abort;

			mutable Hypertable::RecMutex mutex;
			boost::condition cond;

			typedef std::set<int64_t> set_t;
			set_t asyncTableScanners;

			#endif
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif