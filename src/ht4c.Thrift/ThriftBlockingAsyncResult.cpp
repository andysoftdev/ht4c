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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "stdafx.h"
#include "ThriftBlockingAsyncResult.h"
#include "ThriftAsyncResult.h"
#include "ThriftClient.h"
#include "ThriftException.h"

#include "ht4c.Common/AsyncResultSink.h"
#include "ht4c.Common/Cells.h"

namespace ht4c { namespace Thrift {

	ThriftBlockingAsyncResult* ThriftBlockingAsyncResult::create( size_t capacity ) {
		return new ThriftBlockingAsyncResult( capacity );
	}

	ThriftBlockingAsyncResult::ThriftBlockingAsyncResult( size_t _capacity )
	: future( 0 )
	, capacity( _capacity >= 0 ? _capacity : 0 )
	, cancelled( false )
	, mutex( )
	, asyncTableScanners( )
	{
	}

	Hypertable::ThriftGen::Future ThriftBlockingAsyncResult::get( Hypertable::Thrift::ClientPtr _client ) {
		HT4C_TRY {
			if( !future ) {
				client = _client->get_pooled();
				ThriftClientLock sync( client.get() );
				future = client->future_open( capacity );
			}
			return future;
		}
		HT4C_THRIFT_RETHROW
	}

	ThriftBlockingAsyncResult::~ThriftBlockingAsyncResult( ) {
		HT4C_TRY {
			if( future ) {
				ThriftClientLock sync( client.get() );
				HT4C_THRIFT_RETRY( client->future_close(future) );
				future = 0;
				for each( int64_t asyncScannerId in asyncTableScanners ) {
					HT4C_THRIFT_RETRY( client->async_scanner_close(asyncScannerId) );
				}
			}
			client = 0;
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftBlockingAsyncResult::attachAsyncScanner( int64_t asyncScannerId ) {
		if( asyncScannerId ) {
			Hypertable::ScopedRecLock lock( mutex );
			asyncTableScanners.insert( asyncScannerId );
			cancelled = false;
		}
	}

	void ThriftBlockingAsyncResult::attachAsyncMutator( int64_t asyncMutatorId ) {
		if( asyncMutatorId ) {
			Hypertable::ScopedRecLock lock( mutex );
			cancelled = false;
		}
	}

	void ThriftBlockingAsyncResult::join( ) {
		HT4C_TRY {
			while( !isCompleted() ) {
				Sleep( 40 );
			}
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftBlockingAsyncResult::cancel( ) {
		HT4C_TRY {
			if( future ) {
				{
					ThriftClientLock sync( client.get() );
					client->future_cancel( future );
				}
				Hypertable::ScopedRecLock lock( mutex );
				cancelled = true;
			}
		}
		HT4C_THRIFT_RETHROW
	}

	void ThriftBlockingAsyncResult::cancelAsyncScanner( int64_t asyncScannerId ) { 
		HT4C_TRY {
			if( asyncScannerId ) {
				{
					ThriftClientLock sync( client.get() );
					client->async_scanner_cancel( asyncScannerId );
					client->async_scanner_close( asyncScannerId );
				}
				Hypertable::ScopedRecLock lock( mutex );
				asyncTableScanners.erase( asyncScannerId );
			}
		}
		HT4C_RETHROW
	}

	void ThriftBlockingAsyncResult::cancelAsyncMutator( int64_t asyncMutatorId ) {
		HT4C_TRY {
			if( asyncMutatorId ) {
				ThriftClientLock sync( client.get() );
				client->async_mutator_cancel( asyncMutatorId );
			}
		}
		HT4C_RETHROW
	}

	bool ThriftBlockingAsyncResult::isCompleted( ) const {
		HT4C_TRY {
			if( future ) {
				ThriftClientLock sync( client.get() );
				return !client->future_has_outstanding( future );
			}
			else {
				return true;
			}
		}
		HT4C_THRIFT_RETHROW
	}

	bool ThriftBlockingAsyncResult::isCancelled( ) const {
		if( future ) {
			Hypertable::ScopedRecLock lock( mutex );
			if( cancelled ) {
				return true;
			}
			ThriftClientLock sync( client.get() );
			return cancelled = client->future_is_cancelled( future );
		}
		return false;
	}

	bool ThriftBlockingAsyncResult::isEmpty( ) const {
		HT4C_TRY {
			if( future ) {
				ThriftClientLock sync( client.get() );
				return client->future_is_empty( future );
			}
			return true;
		}
		HT4C_THRIFT_RETHROW
	}

	bool ThriftBlockingAsyncResult::getCells( Common::AsyncResultSink* asyncResultSink ) {
		bool result;
		bool timedOut = true;
		while( timedOut ) {
			result = getCells( asyncResultSink, queryFutureResultTimeoutMs, timedOut);
		}
		return result;
	}

	bool ThriftBlockingAsyncResult::getCells( Common::AsyncResultSink* asyncResultSink, uint32_t timeoutMsec, bool& timedOut ) {
		HT4C_TRY {
			timedOut = false;
			if( future && asyncResultSink && !isCancelled() ) {
				Hypertable::ThriftGen::ResultSerialized result;
				while (true) {
					try {
						ThriftClientLock sync( client.get() );
						client->future_get_result_serialized( result, future, timeoutMsec );
					}
					catch( Hypertable::ThriftGen::ClientException& e ) {
						if( e.code == Hypertable::Error::REQUEST_TIMEOUT ) {
							timedOut = true;
							return false;
						}
						throw;
					}

					// ignore cancelled scanners
					if( result.id && result.is_scan && !result.is_error && !result.is_empty ) {
						Hypertable::ScopedRecLock lock( mutex );
						if( asyncTableScanners.find(result.id) == asyncTableScanners.end() ) {
							continue;
						}
					}
					return ThriftAsyncResult::publishResult( result, this, asyncResultSink, true ) && !result.is_empty;
				}
			}
			return false;
		}
		HT4C_THRIFT_RETHROW
	}

} }