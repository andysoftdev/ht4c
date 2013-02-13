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
#include "SQLiteBlockingAsyncResult.h"
#include "SQLiteAsyncResult.h"
#include "SQLiteClient.h"
#include "SQLiteException.h"

#include "ht4c.Common/AsyncResultSink.h"
#include "ht4c.Common/Cells.h"

namespace ht4c { namespace SQLite {

	SQLiteBlockingAsyncResult* SQLiteBlockingAsyncResult::create( size_t capacity ) {
		return new SQLiteBlockingAsyncResult( capacity );
	}

	SQLiteBlockingAsyncResult::SQLiteBlockingAsyncResult( size_t _capacity )
	: /*TODO future( 0 )
	, */capacity( _capacity >= 0 ? _capacity : 0 )
	, cancelled( false )
	, mutex( )
	, asyncTableScanners( )
	{
	}

	/*Hypertable::SQLiteGen::Future SQLiteBlockingAsyncResult::get( SQLiteEnvPtr _env ) {
		HT4C_TRY {
			if( !future ) {
				env = _env->get_pooled();
				SQLiteEnvLock sync( env.get() );
				future = env->future_open( capacity );
			}
			return future;
		}
		HT4C_SQLITE_RETHROW
	}*/

	SQLiteBlockingAsyncResult::~SQLiteBlockingAsyncResult( ) {
		/*HT4C_TRY {
			if( future ) {
				SQLiteEnvLock sync( env.get() );
				HT4C_SQLITE_RETRY( env->future_close(future) );
				future = 0;
				for each( int64_t asyncScannerId in asyncTableScanners ) {
					HT4C_SQLITE_RETRY( env->async_scanner_close(asyncScannerId) );
				}
			}
			env = 0;
		}
		HT4C_SQLITE_RETHROW*/
	}

	void SQLiteBlockingAsyncResult::attachAsyncScanner( int64_t asyncScannerId ) {
		if( asyncScannerId ) {
			Hypertable::ScopedRecLock lock( mutex );
			asyncTableScanners.insert( asyncScannerId );
			cancelled = false;
		}
	}

	void SQLiteBlockingAsyncResult::attachAsyncMutator( int64_t asyncMutatorId ) {
		if( asyncMutatorId ) {
			Hypertable::ScopedRecLock lock( mutex );
			cancelled = false;
		}
	}

	void SQLiteBlockingAsyncResult::join( ) {
		HT4C_TRY {
			while( !isCompleted() ) {
				Sleep( 40 );
			}
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteBlockingAsyncResult::cancel( ) {
		/*HT4C_TRY {
			if( future ) {
				{
					SQLiteEnvLock sync( env.get() );
					env->future_cancel( future );
				}
				Hypertable::ScopedRecLock lock( mutex );
				cancelled = true;
			}
		}
		HT4C_SQLITE_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	void SQLiteBlockingAsyncResult::cancelAsyncScanner( int64_t asyncScannerId ) { 
		/*HT4C_TRY {
			if( asyncScannerId ) {
				{
					SQLiteEnvLock sync( env.get() );
					HT4C_SQLITE_RETRY( env->async_scanner_cancel(asyncScannerId) );
				}
				Hypertable::ScopedRecLock lock( mutex );
				asyncTableScanners.erase( asyncScannerId );
			}
		}
		HT4C_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	void SQLiteBlockingAsyncResult::cancelAsyncMutator( int64_t asyncMutatorId ) {
		/*HT4C_TRY {
			if( asyncMutatorId ) {
				SQLiteEnvLock sync( env.get() );
				HT4C_SQLITE_RETRY( env->async_mutator_cancel(asyncMutatorId) );
			}
		}
		HT4C_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	bool SQLiteBlockingAsyncResult::isCompleted( ) const {
		/*HT4C_TRY {
			if( future ) {
				SQLiteEnvLock sync( env.get() );
				return !env->future_has_outstanding( future );
			}
			else {
				return true;
			}
		}
		HT4C_SQLITE_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	bool SQLiteBlockingAsyncResult::isCancelled( ) const {
		/*if( future ) {
			Hypertable::ScopedRecLock lock( mutex );
			if( cancelled ) {
				return true;
			}
			SQLiteEnvLock sync( env.get() );
			return cancelled = env->future_is_cancelled( future );
		}
		return false;*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	bool SQLiteBlockingAsyncResult::isEmpty( ) const {
		/*HT4C_TRY {
			if( future ) {
				SQLiteEnvLock sync( env.get() );
				return env->future_is_empty( future );
			}
			return true;
		}
		HT4C_SQLITE_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	bool SQLiteBlockingAsyncResult::getCells( Common::AsyncResultSink* asyncResultSink ) {
		bool result;
		bool timedOut = true;
		while( timedOut ) {
			result = getCells( asyncResultSink, queryFutureResultTimeoutMs, timedOut);
		}
		return result;
	}

	bool SQLiteBlockingAsyncResult::getCells( Common::AsyncResultSink* asyncResultSink, uint32_t timeoutMsec, bool& timedOut ) {
		/*HT4C_TRY {
			timedOut = false;
			if( future && asyncResultSink && !isCancelled() ) {
				Hypertable::SQLiteGen::ResultSerialized result;
				while (true) {
					try {
						SQLiteEnvLock sync( env.get() );
						env->future_get_result_serialized( result, future, timeoutMsec );
					}
					catch( Hypertable::SQLiteGen::ClientException& e ) {
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
					return SQLiteAsyncResult::publishResult( result, this, asyncResultSink, true ) && !result.is_empty;
				}
			}
			return false;
		}
		HT4C_SQLITE_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

} }