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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "stdafx.h"
#include "SQLiteAsyncResult.h"
#include "SQLiteClient.h"
#include "SQLiteException.h"

#include "ht4c.Common/AsyncResultSink.h"
#include "ht4c.Common/Cells.h"

namespace ht4c { namespace SQLite {

	SQLiteAsyncResult* SQLiteAsyncResult::create( Common::AsyncResultSink* asyncResultSink, size_t capacity ) {
		return new SQLiteAsyncResult( asyncResultSink, capacity );
	}

	SQLiteAsyncResult::SQLiteAsyncResult( size_t _capacity )
	: /*TODO future( 0 )
	, */capacity( _capacity >= 0 ? _capacity : 0 )
	, cancelled( false )
	, outstanding( 0 )
	, thread( 0 )
	, abort( false )
	, mutex( )
	, cond( )
	, asyncTableScanners( )
	{
	}

	SQLiteAsyncResult::SQLiteAsyncResult( Common::AsyncResultSink* _asyncResultSink, size_t _capacity )
	: /*TODO future( 0 )
	, */asyncResultSink( _asyncResultSink )
	, capacity( _capacity )
	, cancelled( false )
	, outstanding( 0 )
	, thread( 0 )
	, abort( false )
	, mutex( )
	, cond( )
	, asyncTableScanners( )
	{
	}

	/*Hypertable::SQLiteGen::Future SQLiteAsyncResult::get( SQLiteEnvPtr _env ) {
		HT4C_TRY {
			if( !future ) {
				env = _env->get_pooled();
				SQLiteEnvLock sync( env.get() );
				future = env->future_open( capacity );
				if( future ) {
					thread = ::CreateThread( 0, 0, threadProc, this, 0, 0 );
					if( !thread ) {
						DWORD err = ::GetLastError();
						HT4C_SQLITE_RETRY( env->future_close(future) );
						throw ht4c::Common::HypertableException( Hypertable::Error::EXTERNAL, winapi_strerror(err), __LINE__, __FUNCTION__, __FILE__ );
					}
				}
			}
			return future;
		}
		HT4C_SQLITE_RETHROW
	}*/

	/*bool SQLiteAsyncResult::publishResult( Hypertable::SQLiteGen::ResultSerialized& result, Common::AsyncResult* asyncResult, Common::AsyncResultSink* asyncResultSink, bool raiseException ) {
		if( result.id ) {
			if( result.is_error ) {
				asyncResult->cancel();
				Common::HypertableException exception( result.error, result.error_msg );
				asyncResultSink->failure( exception );
				if( raiseException ) {
					throw exception;
				}
				return false;
			}
			else if( result.is_scan ) {
				if( result.cells.length() ) {
					Common::Cells cells( 1024 );

					Hypertable::Cell cell;
					Hypertable::SerializedCellsReader reader( reinterpret_cast<void*>(const_cast<char*>(result.cells.c_str())), (uint32_t)result.cells.length() );
					while( reader.next() ) {
						reader.get( cell );
						cells.add( cell );
					}
					switch( asyncResultSink->scannedCells(result.id, cells) ) {
						case Common::ACR_Cancel:
								asyncResult->cancelAsyncScanner( result.id );
								break;
							case Common::ACR_Abort:
								asyncResult->cancel();
								return false;
							default:
								break;
					}
				}
				return true;
			}
		}
		return true;
	}*/

	SQLiteAsyncResult::~SQLiteAsyncResult( ) {
		/*HT4C_TRY {
			{
				Hypertable::ScopedRecLock lock( mutex );
				abort = true;
				cond.notify_all();
			}
			if( thread ) {
				::WaitForSingleObject( thread, INFINITE );
				::CloseHandle( thread );
			}
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

	void SQLiteAsyncResult::attachAsyncScanner( int64_t asyncScannerId ) {
		if( asyncScannerId ) {
			Hypertable::ScopedRecLock lock( mutex );
			asyncTableScanners.insert( asyncScannerId );
			cancelled = false;
			++outstanding;
			cond.notify_all();
		}
	}

	void SQLiteAsyncResult::attachAsyncMutator( int64_t asyncMutatorId ) {
		if( asyncMutatorId ) {
			Hypertable::ScopedRecLock lock( mutex );
			cancelled = false;
			++outstanding;
			cond.notify_all();
		}
	}

	void SQLiteAsyncResult::join( ) {
		HT4C_TRY {
			Hypertable::ScopedRecLock lock( mutex );
			// wake up the polling thread, required if async mutators have been attached
			++outstanding;
			cond.notify_all();
			// wait for completion
			while( outstanding > 0 ) {
				cond.wait( lock );
			}
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteAsyncResult::cancel( ) {
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

	void SQLiteAsyncResult::cancelAsyncScanner( int64_t asyncScannerId ) { 
		/*HT4C_TRY {
			if( asyncScannerId ) {
				{
					SQLiteEnvLock sync( env.get() );
					env->async_scanner_cancel( asyncScannerId );
					env->async_scanner_close( asyncScannerId );
				}
				Hypertable::ScopedRecLock lock( mutex );
				asyncTableScanners.erase( asyncScannerId );
			}
		}
		HT4C_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	void SQLiteAsyncResult::cancelAsyncMutator( int64_t asyncMutatorId ) {
		/*HT4C_TRY {
			if( asyncMutatorId ) {
				SQLiteEnvLock sync( env.get() );
				env->async_mutator_cancel( asyncMutatorId );
			}
		}
		HT4C_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	bool SQLiteAsyncResult::isCompleted( ) const {
		/*HT4C_TRY {
			Hypertable::ScopedRecLock lock( mutex );
			return future ? outstanding == 0 : true;
		}
		HT4C_SQLITE_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	bool SQLiteAsyncResult::isCancelled( ) const {
		/*HT4C_TRY {
			if( future ) {
				Hypertable::ScopedRecLock lock( mutex );
				if( cancelled ) {
					return true;
				}
				SQLiteEnvLock sync( env.get() );
				return cancelled = env->future_is_cancelled( future );
			}
			return false;
		}
		HT4C_SQLITE_RETHROW*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	void SQLiteAsyncResult::readAndPublishResult( ) {
		/*while( future && asyncResultSink ) {
			try {
				{
					Hypertable::ScopedRecLock lock( mutex );
					while( outstanding == 0 && !abort ) {
						cond.wait( lock );
					}
					if( abort ) {
						break;
					}
				}
				Hypertable::SQLiteGen::ResultSerialized result;
				HT4C_TRY {
					try {
						SQLiteEnvLock sync( env.get() );
						env->future_get_result_serialized( result, future, queryFutureResultTimeoutMs );
					}
					catch( Hypertable::SQLiteGen::ClientException& e ) {
						if(  e.code == Hypertable::Error::REQUEST_TIMEOUT
							|| e.code == Hypertable::Error::NOT_IMPLEMENTED ) { // FIXME, SQLiteBroker does not implement get_future_result_serialized for async mutators

							continue;
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
				}
				HT4C_SQLITE_RETHROW

				if( !publishResult(result, this, asyncResultSink, false) || result.is_empty ) {
					Hypertable::ScopedRecLock lock( mutex );
					outstanding = std::max( 0, --outstanding );
					cond.notify_all();
				}
			}
			catch( Common::HypertableException& e ) {
				cancel();
				asyncResultSink->failure( e );
			}
			catch( ... ) {
				cancel();
				std::stringstream ss;
				ss << "Caught unknown exception\n\tat " << __FUNCTION__ << " (" << __FILE__ << ':' << __LINE__ << ')';
				Common::HypertableException e( Hypertable::Error::EXTERNAL, ss.str(), __LINE__, __FUNCTION__, __FILE__ );
				asyncResultSink->failure( e );
			}
		}*/
		HT4C_THROW_NOTIMPLEMENTED();
	}

	DWORD SQLiteAsyncResult::threadProc( void* param ) {
		SQLiteAsyncResult* asyncResult = reinterpret_cast<SQLiteAsyncResult*>( param );
		if( asyncResult ) {
			asyncResult->readAndPublishResult();
		}
		return 0;
	}

} }