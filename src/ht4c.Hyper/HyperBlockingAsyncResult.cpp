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
#include "HyperBlockingAsyncResult.h"

#include "ht4c.Common/AsyncResultSink.h"
#include "ht4c.Common/Cells.h"
#include "ht4c.Common/Exception.h"

namespace Hypertable {
	class TableScannerAsync;
}

namespace ht4c { namespace Hyper {
	using namespace Hypertable;

	HyperBlockingAsyncResult* HyperBlockingAsyncResult::create( size_t capacity ) {
		HT4C_TRY {
			return new HyperBlockingAsyncResult( capacity );
		}
		HT4C_RETHROW
	}

	HyperBlockingAsyncResult::HyperBlockingAsyncResult( size_t capacity )
	: future( new Future(capacity >= 0 ? capacity : 0) )
	{
	}

	HyperBlockingAsyncResult::~HyperBlockingAsyncResult( )
	{
		future = 0;
	}

	void HyperBlockingAsyncResult::join( ) {
		HT4C_TRY {
			if( future ) {
				future->wait_for_completion();
			}
		}
		HT4C_RETHROW
	}

	void HyperBlockingAsyncResult::cancel( ) {
		HT4C_TRY {
			if( future ) {
				future->cancel();
			}
		}
		HT4C_RETHROW
	}

	void HyperBlockingAsyncResult::cancelAsyncScanner( int64_t asyncScannerId ) { 
		HT4C_TRY {
			if( asyncScannerId ) {
				reinterpret_cast<Hypertable::TableScannerAsync*>(asyncScannerId)->cancel();
			}
		}
		HT4C_RETHROW
	}

	void HyperBlockingAsyncResult::cancelAsyncMutator( int64_t asyncMutatorId ) {
		HT4C_TRY {
			if( asyncMutatorId ) {
				reinterpret_cast<Hypertable::TableMutatorAsync*>(asyncMutatorId)->cancel();
			}
		}
		HT4C_RETHROW
	}

	bool HyperBlockingAsyncResult::isCompleted( ) const {
		HT4C_TRY {
			return future ? future->is_done() : true;
		}
		HT4C_RETHROW
	}

	bool HyperBlockingAsyncResult::isCancelled( ) const {
		HT4C_TRY {
			return future ? future->is_cancelled() : false;
		}
		HT4C_RETHROW
	}

	bool HyperBlockingAsyncResult::isEmpty( ) const {
		HT4C_TRY {
			return future ? future->is_empty() : false;
		}
		HT4C_RETHROW
	}

	bool HyperBlockingAsyncResult::getCells( Common::AsyncResultSink* asyncResultSink ) {
		HT4C_TRY {
			if( future && asyncResultSink ) {
				ResultPtr result;
				if( future->get(result) ) {
					return publishResult( result, asyncResultSink );
				}
			}
			return false;
		}
		HT4C_RETHROW
	}

	bool HyperBlockingAsyncResult::getCells( Common::AsyncResultSink* asyncResultSink, uint32_t timeoutMsec, bool& timedOut ) {
		HT4C_TRY {
			timedOut = false;
			if( future && asyncResultSink ) {
				ResultPtr result;
				if( future->get(result, timeoutMsec, timedOut) ) {
					return publishResult( result, asyncResultSink );
				}
			}
			return false;
		}
		HT4C_RETHROW
	}

	bool HyperBlockingAsyncResult::publishResult( ResultPtr& result, Common::AsyncResultSink* asyncResultSink ) {
		if( result->is_error() ) {
			future->cancel();
			int error;
			String error_msg;
			result->get_error( error, error_msg );
			Common::HypertableException exception( error, error_msg );
			asyncResultSink->failure( exception );
			throw exception;
		}
		else if( result->is_scan() ) {
			Hypertable::Cells _cells;
			result->get_cells( _cells );
			Common::Cells cells( &_cells );
			switch( asyncResultSink->scannedCells(reinterpret_cast<int64_t>(result->get_scanner()), cells) ) {
				case Common::ACR_Cancel:
						result->get_scanner()->cancel();
						break;
					case Common::ACR_Abort:
						future->cancel();
						return false;
					default:
						break;
			}
			return true;
		}
		return true;
	}

} }