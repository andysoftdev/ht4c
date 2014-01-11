/** -*- C++ -*-
 * Copyright (C) 2010-2014 Thalmann Software & Consulting, http://www.softdev.ch
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
#include "HyperAsyncResult.h"

#include "ht4c.Common/AsyncResultSink.h"
#include "ht4c.Common/AsyncTableScanner.h"
#include "ht4c.Common/Cells.h"
#include "ht4c.Common/Exception.h"

namespace ht4c { namespace Hyper {
	using namespace Hypertable;

	class HyperAsyncResultCallback : public Hypertable::FutureCallback {

		public:

			explicit HyperAsyncResultCallback( Common::AsyncResultSink* _asyncResultSink ) 
				: asyncResultSink( _asyncResultSink )
			{
			}

	protected:

			virtual void deregister_scanner( Hypertable::TableScannerAsync* scanner ) {
				Hypertable::FutureCallback::deregister_scanner( scanner );
				if( asyncResultSink ) {
					asyncResultSink->detachAsyncScanner( reinterpret_cast<int64_t>(scanner) );
				}
			}

			virtual void scan_ok( Hypertable::TableScannerAsync* scanner, ScanCellsPtr &result ) {
				if( result && asyncResultSink && !is_cancelled() && !scanner->is_cancelled() ) {
					Hypertable::Cells _cells;
					result->get( _cells );
					Common::Cells cells( &_cells );
					switch( asyncResultSink->scannedCells(reinterpret_cast<int64_t>(scanner), cells) ) {
						case Common::ACR_Cancel:
							scanner->cancel();
							break;
						case Common::ACR_Abort:
							cancel();
							break;
						default:
							break;
					}
				}
			}

			virtual void scan_error( TableScannerAsync* /*scanner*/, int error, const String &error_msg, bool /*eos*/ ) {
				if( asyncResultSink && error != Error::OK ) {
					cancel();
					Common::HypertableException exception( error, error_msg );
					asyncResultSink->failure( exception );
				}
			}

			virtual void deregister_mutator( Hypertable::TableMutatorAsync* mutator ) {
				Hypertable::FutureCallback::deregister_mutator( mutator );
				if( asyncResultSink ) {
					asyncResultSink->detachAsyncMutator( reinterpret_cast<int64_t>(mutator) );
				}
			}

			virtual void update_ok( Hypertable::TableMutatorAsync* /*mutator*/ ) {
				//FIXME, client callback?
			}

			virtual void update_error( Hypertable::TableMutatorAsync* /*mutator*/, int error, Hypertable::FailedMutations& /*failedMutations*/ ) {
				if( asyncResultSink && error != Error::OK ) {
					cancel();
					Common::HypertableException exception( error, "" );
					asyncResultSink->failure( exception );
				}
			}

		private:

			HyperAsyncResultCallback( const HyperAsyncResultCallback& ) { }
			HyperAsyncResultCallback& operator = ( const HyperAsyncResultCallback& ) { return *this; }

			Common::AsyncResultSink* asyncResultSink;
	};

	HyperAsyncResult* HyperAsyncResult::create( Common::AsyncResultSink* asyncResultSink ) {
		HT4C_TRY {
			return new HyperAsyncResult( asyncResultSink );
		}
		HT4C_RETHROW
	}

	HyperAsyncResult::HyperAsyncResult( Common::AsyncResultSink* _asyncResultSink )
	: fcb( new HyperAsyncResultCallback(_asyncResultSink) )
	{
	}

	HyperAsyncResult::~HyperAsyncResult( )
	{
		if( fcb ) {
			fcb->wait_for_completion();
		}
		fcb = 0;
	}

	void HyperAsyncResult::join( ) {
		HT4C_TRY {
			if( fcb ) {
				fcb->wait_for_completion();
			}
		}
		HT4C_RETHROW
	}

	void HyperAsyncResult::cancel( ) {
		HT4C_TRY {
			if( fcb ) {
				fcb->cancel();
			}
		}
		HT4C_RETHROW
	}

	void HyperAsyncResult::cancelAsyncScanner( int64_t asyncScannerId ) { 
		HT4C_TRY {
			if( asyncScannerId ) {
				reinterpret_cast<Hypertable::TableScannerAsync*>(asyncScannerId)->cancel();
			}
		}
		HT4C_RETHROW
	}

	void HyperAsyncResult::cancelAsyncMutator( int64_t asyncMutatorId ) {
		HT4C_TRY {
			if( asyncMutatorId ) {
				reinterpret_cast<Hypertable::TableMutatorAsync*>(asyncMutatorId)->cancel();
			}
		}
		HT4C_RETHROW
	}

	bool HyperAsyncResult::isCompleted( ) const {
		HT4C_TRY {
			return fcb ? fcb->is_done() : true;
		}
		HT4C_RETHROW
	}

	bool HyperAsyncResult::isCancelled( ) const {
		HT4C_TRY {
			return fcb ? fcb->is_cancelled() : false;
		}
		HT4C_RETHROW
	}

} }