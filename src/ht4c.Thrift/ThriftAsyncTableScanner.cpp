/** -*- C++ -*-
 * Copyright (C) 2010-2012 Thalmann Software & Consulting, http://www.softdev.ch
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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "stdafx.h"
#include "ThriftAsyncTableScanner.h"
#include "ThriftException.h"

namespace ht4c { namespace Thrift {

	Common::AsyncTableScanner* ThriftAsyncTableScanner::create( Hypertable::Thrift::ClientPtr client, const Hypertable::ThriftGen::ScannerAsync& tableScanner ) {
		HT4C_TRY {
			return new ThriftAsyncTableScanner( client, tableScanner );
		}
		HT4C_THRIFT_RETHROW
	}

	int64_t ThriftAsyncTableScanner::id( const Hypertable::ThriftGen::ScannerAsync& tableScanner ) {
		return tableScanner;
	}

	ThriftAsyncTableScanner::~ThriftAsyncTableScanner( ) {
		HT4C_TRY {
			ThriftClientLock sync( client.get() );
			HT4C_THRIFT_RETRY( client->async_scanner_close(tableScanner) );
		}
		HT4C_THRIFT_RETHROW
	}

	int64_t ThriftAsyncTableScanner::id( ) const {
		return id( tableScanner );
	}

	ThriftAsyncTableScanner::ThriftAsyncTableScanner( Hypertable::Thrift::ClientPtr _client, const Hypertable::ThriftGen::ScannerAsync& _tableScanner )
	: client( )
	, tableScanner( _tableScanner )
	{
		HT4C_TRY {
			client = _client;
		}
		HT4C_THRIFT_RETHROW
	}

} }