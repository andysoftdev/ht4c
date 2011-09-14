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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "stdafx.h"
#include "ThriftTableScanner.h"
#include "ThriftException.h"

namespace ht4c { namespace Thrift {

	Common::TableScanner* ThriftTableScanner::create( Hypertable::Thrift::ClientPtr client, const Hypertable::ThriftGen::Scanner& tableScanner ) {
		HT4C_TRY {
			return new ThriftTableScanner( client, tableScanner );
		}
		HT4C_THRIFT_RETHROW
	}


	ThriftTableScanner::~ThriftTableScanner( ) {
		HT4C_TRY {
			{
				ThriftClientLock sync( client.get() );
				HT4C_THRIFT_RETRY( client->close_scanner(tableScanner) );
			}
			if( reader ) delete reader;
			client = 0;
		}
		HT4C_THRIFT_RETHROW
	}

	bool ThriftTableScanner::next( Common::Cell*& _cell ) {
		HT4C_TRY {
			while( !eos ) {
				if( !reader ) {
					{
						ThriftClientLock sync( client.get() );
						client->next_cells_serialized( cells, tableScanner );
					}
					reader = new Hypertable::SerializedCellsReader( reinterpret_cast<void*>(const_cast<char*>(cells.c_str())), (uint32_t)cells.length() );
				}
			
				if( reader->next() ) {
					reader->get( cell.get() );
					_cell = &cell;
					return true;
				}
				eos = reader->eos();
				delete reader;
				reader = 0;
			}
			return false;
		}
		HT4C_THRIFT_RETHROW
	}

	ThriftTableScanner::ThriftTableScanner( Hypertable::Thrift::ClientPtr _client, const Hypertable::ThriftGen::Scanner& _tableScanner )
	: client( )
	, tableScanner( _tableScanner )
	, reader( 0 )
	, eos( false )
	{
		HT4C_TRY {
			client = _client;
		}
		HT4C_THRIFT_RETHROW
	}

} }