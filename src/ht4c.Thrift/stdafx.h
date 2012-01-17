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

#include "Common/Compat.h"

#pragma warning( push, 3 )

#include "Common/HRTimer.h"
#include "Hypertable/Lib/Namespace.h"

#include "ThriftBroker/Client.h"
#include "ThriftBroker/SerializedCellsWriter.h"
#include "ThriftBroker/SerializedCellsReader.h"

#pragma warning( pop )

#define HT4C_THRIFT_RETRY( c ) \
	{ \
		if( !client->is_open() ) { \
			client->renew_nothrow(); \
		} \
		if( client->is_open() ) { \
			(c); \
		} \
	}

#include "ht4c.Common/Utils.h"

inline const char* CF( const char* columnFamily ) {
	return ht4c::Common::CF( columnFamily );
}

inline uint8_t FLAG( const char* columnFamily, const char* columnQualifier, uint8_t flag ) {
	return ht4c::Common::FLAG( columnFamily, columnQualifier, flag );
}

inline uint8_t FLAG_DELETE( const char* columnFamily, const char* columnQualifier ) {
	return ht4c::Common::FLAG_DELETE( columnFamily, columnQualifier );
}

inline uint64_t TIMESTAMP( uint64_t timestamp, uint8_t flag ) {
	return ht4c::Common::TIMESTAMP( timestamp, flag );
}