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
#include "ThriftFactory.h"
#include "ThriftException.h"

#include "ThriftBroker/Client.h"

namespace ht4c { namespace Thrift {

	Hypertable::Thrift::ThriftClient* ThriftFactory::create( const std::string &host, int port, int timeout_ms ) {
		HT4C_TRY {
			return new Hypertable::Thrift::ThriftClient( host, port, timeout_ms );
		}
		HT4C_THRIFT_RETHROW
	}

} }