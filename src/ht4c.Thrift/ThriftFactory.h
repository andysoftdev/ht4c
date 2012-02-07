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
#error compile native
#endif

namespace Hypertable { namespace Thrift {
	class ThriftClient;
} }

namespace ht4c { namespace Thrift {

	/// <summary>
	/// Represents thrift client factory.
	/// </summary>
	class ThriftFactory {

		public:

			/// <summary>
			/// Creates a new ThriftClient instance.
			/// </summary>
			/// <param name="host">Thrift broker host</param>
			/// <param name="port">Thrift broker port</param>
			/// <param name="connectionTimeout_ms">Connection time out [ms]</param>
			/// <param name="timeout_ms">Send/receive time out [ms]</param>
			/// <returns>New ThriftClient instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Hypertable::Thrift::ThriftClient* create( const std::string &host, int port, int connectionTimeout_ms, int timeout_ms );
	};

} }
