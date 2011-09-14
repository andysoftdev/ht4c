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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "ht4c.Common/Exception.h"

/// <summary>
/// Defines the catch clause of ht4c thrift try/catch blocks.
/// </summary>
/// <remarks>
/// Translates thrift client exceptions, protocol and transport exceptions into ht4c exceptions. Handles
/// reconnection automatically.
/// </remarks>
#define HT4C_THRIFT_RETHROW \
	catch( Hypertable::ThriftGen::ClientException& e ) { \
		std::stringstream ss; \
		ss << e.message << "\n\tat " << __FUNCTION__ << " (" << __FILE__ << ':' << __LINE__ << ')'; \
		throw ht4c::Common::HypertableException( e.code, ss.str(), __LINE__, __FUNCTION__, __FILE__ ); \
	} \
	catch( apache::thrift::protocol::TProtocolException& e ) { \
		__if_exists (client) { if( client && !client->is_open() ) client->renew_nothrow(); } \
		std::stringstream ss; \
		ss << e.what() << "\n\tat " << __FUNCTION__ << " (" << __FILE__ << ':' << __LINE__ << ')'; \
		throw ht4c::Common::HypertableException( Hypertable::Error::PROTOCOL_ERROR, ss.str(), __LINE__, __FUNCTION__, __FILE__ ); \
	} \
	catch( apache::thrift::transport::TTransportException& e ) { \
		__if_exists (client) { if( client && !client->is_open() ) client->renew_nothrow(); } \
		std::stringstream ss; \
		ss << e.what() << "\n\tat " << __FUNCTION__ << " (" << __FILE__ << ':' << __LINE__ << ')'; \
		if( e.getType() == apache::thrift::transport::TTransportException::NOT_OPEN ) \
			throw ht4c::Common::HypertableException( Hypertable::Error::COMM_NOT_CONNECTED, ss.str(), __LINE__, __FUNCTION__, __FILE__ ); \
		if( e.getType() == apache::thrift::transport::TTransportException::TIMED_OUT ) \
			throw ht4c::Common::HypertableException( Hypertable::Error::REQUEST_TIMEOUT, ss.str(), __LINE__, __FUNCTION__, __FILE__ ); \
		throw ht4c::Common::HypertableException( Hypertable::Error::COMM_SOCKET_ERROR, ss.str(), __LINE__, __FUNCTION__, __FILE__ ); \
	} \
	catch( apache::thrift::TException& e ) { \
		__if_exists (client) { if( client && !client->is_open() ) client->renew_nothrow(); } \
		std::stringstream ss; \
		ss << e.what() << "\n\tat " << __FUNCTION__ << " (" << __FILE__ << ':' << __LINE__ << ')'; \
		throw ht4c::Common::HypertableException( Hypertable::Error::EXTERNAL, ss.str(), __LINE__, __FUNCTION__, __FILE__ ); \
	} \
	HT4C_RETHROW
