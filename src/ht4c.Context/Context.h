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
#pragma managed( push, off )
#endif

#include "ht4c.Common/Context.h"

namespace ht4c { namespace Common {
	class Client;
	class Properties;
} }

namespace ht4c {

	/// <summary>
	/// Represents a Hypertable context, handles connection to a Hypertable instance.
	/// </summary>
	/// <remarks>Links between native and C++/CLI.</remarks>
	/// <seealso cref="ht4c::Common::Context"/>
	class Context : public Common::Context {

		public:

			/// <summary>
			/// Creates a new Context instance.
			/// </summary>
			/// <param name="ctxKind">Context kind</param>
			/// <param name="host">Hypertable instance host name</param>
			/// <param name="port">Hypertable instance port number</param>
			/// <param name="prop">Configuration properties</param>
			/// <param name="loggingLevel">Preferred logging level, might be null or empty</param>
			/// <returns>New Context instance</returns>
			/// <remarks>To free the created instance, use the destroy method.</remarks>
			/// <seealso cref="ht4c::Common::ContextKind"/>
			/// <seealso cref="ht4c::Common::Properties"/>
			static Context* create( Common::ContextKind ctxKind, const char* host, uint16_t port, const Common::Properties& prop, const char* loggingLevel );

			/// <summary>
			/// Creates a new Context instance.
			/// </summary>
			/// <param name="ctxKind">Context kind</param>
			/// <param name="commandLine">Command line arguments</param>
			/// <param name="includesModuleFileName">If true the commandLine parameter contains the module filename</param>
			/// <param name="loggingLevel">Preferred logging level, might be null or empty</param>
			/// <returns>New Context instance</returns>
			/// <remarks>To free the created instance, use the destroy method.</remarks>
			/// <seealso cref="ht4c::Common::ContextKind"/>
			static Context* create( Common::ContextKind ctxKind, const char* commandLine, bool includesModuleFileName, const char* loggingLevel );

			/// <summary>
			/// Destroys the Context instance.
			/// </summary>
			/// <param name="ctx">Context to destroy</param>
			static void destroy( Context* ctx );

			/// <summary>
			/// Frees resources used.
			/// </summary>
			static void shutdown( );

			/// <summary>
			/// Destroys the Context instance.
			/// </summary>
			virtual ~Context( );

			#pragma region Common::Context methods

			virtual Common::Client* createClient( );
			virtual Common::ContextKind getContextKind( ) const;
			virtual void getProperties( Common::Properties& prop ) const;

			#pragma endregion

			#ifndef __cplusplus_cli

			/// <summary>
			/// Returns the underlying Hypertable properties.
			/// </summary>
			/// <returns>Underlying Hypertable properties</returns>
			/// <remarks>Pure native method.</remarks>
			Hypertable::PropertiesPtr getProperties( ) const;

			/// <summary>
			/// Returns the Hypertable connection manager.
			/// </summary>
			/// <returns>Hypertable connection manager</returns>
			/// <remarks>Pure native method.</remarks>
			Hypertable::ConnectionManagerPtr getConnectionManager( );

			/// <summary>
			/// Returns the Hyperspace session.
			/// </summary>
			/// <returns>Hyperspace session</returns>
			/// <remarks>Pure native method.</remarks>
			Hyperspace::SessionPtr getHyperspaceSession( );

			/// <summary>
			/// Returns the communication manager.
			/// </summary>
			/// <returns>Communication manager</returns>
			/// <remarks>Pure native method.</remarks>
			Hyperspace::Comm* getComm( );

			/// <summary>
			/// Returns the thrift client.
			/// </summary>
			/// <returns>Thrift client</returns>
			/// <remarks>Pure native method.</remarks>
			Hypertable::Thrift::ClientPtr getThriftClient( );

			#endif

		private:

			Context( const Context& ) { }
			Context& operator = ( const Context& ) { return *this; }

			#ifndef __cplusplus_cli

			typedef std::pair<Hypertable::ConnectionManagerPtr, uint32_t> connection_t;
			typedef std::map<Hyperspace::SessionPtr, connection_t> sessions_t;

			Context( Common::ContextKind ctxKind, Hypertable::PropertiesPtr prop, const char* loggingLevel );

			static Hyperspace::SessionPtr findSession( Hypertable::PropertiesPtr prop, Hypertable::ConnectionManagerPtr& connMgr );
			static void registerSession( Hyperspace::SessionPtr session, Hypertable::ConnectionManagerPtr connMgr );
			static void unregisterSession( Hyperspace::SessionPtr session );

			static Hypertable::PropertiesPtr getProperties( int argc, char *argv[] );
			static Hypertable::PropertiesPtr getProperties( const char* commandLine, bool includesModuleFileName );
			static char** commandLineToArgv( const char* commandLine, int& argc, bool includesModuleFileName );
			static bool getPropValue( Hypertable::PropertiesPtr prop, const std::string& name, boost::any& value );

			Hypertable::PropertiesPtr prop;
			Hypertable::ConnectionManagerPtr connMgr;
			Hyperspace::SessionPtr session;
			Hypertable::Thrift::ClientPtr thriftClient;
			Common::ContextKind ctxKind;

			static Hypertable::RecMutex mutex;
			static sessions_t sessions;

			#endif
	};

}

#ifdef __cplusplus_cli
#pragma managed( pop )
#endif