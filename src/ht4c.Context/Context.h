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
			/// <param name="properties">Configuration properties</param>
			/// <returns>New Context instance</returns>
			/// <remarks>To free the created instance, use the destroy method.</remarks>
			/// <seealso cref="ht4c::Common::Properties"/>
			static Context* create( const Common::Properties& properties );

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
			/// Merges the connection string with all default properties and those from a configuration file.
			/// </summary>
			/// <param name="connectionString">Connection string, might be null or empty</param>
			/// <param name="loggingLevel">Preferred logging level, might be null or empty</param>
			/// <param name="properties">Configuration properties</param>
			/// <seealso cref="ht4c::Common::Properties"/>
			static void mergeProperties( const char* connectionString, const char* loggingLevel, Common::Properties& properties );

			/// <summary>
			/// Destroys the Context instance.
			/// </summary>
			virtual ~Context( );

			#pragma region Common::Context methods

			virtual Common::Client* createClient( );
			virtual void getProperties( Common::Properties& properties ) const;
			virtual bool hasFeature( Common::ContextFeature contextFeature ) const;

			#pragma endregion

			#ifndef __cplusplus_cli

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

#ifdef SUPPORT_HAMSTERDB

			/// <summary>
			/// Returns the hamster db environment.
			/// </summary>
			/// <returns>Hamster db environment</returns>
			/// <remarks>Pure native method.</remarks>
			Hamster::HamsterEnvPtr getHamsterEnv( );

#endif

			#endif

		private:

			Context( const Context& ) { }
			Context& operator = ( const Context& ) { return *this; }

			#ifndef __cplusplus_cli

			typedef std::pair<Hypertable::ConnectionManagerPtr, uint32_t> connection_t;
			typedef std::map<Hyperspace::SessionPtr, connection_t> sessions_t;

			Context( Common::ContextKind contextKind, Hypertable::PropertiesPtr properties );

			static Hyperspace::SessionPtr findSession( Hypertable::PropertiesPtr properties, Hypertable::ConnectionManagerPtr& connMgr );
			static void registerSession( Hyperspace::SessionPtr session, Hypertable::ConnectionManagerPtr connMgr );
			static void unregisterSession( Hyperspace::SessionPtr session );

			static Hypertable::PropertiesPtr initializeProperties( int argc, char *argv[], const Common::Properties& properties, const char* loggingLevel );
			static Hypertable::PropertiesPtr convertProperties( const Common::Properties& properties );
			static char** commandLineToArgv( const char* commandLine, int& argc );
			static bool getPropValue( Hypertable::PropertiesPtr properties, const std::string& name, boost::any& value );
			static Common::ContextKind getContextKind( Hypertable::PropertiesPtr properties );

			Common::ContextKind contextKind;
			Hypertable::PropertiesPtr properties;
			Hypertable::ConnectionManagerPtr connMgr;
			Hyperspace::SessionPtr session;
			Hypertable::Thrift::ClientPtr thriftClient;

#ifdef SUPPORT_HAMSTERDB

			struct stricmp_t {
				bool operator () ( const std::string& a, const std::string& b ) const {
					return stricmp( a.c_str(), b.c_str() ) < 0;
				}
			};

			typedef std::pair<Hamster::HamsterEnvPtr, uint32_t> hamster_env_t;
			typedef std::map<std::string, hamster_env_t> hamster_envs_t;

			Hamster::HamsterEnvPtr hamsterEnv;

			static hamster_envs_t hamsterEnvs;

#endif

			static Hypertable::RecMutex mutex;
			static sessions_t sessions;

			#endif
	};

}

#ifdef __cplusplus_cli
#pragma managed( pop )
#endif