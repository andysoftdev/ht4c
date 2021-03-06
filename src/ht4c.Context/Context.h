/** -*- C++ -*-
 * Copyright (C) 2010-2016 Thalmann Software & Consulting, http://www.softdev.ch
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
#pragma managed( push, off )
#endif

#include "ht4c.Common/Context.h"
#include "ht4c.Common/SessionState.h"

namespace ht4c { namespace Common {
	class Client;
	class Properties;
} }

namespace ht4c {

	class SessionCallback;

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
			/// Frees resources used.
			/// </summary>
			static void shutdown( );

			/// <summary>
			/// Returns true if the implementation supports the context kind specified, otherwise false.
			/// </summary>
			/// <param name="contextKind">The context kind</param>
			/// <returns> Returns true if the implementation supports the context kind specified, otherwise false</returns>
			/// <seealso cref="ht4c::Common::ContextKind"/>
			static bool hasContext( Common::ContextKind contextKind );

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
			virtual void addSessionStateSink( Common::SessionStateSink* SessionStateSink );
			virtual void removeSessionStateSink( Common::SessionStateSink* SessionStateSink );

			#pragma endregion

	private:

			friend class SessionCallback;

			#ifndef __cplusplus_cli

#ifdef SUPPORT_HYPERTABLE

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
			/// Returns the application queue.
			/// </summary>
			/// <returns>Application queue</returns>
			/// <remarks>Pure native method.</remarks>
			Hypertable::ApplicationQueueInterfacePtr getApplicationQueue( );

			/// <summary>
			/// Returns the communication manager.
			/// </summary>
			/// <returns>Communication manager</returns>
			/// <remarks>Pure native method.</remarks>
			Hyperspace::Comm* getComm( );

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

			/// <summary>
			/// Returns the thrift client.
			/// </summary>
			/// <returns>Thrift client</returns>
			/// <remarks>Pure native method.</remarks>
			Hypertable::Thrift::ThriftClientPtr getThriftClient( );

#endif

#ifdef SUPPORT_HAMSTERDB

			/// <summary>
			/// Returns the hamster db environment.
			/// </summary>
			/// <returns>Hamster db environment</returns>
			/// <remarks>Pure native method.</remarks>
			Hamster::HamsterEnvPtr getHamsterEnv( );

#endif

#ifdef SUPPORT_SQLITEDB

			/// <summary>
			/// Returns the sqlite db environment.
			/// </summary>
			/// <returns>SQLite db environment</returns>
			/// <remarks>Pure native method.</remarks>
			SQLite::SQLiteEnvPtr getSQLiteEnv( );

#endif

#ifdef SUPPORT_ODBC

			/// <summary>
			/// Returns the ODBC db environment.
			/// </summary>
			/// <returns>ODBC db environment</returns>
			/// <remarks>Pure native method.</remarks>
			Odbc::OdbcEnvPtr getOdbcEnv( );

#endif

			#endif

			Context( const Context& ) { }
			Context& operator = ( const Context& ) { return *this; }

			#ifndef __cplusplus_cli

			typedef std::pair<Hypertable::ConnectionManagerPtr, uint32_t> connection_t;
			typedef std::map<Hyperspace::SessionPtr, connection_t> sessions_t;
			typedef std::set<Common::SessionStateSink*> sessionStateSinks_t;

			struct stricmp_t {
				bool operator () ( const std::string& a, const std::string& b ) const {
					return stricmp( a.c_str(), b.c_str() ) < 0;
				}
			};

			Context( Common::ContextKind contextKind, Hypertable::PropertiesPtr properties );

#ifdef SUPPORT_HYPERTABLE

			void fireSessionStateChanged( Common::SessionState oldSessionState, Common::SessionState newSessionState );

			static Hyperspace::SessionPtr findSession( Hypertable::PropertiesPtr properties, Hypertable::ConnectionManagerPtr& connMgr );
			static void registerSession( Hyperspace::SessionPtr session, Hypertable::ConnectionManagerPtr connMgr );
			static void unregisterSession( Hyperspace::SessionPtr session );

#endif

			static Hypertable::PropertiesPtr initializeProperties( int argc, char *argv[], const Common::Properties& properties, const char* loggingLevel );
			static Hypertable::PropertiesPtr convertProperties( const Common::Properties& properties );
			static char** commandLineToArgv( const char* commandLine, int& argc );
			static bool getPropValue( Hypertable::PropertiesPtr properties, const std::string& name, boost::any& value );
			static Common::ContextKind getContextKind( Hypertable::PropertiesPtr properties );

			std::mutex ctxMutex;
			Common::ContextKind contextKind;
			Hypertable::PropertiesPtr properties;

#ifdef SUPPORT_HYPERTABLE

			Hypertable::ConnectionManagerPtr connMgr;
			Hypertable::ApplicationQueueInterfacePtr appQueue;
			Hyperspace::SessionPtr session;
			sessionStateSinks_t sessionStateSinks;
			SessionCallback* sessionCallback;

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

			Hypertable::Thrift::ThriftClientPtr thriftClient;

#endif

#ifdef SUPPORT_HAMSTERDB

			typedef std::pair<Hamster::HamsterEnvPtr, uint32_t> hamster_env_t;
			typedef std::map<std::string, hamster_env_t> hamster_envs_t;

			Hamster::HamsterEnvPtr hamsterEnv;
			static hamster_envs_t hamsterEnvs;

#endif

#ifdef SUPPORT_SQLITEDB

			typedef std::pair<SQLite::SQLiteEnvPtr, uint32_t> sqlite_env_t;
			typedef std::unordered_map<std::string, sqlite_env_t> sqlite_envs_t;

			SQLite::SQLiteEnvPtr sqliteEnv;
			static sqlite_envs_t sqliteEnvs;

#endif

#ifdef SUPPORT_ODBC

			Odbc::OdbcEnvPtr odbcEnv;

#endif

			static boost::mutex envMutex;
			static sessions_t sessions;

			#endif
	};

}

#ifdef __cplusplus_cli
#pragma managed( pop )
#endif