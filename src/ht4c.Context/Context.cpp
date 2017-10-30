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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "stdafx.h"

#include "Context.h"
#include "Logging.h"

#include "ht4c.Common/Config.h"
#include "ht4c.Common/Properties.h"
#include "ht4c.Common/SessionStateSink.h"
#include "ht4c.Common/Exception.h"
#include "ht4c.Common/CU82W.h"
#include "ht4c.Common/CW2U8.h"

#ifdef SUPPORT_HYPERTABLE

#include "ht4c.Hyper/HyperClient.h"

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

#include "ht4c.Thrift/ThriftFactory.h"
#include "ht4c.Thrift/ThriftClient.h"

#endif

#pragma warning( push, 3 )

#include "Common/Init.h"

#ifdef SUPPORT_HYPERTABLE

#include "FsBroker/Lib/Config.h"
#include "Hyperspace/Config.h"
#include "Hypertable/Lib/Config.h"

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

#include "ThriftBroker/Config.h"

#endif

#pragma warning( pop )

namespace ht4c {

	using namespace Hypertable;
	using namespace Hypertable::Config;

#ifdef SUPPORT_HYPERTABLE

	class SessionCallback : public Hyperspace::SessionCallback {

		public:

			SessionCallback( Context* _ctx )
				: ctx( _ctx )
				, oldSessionState( Common::SS_Jeopardy )
			{
			}

		private:

			virtual void safe() {
				if( oldSessionState != Common::SS_Safe ) {
					ctx->fireSessionStateChanged( oldSessionState, Common::SS_Safe );
					oldSessionState = Common::SS_Safe;
				}
			}

			virtual void expired() {
				if( oldSessionState != Common::SS_Expired ) {
					ctx->fireSessionStateChanged( oldSessionState, Common::SS_Expired );
					oldSessionState = Common::SS_Expired;
				}
			}

			virtual void jeopardy() {
				if( oldSessionState != Common::SS_Jeopardy ) {
					ctx->fireSessionStateChanged( oldSessionState, Common::SS_Jeopardy );
					oldSessionState = Common::SS_Jeopardy;
				}
			}

			virtual void disconnected() {
				if( oldSessionState != Common::SS_Disconnected ) {
					ctx->fireSessionStateChanged( oldSessionState, Common::SS_Disconnected );
					oldSessionState = Common::SS_Disconnected;
				}
			}

			virtual void reconnected() {
				if( oldSessionState != Common::SS_Safe ) {
					ctx->fireSessionStateChanged( oldSessionState, Common::SS_Safe );
					oldSessionState = Common::SS_Safe;
				}
			}

			Context* ctx;
			Common::SessionState oldSessionState;
	};

#endif

	namespace {

		const char* localhost													= "net.tcp://localhost";
		const char* requestTimeout										= "Hypertable.Request.Timeout";
		const char* hyperspace												= "hyperspace";
		const char* hyperspaceHost										= "hs-host";
		const char* hyperspacePort										= "hs-port";
		const char* thriftBroker											= "thrift-broker";
		const char* thriftBrokerHost									= "thrift-host";
		const char* thriftBrokerPort									= "thrift-port";
		const char* thriftBrokerTimeout								= "thrift-timeout";
		const char* loggingLevel											= "logging-level";
		const char* verbose														= "verbose";
		const char* silent														= "silent";
		const char* leaseInterval											= "Hyperspace.Lease.Interval";
		const char* gracePeriod												= "Hyperspace.GracePeriod";
		const char* sessionReconnect									= "Hyperspace.Session.Reconnect"; // after lost connection the reconnection will be initiated after Hyperspace.GracePeriod (default 1min)

		const uint16_t defaultHyperspacePort					= 15861;
		const uint16_t defaultThriftBrokerPort				= 15867;

		const int32_t defaultConnectionTimeoutMsec		= 30000;
		const int32_t defaultLeaseIntervalMsec				= 1000000;
		const int32_t defaultGracePeriodMsec					= 20000;
		const bool defaultSessionReconnect						= true;

		template<typename SequenceT, typename Range1T, typename Range2T>
		inline void replace_all_recursive( SequenceT& input, const Range1T& search, const Range2T& format ) {
			SequenceT previous;
			do {
				previous = input;
				boost::replace_all( input, search, format );
			}
			while( previous != input );
		}

		inline bool expand_environment_strings( std::string& str ) {
			std::wstring wstr( ht4c::Common::CU82W(str.c_str()) );
			int cc = ExpandEnvironmentStringsW( wstr.c_str(), 0, 0 );
			if( cc ) {
				wchar_t* wsz = static_cast<wchar_t*>( malloc((cc + 1) * sizeof(wchar_t)) );
				if( wsz ) {
					if( ExpandEnvironmentStringsW(wstr.c_str(), wsz, cc) ) {
						if( wstr != wsz ) {
							str = ht4c::Common::CW2U8(wsz);
							free(wsz);
							return true;
						}
					}
					free(wsz);
				}
			}
			return false;
		}

		typedef Meta::list<

#ifdef SUPPORT_HYPERTABLE

			FsClientPolicy
		, HyperspaceClientPolicy
		, MasterClientPolicy
		, RangeServerClientPolicy,

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

			ThriftClientPolicy,

#endif

#if defined(SUPPORT_HYPERTABLE) || defined(SUPPORT_HYPERTABLE_THRIFT)

			DefaultCommPolicy

#else

			DefaultPolicy

#endif

		> PolicyList;

		typedef Join<PolicyList>::type JoinedPolicyList;

		struct Policies : JoinedPolicyList {

			static PropertiesPtr initialProperties;
			static std::string prefferedLoggingLevel;

			static void init_options( ) {
				Config::allow_unregistered_options( true );

				const char* defaultProvider = "";

#ifdef SUPPORT_HYPERTABLE

				defaultProvider = Common::Config::ProviderHyper;

#elif SUPPORT_HYPERTABLE_THRIFT

				defaultProvider = Common::Config::ProviderThrift;

#elif SUPPORT_SQLITEDB

				defaultProvider = Common::Config::ProviderSQLite;

#elif SUPPORT_HAMSTERDB

				defaultProvider = Common::Config::ProviderHamster;

#elif SUPPORT_ODBC

				defaultProvider = Common::Config::ProviderOdbc;

#endif

				cmdline_desc().add_options()
					(Common::Config::ProviderNameAlias, str()->default_value(defaultProvider), format("Provider name (default: %s)\n", defaultProvider).c_str())
					(Common::Config::UriAlias, str()->default_value(localhost), "Uri hostname[:port] (default: net.tcp://localhost)\n")
					(Common::Config::ConnectionTimeoutAlias, i32()->default_value(defaultConnectionTimeoutMsec), "Connection timeout [ms] (default: 30000)\n");

				alias( Common::Config::ProviderNameAlias, Common::Config::ProviderName );
				alias( Common::Config::UriAlias, Common::Config::Uri );
				alias( Common::Config::ConnectionTimeoutAlias, Common::Config::ConnectionTimeout );

				file_desc().add_options()
					(Common::Config::ProviderName, str()->default_value(defaultProvider), format("Provider name (default: %s)\n", defaultProvider).c_str())
					(Common::Config::Uri, str()->default_value(localhost), "Uri hostname[:port] (default: net.tcp://localhost)\n")
					(Common::Config::ConnectionTimeout, i32()->default_value(defaultConnectionTimeoutMsec), "Connection timeout [ms] (default: 30000)\n");

#ifdef SUPPORT_HAMSTERDB

				file_desc().add_options()
					(Common::Config::HamsterFilename, str(), "Hamster db filename\n")
					(Common::Config::HamsterEnableRecovery, boo()->default_value(false), "Enable or disable hamster db recovery (default: false)\n")
					(Common::Config::HamsterEnableAutoRecovery, boo()->default_value(false), "Enable or disable hamster db auto-recovery (default: false)\n")
					(Common::Config::HamsterCacheSizeMB, i32()->default_value(64), "Hamster db cache size [MB] (default:64)\n")
					(Common::Config::HamsterPageSizeKB, i32()->default_value(64), "Hamster db page size [KB] (default:64)\n");

#endif

#ifdef SUPPORT_SQLITEDB

				file_desc().add_options()
					(Common::Config::SQLiteFilename, str(), "SQLite db filename\n")
					(Common::Config::SQLiteCacheSizeMB, i32()->default_value(64), "SQLite db cache size [MB] (default:64)\n")
					(Common::Config::SQLitePageSizeKB, i32()->default_value(4), "SQLite db page size [KB] (default:4)\n")
					(Common::Config::SQLiteSynchronous, boo()->default_value(false), "SQLite synchronous (default:false)\n")
					(Common::Config::SQLiteUniqueRows, boo()->default_value(false), "SQLite unique rows (default:false)\n")
					(Common::Config::SQLiteNoCellRevisions, boo()->default_value(false), "SQLite no cell revisions (default:false)\n")
					(Common::Config::SQLiteIndexColumn, boo()->default_value(false), "Enables SQLite column index (default:false)\n")
					(Common::Config::SQLiteIndexColumnFamily, boo()->default_value(false), "Enables SQLite column family index (default:false)\n")
					(Common::Config::SQLiteIndexColumnQualifier, boo()->default_value(false), "Enables SQLite column qualifier index (default:false)\n")
					(Common::Config::SQLiteIndexTimestamp, boo()->default_value(false), "Enables SQLite timestamp index (default:false)\n");

#endif

#ifdef SUPPORT_ODBC

				file_desc().add_options()
					(Common::Config::OdbcConnectionString, str(), "ODBC connection string\n")
					(Common::Config::OdbcIndexColumn, boo()->default_value(false), "Enables ODBC column index (default:false)\n")
					(Common::Config::OdbcIndexColumnFamily, boo()->default_value(false), "Enables ODBC column family index (default:false)\n")
					(Common::Config::OdbcIndexColumnQualifier, boo()->default_value(false), "Enables ODBC column qualifier index (default:false)\n")
					(Common::Config::OdbcIndexTimestamp, boo()->default_value(false), "Enables ODBC timestamp index (default:false)\n");

#endif

				JoinedPolicyList::init_options();
			}

			static void init( ) {
				PropertiesPtr properties = Config::properties;

				if( initialProperties ) {
					std::vector<String> names;
					initialProperties->get_names( names );
					for each( const String& name in names ) {
						properties->set( name, (*initialProperties)[name] );
					}
					initialProperties = 0;
				}

				properties->sync_aliases();

				if( !prefferedLoggingLevel.empty() ) {
					if( properties->defaulted(loggingLevel) ) {
						properties->set( loggingLevel, prefferedLoggingLevel );
					}
					prefferedLoggingLevel.clear();
				}

				if( properties->defaulted(verbose) && properties->defaulted(silent) ) {
					properties->set( silent, true );
				}

				if( properties->defaulted(leaseInterval) ) {
					properties->set( leaseInterval, defaultLeaseIntervalMsec );
				}
				if( properties->defaulted(gracePeriod) ) {
					properties->set( gracePeriod, defaultGracePeriodMsec );
				}
				if( properties->defaulted(sessionReconnect) ) {
					properties->set( sessionReconnect, defaultSessionReconnect );
				}

				if( !properties->has(thriftBrokerTimeout) ) {
					int32_t timeoutMsec = defaultConnectionTimeoutMsec;
					if( properties->has(Common::Config::ConnectionTimeoutAlias) ) {
						timeoutMsec = properties->get_i32( Common::Config::ConnectionTimeoutAlias );
					}
					properties->set( thriftBrokerTimeout, timeoutMsec );
				}

				properties->sync_aliases();

				std::string providerName = properties->get_str( Common::Config::ProviderName );

				if( false ) {
				}

#ifdef SUPPORT_HYPERTABLE

				else if( providerName == Common::Config::ProviderHyper ) {
					if( !properties->defaulted(Common::Config::Uri) || !properties->has(hyperspace) || properties->defaulted(hyperspace) ) {
						properties->set( hyperspace, get_nettcp_uri(hyperspacePort, defaultHyperspacePort) );
					}
				}

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

				else if( providerName == Common::Config::ProviderThrift ) {
					if( !properties->defaulted(Common::Config::Uri) || !properties->has(thriftBroker) || properties->defaulted(thriftBroker) ) {
						properties->set( thriftBroker, get_nettcp_uri(thriftBrokerPort, defaultThriftBrokerPort) );
					}
				}

#endif

#ifdef SUPPORT_HAMSTERDB

				else if( providerName == Common::Config::ProviderHamster ) {
					set_file_uri( Common::Config::HamsterFilename );
				}

#endif

#ifdef SUPPORT_SQLITEDB

				else if( providerName == Common::Config::ProviderSQLite ) {
					set_file_uri( Common::Config::SQLiteFilename );
				}

#endif

#ifdef SUPPORT_ODBC

				else if( providerName == Common::Config::ProviderOdbc ) {
				}

#endif

				properties->sync_aliases();

				JoinedPolicyList::init();
			}

			static void on_init_error( Exception &e ) {
				Config::cleanup();
				HT_ERROR_OUT << e << HT_END;
				throw e;
			}

			static std::string get_uri( ) {
				PropertiesPtr properties = Config::properties;
				std::string uri = properties->get_str( Common::Config::Uri );
				boost::trim( uri );
				if( expand_environment_strings(uri) ) {
					boost::trim( uri );
					properties->set( Common::Config::UriAlias, uri );
				}
				return uri;
			}

			static std::string get_nettcp_uri( const char* portProperty, uint16_t defaultPort ) {
				std::string uri = get_uri();
				if( uri.empty() ) {
					uri = localhost;
				}
				replace_all_recursive( uri, "//", "/" );
				if( uri.find("net.tcp:/") != 0 ) {
					HT_THROW( Error::CONFIG_BAD_VALUE, "Invalid uri scheme, net.tcp://hostname[:port] required" );
				}
				uri = uri.substr( 9 );
				if( uri.empty() ) {
					HT_THROW( Error::CONFIG_BAD_VALUE, "Empty hostname, net.tcp://hostname[:port] required" );
				}
				if( uri.find(':') == std::string::npos ) {
					uint16_t port = defaultPort;
					if( properties->has(portProperty) ) {
						port = properties->get_i16( portProperty );
					}
					uri = format( "%s:%d", uri.c_str(), port );
				}
				return uri;
			}

			static std::string uri_decode( const std::string& encoded ) {
				std::stringstream decoded;
				for( size_t i = 0; i < encoded.size(); ++i ) {
					if (encoded[i] == '%' && (i + 2) < encoded.size() && isxdigit(encoded[i+1]) && isxdigit(encoded[i+2])) {
						char buf[3];
						++i;
						buf[0] = encoded[i++];
						buf[1] = encoded[i];
						buf[2] = '\0';
						decoded << ((char)strtol(buf, 0, 16));
					}
					else {
						decoded << encoded[i];
					}
				}
				return decoded.str();
			}

			static void uri_to_filename( std::string& uri ) {
				if( uri.find("file://") != 0 ) {
					HT_THROW( Error::CONFIG_BAD_VALUE, "Invalid uri scheme, file:///[drive][/path/]uri required" );
				}
				bool abs = uri.find("file:///") == 0;
				uri = uri.substr( 7 );
				if( abs && uri.size() >= 3 && isalpha(uri[1]) && (uri[2] == ':' || uri[2] == '/') ) {
					boost::trim_left_if( uri, boost::is_any_of("/") );
					if( uri[1] != ':' ) {
						uri.insert(uri.begin() + 1, ':');
					}
				}
				if( uri.empty() ) {
					HT_THROW( Error::CONFIG_BAD_VALUE, "Empty uri, file:///[drive][/path/]uri required" );
				}
				replace_all_recursive( uri, "///", "//" );
				replace_all_recursive( uri, "/", "\\" );
				uri = uri_decode( uri );
			}

			static void set_file_uri( const char* filenameProperty ) {
				if( !properties->defaulted(Common::Config::Uri) || !properties->has(filenameProperty) || properties->defaulted(filenameProperty) ) {
					std::string filename = get_uri();
					uri_to_filename( filename );
					properties->set( filenameProperty, filename );
				}
			}

		};

		PropertiesPtr Policies::initialProperties;
		std::string Policies::prefferedLoggingLevel;

	}

	boost::mutex Context::envMutex;
	Context::sessions_t Context::sessions;

#ifdef SUPPORT_HAMSTERDB

	Context::hamster_envs_t Context::hamsterEnvs;

#endif

#ifdef SUPPORT_SQLITEDB

	Context::sqlite_envs_t Context::sqliteEnvs;

#endif

	Context* Context::create( const Common::Properties& _properties ) {
		HT4C_TRY {
			Hypertable::PropertiesPtr properties = convertProperties( _properties );
			Common::ContextKind contextKind = getContextKind( properties );
			return contextKind != Common::CK_Unknown ? new Context( contextKind, properties ) : 0;
		}
		HT4C_RETHROW
	}

	void Context::shutdown( ) {
		HT4C_TRY {
			{
				boost::lock_guard<boost::mutex> lock( envMutex );

#ifdef SUPPORT_HAMSTERDB

				hamsterEnvs.clear();

#endif

#ifdef SUPPORT_SQLITEDB

				sqliteEnvs.clear();

#endif

				sessions.clear();
			}

#ifdef SUPPORT_HYPERTABLE

			Comm::destroy();

#endif

			Config::cleanup();
			Logging::shutdown();
			delete Logger::get();
		}
		HT4C_RETHROW
	}

	bool Context::hasContext(Common::ContextKind contextKind) {
		switch (contextKind) {

		case Common::CK_Hyper:
#ifdef SUPPORT_HYPERTABLE
			return true;
#else
			return false;
#endif

		case Common::CK_Thrift:
#ifdef SUPPORT_HYPERTABLE_THRIFT
			return true;
#else
			return false;
#endif

		case Common::CK_SQLite:
#ifdef SUPPORT_SQLITEDB
			return true;
#else
			return false;
#endif

		case Common::CK_Hamster:
#ifdef SUPPORT_HAMSTERDB
			return true;
#else
			return false;
#endif

		case Common::CK_ODBC:
#ifdef SUPPORT_ODBC
			return true;
#else
			return false;
#endif

		default:
			break;
		}
		return false;
	}

	void Context::mergeProperties( const char* connectionString, const char* loggingLevel, Common::Properties& _properties ) {
		HT4C_TRY {
			int argc;
			char** argv = Context::commandLineToArgv( connectionString, argc );
			Hypertable::PropertiesPtr properties = initializeProperties( argc, argv, _properties, loggingLevel );
			if( argv ) {
				free( argv );
			}
			std::vector<String> names;
			properties->get_names( names );
			for each( const String& name in names ) {
				_properties.addOrUpdate( name, (*properties)[name] );
			}
		}
		HT4C_RETHROW
	}

	Common::Client* Context::createClient( ) {
		HT4C_TRY {
			boost::lock_guard<boost::mutex> lock( envMutex );

			switch( contextKind ) {

#ifdef SUPPORT_HYPERTABLE

				case Common::CK_Hyper:
					return ht4c::Hyper::HyperClient::create( 
						getConnectionManager(), 
						getHyperspaceSession(), 
						getApplicationQueue(),
						properties );

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

				case Common::CK_Thrift:
					return ht4c::Thrift::ThriftClient::create( getThriftClient() );

#endif

#ifdef SUPPORT_HAMSTERDB

				case Common::CK_Hamster:
					return getHamsterEnv()->createClient( );

#endif

#ifdef SUPPORT_SQLITEDB

				case Common::CK_SQLite:
					return getSQLiteEnv()->createClient( );

#endif

#ifdef SUPPORT_ODBC

				case Common::CK_ODBC:
					return getOdbcEnv()->createClient( );

#endif

			}
			return 0;
		}
		HT4C_RETHROW
	}

	void Context::getProperties( Common::Properties& _properties ) const {
		HT4C_TRY {
			_properties.clear();
			std::vector<String> names;
			properties->get_names( names );
			for each( const String& name in names ) {
				_properties.addOrUpdate( name, (*properties)[name] );
			}
		}
		HT4C_RETHROW
	}

	bool Context::hasFeature( Common::ContextFeature contextFeature ) const {
		switch( contextFeature ) {
		case Common::CF_HQL:
		case Common::CF_AsyncTableMutator:
		case Common::CF_PeriodicFlushTableMutator:
		case Common::CF_AsyncTableScanner:
		case Common::CF_CounterColumn:

#ifdef SUPPORT_HYPERTABLE

			if( contextKind == Common::CK_Hyper ) {
				return true;
			}

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

			if( contextKind == Common::CK_Thrift ) {
				return true;
			}

#endif

			return false;

		case Common::CF_NotifySessionStateChanged:

#ifdef SUPPORT_HYPERTABLE

			if( contextKind == Common::CK_Hyper ) {
				return true;
			}

#endif

			return false;
		default:
			break;
		}
		return false;
	}

	void Context::addSessionStateSink( Common::SessionStateSink* sessionStateSink ) {

#ifdef SUPPORT_HYPERTABLE

		if(  sessionStateSink 
			&& contextKind == Common::CK_Hyper ) {

			std::lock_guard<std::mutex> lock( ctxMutex );

			if( !sessionCallback ) {
				sessionCallback = new SessionCallback( this );

				if( session ) {
					session->add_callback( sessionCallback );
				}
			}

			sessionStateSinks.insert( sessionStateSink );
		}

#endif

	}

	void Context::removeSessionStateSink( Common::SessionStateSink* sessionStateSink ) {

#ifdef SUPPORT_HYPERTABLE

		if( sessionStateSink ) {
			std::lock_guard<std::mutex> lock( ctxMutex );
			sessionStateSinks.erase( sessionStateSink );
		}

#endif

	}

	Context::~Context( ) {

#ifdef SUPPORT_HAMSTERDB

		if( hamsterEnv ) {
			boost::lock_guard<boost::mutex> lock( envMutex );

			for( hamster_envs_t::iterator it = hamsterEnvs.begin(); it != hamsterEnvs.end(); ++it ) {
				if( (*it).second.first == hamsterEnv ) {
					if( --(*it).second.second == 0 ) {
						hamsterEnvs.erase( it );
					}
					break;
				}
			}
			hamsterEnv = 0;
		}

#endif

#ifdef SUPPORT_SQLITEDB

		if( sqliteEnv ) {
			boost::lock_guard<boost::mutex> lock( envMutex );

			for( sqlite_envs_t::iterator it = sqliteEnvs.begin(); it != sqliteEnvs.end(); ++it ) {
				if( (*it).second.first == sqliteEnv ) {
					if( --(*it).second.second == 0 ) {
						sqliteEnvs.erase( it );
					}
					break;
				}
			}
			sqliteEnv = 0;
		}

#endif

#ifdef SUPPORT_ODBC

		sqliteEnv = 0;

#endif

#ifdef SUPPORT_HYPERTABLE

		{
			std::lock_guard<std::mutex> lock( ctxMutex );

			if( sessionCallback ) {
				if( session ) {
					session->remove_callback( sessionCallback );
				}

				delete sessionCallback;
				sessionCallback = 0;
			}

			sessionStateSinks.clear();
		}

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

		thriftClient = 0;

#endif

#ifdef SUPPORT_HYPERTABLE

		if( session ) {
			boost::lock_guard<boost::mutex> lock( envMutex );
			unregisterSession( session );
		}
		session = 0;
		appQueue = 0;

#endif

		properties = 0;
	}

#ifdef SUPPORT_HYPERTABLE

	ConnectionManagerPtr Context::getConnectionManager( ) {
		HT4C_TRY {
			if( !connMgr ) {
				connMgr = std::make_shared<ConnectionManager>( getComm() );
			}
			return connMgr;
		}
		HT4C_RETHROW
	}

	Hyperspace::SessionPtr Context::getHyperspaceSession( ) {
		HT4C_TRY {
			if( !session ) {
				session = findSession( properties, connMgr );
				if( !session ) {
					HT_INFO_OUT << "Creating hyperspace session " << properties->get_str(hyperspace) << HT_END;
					session = std::make_shared<Hyperspace::Session>( getComm(), properties );

					if( sessionCallback ) {
						session->add_callback( sessionCallback );
					}
				}
				registerSession( session, getConnectionManager() );
			}
			return session;
		}
		HT4C_RETHROW
	}

	Hypertable::ApplicationQueueInterfacePtr Context::getApplicationQueue( ) {
		HT4C_TRY {
			if( !appQueue ) {
				int workers = properties->get_i32("Hypertable.Client.Workers");
				appQueue = std::make_shared<ApplicationQueue>( workers );
			}
			return appQueue;
		}
		HT4C_RETHROW
	}

	Hyperspace::Comm* Context::getComm( ) {
		HT4C_TRY {
			return Comm::instance();
		}
		HT4C_RETHROW
	}

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

	Hypertable::Thrift::ThriftClientPtr Context::getThriftClient( ) {
		HT4C_TRY {
			if( !thriftClient ) {
				std::string host = properties->get_str( thriftBrokerHost );
				uint16_t port = properties->get_i16( thriftBrokerPort );
				int32_t connectionTimeoutMsec = properties->get_i32( thriftBrokerTimeout );
				int32_t timeoutMsec = properties->get_i32( requestTimeout );
				HT_INFO_OUT << "Creating thrift client " << host << ":" << port << HT_END;
				thriftClient = ht4c::Thrift::ThriftFactory::create( host, port, connectionTimeoutMsec, timeoutMsec );
			}
			return thriftClient;
		}
		HT4C_RETHROW
	}

#endif

#ifdef SUPPORT_HAMSTERDB

	Hamster::HamsterEnvPtr Context::getHamsterEnv( ) {
		if( !hamsterEnv ) {
			std::string filename = properties->get_str( Common::Config::HamsterFilename );
			hamster_envs_t::iterator it = hamsterEnvs.find( filename );
			if( it == hamsterEnvs.end() ) {
				Hamster::HamsterEnvConfig config;
				config.enableRecovery = properties->get_bool( Common::Config::HamsterEnableRecovery );
				config.enableAutoRecovery = properties->get_bool( Common::Config::HamsterEnableAutoRecovery );
				config.cacheSizeMB = properties->get_i32( Common::Config::HamsterCacheSizeMB );
				config.pageSizeKB = properties->get_i32( Common::Config::HamsterPageSizeKB );

				HT_INFO_OUT << "Creating hamster environment " << filename << HT_END;
				hamsterEnv = Hamster::HamsterFactory::create( filename, config );
				hamsterEnvs.insert( hamster_envs_t::value_type(filename, std::make_pair(hamsterEnv, 1)) );
			}
			else {
				++(*it).second.second;
				hamsterEnv = (*it).second.first;
			}
		}

		return hamsterEnv;
	}

#endif

#ifdef SUPPORT_SQLITEDB

	SQLite::SQLiteEnvPtr Context::getSQLiteEnv( ) {
		if( !sqliteEnv ) {
			std::string filename = properties->get_str( Common::Config::SQLiteFilename );
			sqlite_envs_t::iterator it = sqliteEnvs.find( filename );
			if( it == sqliteEnvs.end() ) {
				SQLite::SQLiteEnvConfig config;
				config.cacheSizeMB = properties->get_i32( Common::Config::SQLiteCacheSizeMB );
				config.pageSizeKB = properties->get_i32( Common::Config::SQLitePageSizeKB );
				config.synchronous = properties->get_bool( Common::Config::SQLiteSynchronous );
				config.uniqueRows = properties->get_bool( Common::Config::SQLiteUniqueRows );
				config.noCellRevisions = properties->get_bool( Common::Config::SQLiteNoCellRevisions );
				config.indexColumn = properties->get_bool( Common::Config::SQLiteIndexColumn );
				config.indexColumnFamily = properties->get_bool( Common::Config::SQLiteIndexColumnFamily );
				config.indexColumnQualifier = properties->get_bool( Common::Config::SQLiteIndexColumnQualifier );
				config.indexTimestamp = properties->get_bool( Common::Config::SQLiteIndexTimestamp );

				HT_INFO_OUT << "Creating sqlite environment " << filename << HT_END;
				sqliteEnv = SQLite::SQLiteFactory::create( filename, config );
				sqliteEnvs.insert( sqlite_envs_t::value_type(filename, std::make_pair(sqliteEnv, 1)) );
			}
			else {
				++(*it).second.second;
				sqliteEnv = (*it).second.first;
			}
		}

		return sqliteEnv;
	}

#endif

#ifdef SUPPORT_ODBC

Odbc::OdbcEnvPtr Context::getOdbcEnv( ) {
		if( !odbcEnv ) {
			std::string connectionString = properties->get_str( Common::Config::OdbcConnectionString );

			Odbc::OdbcEnvConfig config;
			config.indexColumn = properties->get_bool( Common::Config::OdbcIndexColumn );
			config.indexColumnFamily = properties->get_bool( Common::Config::OdbcIndexColumnFamily );
			config.indexColumnQualifier = properties->get_bool( Common::Config::OdbcIndexColumnQualifier );
			config.indexTimestamp = properties->get_bool( Common::Config::OdbcIndexTimestamp );

			HT_INFO_OUT << "Creating odbc environment " << connectionString << HT_END;
			odbcEnv = Odbc::OdbcFactory::create( connectionString, config );
		}

		return odbcEnv;
	}

#endif

	Context::Context( Common::ContextKind _contextKind, Hypertable::PropertiesPtr _properties )
	: contextKind( _contextKind )
	, properties( _properties )

#ifdef SUPPORT_HYPERTABLE

	, sessionCallback( 0 )

#endif
	{
	}

#ifdef SUPPORT_HYPERTABLE

	void Context::fireSessionStateChanged( Common::SessionState oldSessionState, Common::SessionState newSessionState ) {
		sessionStateSinks_t _sessionStateSinks;

		{
			std::lock_guard<std::mutex> lock( ctxMutex );
			_sessionStateSinks = sessionStateSinks;
		}

		for each( Common::SessionStateSink* sink in _sessionStateSinks ) {
			sink->stateTransition( oldSessionState, newSessionState );
		}
	}

	Hyperspace::SessionPtr Context::findSession( Hypertable::PropertiesPtr properties, Hypertable::ConnectionManagerPtr& connMgr ) {
		connMgr = 0;
		if( ReactorRunner::handler_map && sessions.size() ) {
			Hypertable::Strings hosts = properties->get_strs( hyperspaceHost );
			if( hosts.size() && hosts.front().size() ) {
				Hypertable::InetAddr addr( hosts.front(), properties->get_i16(hyperspacePort) );
				for each( sessions_t::value_type item in sessions ) {
					if( item.first->get_master_addr() == addr ) {
						connMgr = item.second.first;
						return item.first;
					}
				}
			}
		}
		return Hyperspace::SessionPtr();
	}

	void Context::registerSession( Hyperspace::SessionPtr session, Hypertable::ConnectionManagerPtr connMgr ) {
		if( session ) {
			std::pair<sessions_t::iterator, bool> r = sessions.insert( sessions_t::value_type(session, std::make_pair(connMgr, 1)) );
			if( !r.second ) {
				++(*r.first).second.second;
			}
		}
	}

	void Context::unregisterSession( Hyperspace::SessionPtr session ) {
		if( session ) {
			sessions_t::iterator it = sessions.find( session );
			if( it != sessions.end() ) {
				if( --(*it).second.second == 0 ) {
					(*it).second.first->remove_all();
					sessions.erase( session );
				}
			}
		}
	}

#endif

	Hypertable::PropertiesPtr Context::initializeProperties( int argc, char *argv[], const Common::Properties& initialProperties, const char* loggingLevel ) {
		std::lock_guard<std::recursive_mutex> lock( Config::rec_mutex );
		PropertiesPtr existingProperties = Config::properties;
		Config::cleanup();
		Policies::initialProperties = Context::convertProperties( initialProperties );
		Policies::prefferedLoggingLevel = loggingLevel && *loggingLevel ? loggingLevel : "notice";
		init_with_policy<Policies>( argc, argv, 0 );
		Logging::init();
		Hypertable::PropertiesPtr properties = Config::properties;
		if( existingProperties ) {
			Config::properties = existingProperties;
		}
		HT_INFO_OUT << "Configuration properties initialized" << HT_END;
		return properties;
	}

	Hypertable::PropertiesPtr Context::convertProperties( const Common::Properties& _properties ) {
		Hypertable::PropertiesPtr properties = std::make_shared<Hypertable::Properties>();
		std::vector<std::string> names;
		_properties.names( names );
		for each( const std::string& name in names ) {
			boost::any any;
			if( getPropValue(properties, name, any) && any.type() != _properties.type(name) ) {
				try {
					properties->set( name, Common::Properties::convert(_properties.type(name), any.type(), _properties.get(name)) );
				}
				catch( std::bad_cast& ) {
					std::stringstream ss;
					ss << "Invalid property value for '" << name << "'\n\tat " << __FUNCTION__ << " (" << __FILE__ << ':' << __LINE__ << ')';
#if _MSC_VER < 1900
						throw std::bad_cast(ss.str().c_str());
#else
					throw std::bad_cast::__construct_from_string_literal( ss.str().c_str() );
#endif
				}
			}
			else {
				properties->set( name, _properties.get(name) );
			}
		}
		return properties;
	}

	char** Context::commandLineToArgv( const char* commandLine, int& argc ) {
		std::string _commandLine;
		if( commandLine && *commandLine ) {
			_commandLine = commandLine;
			boost::trim( _commandLine );
			replace_all_recursive( _commandLine, " ;", ";" );
			replace_all_recursive( _commandLine, "; ", ";" );
			replace_all_recursive( _commandLine, ";;", ";" );
			boost::trim_if( _commandLine, boost::is_any_of(";") );
			boost::replace_all( _commandLine, ";", ";--" );
			boost::trim( _commandLine );
			if( !_commandLine.empty() && _commandLine.front() != '-' && _commandLine.front() != ';' ) {
				_commandLine = "--" + _commandLine;
			}
			commandLine = _commandLine.c_str();
		}

		char filename[MAX_PATH];
		uint32_t lenfn = ::GetModuleFileNameA( 0, filename, sizeof(filename) );

		uint32_t len = (commandLine ? strlen( commandLine ) : 0) + lenfn + 1;
		uint32_t i = ((len+2)/2)*sizeof(PVOID) + sizeof(PVOID);

		char** argv = (char**)malloc( i + (len+2)*sizeof(char) );
		char* _argv = (char*)(((uint8_t*)argv)+i);

		argc = 0;
		argv[argc] = _argv;

		strcpy_s( _argv, (len+2)*sizeof(char), filename );
		int j = lenfn + 1;
		argv[++argc] = _argv+j;

		if( commandLine ) {
			bool inQM = false;
			bool inText = false;
			bool inSpace = true;
			i = 0;

			char a;
			while( (a = commandLine[i]) != '\0' ) {
				if( inQM ) {
					if( a == '\"' ) {
						inQM = false;
					}
					else if( a == ';' ) {
						_argv[j++] = a;
						i += 2; // skip '--'
					}
					else {
						_argv[j++] = a;
					}
				} 
				else {
					switch( a ) {
					case '\"':
						inQM = inText = true;
						if( inSpace ) {
							argv[argc++] = _argv+j;
						}
						inSpace = false;
						break;
					case ' ':
					case ';':
					case '\t':
					case '\n':
					case '\r':
						if( inText ) {
							_argv[j++] = '\0';
						}
						inText = false;
						inSpace = true;
						break;
					default:
						inText = true;
						if( inSpace ) {
							argv[argc++] = _argv+j;
						}
						_argv[j++] = a;
						inSpace = false;
						break;
					}
				}
				++i;
			}
		}

		_argv[j] = '\0';
		argv[argc] = 0;

		return argv;
	}

	bool Context::getPropValue( PropertiesPtr properties, const std::string& name, boost::any& value ) {
		HT4C_TRY {
			if( properties && properties->has(name) ) {
				value = (*properties)[name];
				return true;
			}
			return false;
		}
		HT4C_RETHROW
	}

	Common::ContextKind Context::getContextKind( Hypertable::PropertiesPtr properties ) {
		Common::ContextKind contextKind = Common::CK_Unknown;
		std::string providerName = properties->get_str( Common::Config::ProviderName );

		if( false ) {
		}

#ifdef SUPPORT_HYPERTABLE

		else if( providerName == Common::Config::ProviderHyper ) {
			contextKind = Common::CK_Hyper;
		}

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

		else if( providerName == Common::Config::ProviderThrift ) {
			contextKind = Common::CK_Thrift;
		}

#endif

#ifdef SUPPORT_HAMSTERDB

		else if( providerName == Common::Config::ProviderHamster ) {
			contextKind = Common::CK_Hamster;
		}

#endif

#ifdef SUPPORT_SQLITEDB

		else if( providerName == Common::Config::ProviderSQLite ) {
			contextKind = Common::CK_SQLite;
		}

#endif

#ifdef SUPPORT_ODBC

		else if( providerName == Common::Config::ProviderOdbc ) {
			contextKind = Common::CK_ODBC;
		}

#endif

		return contextKind;
	}

}
