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

#include "Context.h"
#include "Logging.h"

#include "ht4c.Common/Config.h"
#include "ht4c.Common/Properties.h"
#include "ht4c.Common/Exception.h"
#include "ht4c.Hyper/HyperClient.h"
#include "ht4c.Thrift/ThriftFactory.h"
#include "ht4c.Thrift/ThriftClient.h"

#pragma warning( push, 3 )

#include "Common/Init.h"
#include "DfsBroker/Lib/Config.h"
#include "Hyperspace/Config.h"
#include "Hypertable/Lib/Config.h"
#include "ThriftBroker/Config.h"

#pragma warning( pop )

namespace ht4c {

	using namespace Hypertable;
	using namespace Hypertable::Config;

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

		const uint16_t defaultHyperspacePort					= 38040;
		const uint16_t defaultThriftBrokerPort				= 38080;

		const int32_t defaultThriftBrokerTimeoutMsec	= 30000;
		const int32_t defaultLeaseIntervalMsec				= 300000;
		const int32_t defaultGracePeriodMsec					= 300000;

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
			char expanded[2048];
			if( ExpandEnvironmentStringsA(str.c_str(), expanded, sizeof(expanded)) ) {
				if( str != expanded ) {
					str = expanded;
					return true;
				}
			}
			return false;
		}

		typedef Meta::list<
			DfsClientPolicy
		, HyperspaceClientPolicy
		, MasterClientPolicy
		, RangeServerClientPolicy
		, ThriftClientPolicy
		, DefaultCommPolicy
		> PolicyList;

		typedef Join<PolicyList>::type JoinedPolicyList;

		struct Policies : JoinedPolicyList {

			static PropertiesPtr initialProperties;
			static std::string prefferedLoggingLevel;

			static void init_options( ) {
				Config::allow_unregistered_options( true );

				cmdline_desc().add_options()
					(Common::Config::ProviderNameAlias, str()->default_value(Common::Config::ProviderHyper), "Provider name (default: Hyper)\n")
					(Common::Config::UriAlias, str()->default_value(localhost), "Uri hostname[:port] (default: net.tcp://localhost)\n");

				alias( Common::Config::ProviderNameAlias, Common::Config::ProviderName );
				alias( Common::Config::UriAlias, Common::Config::Uri );

				file_desc().add_options()
					(Common::Config::ProviderName, str()->default_value(Common::Config::ProviderHyper), "Provider name (default: Hyper)\n")
					(Common::Config::Uri, str()->default_value(localhost), "Uri hostname[:port] (default: net.tcp://localhost)\n");

#ifdef SUPPORT_HAMSTERDB

				cmdline_desc().add_options()
					(Common::Config::HamsterFilenameAlias, str(), "Hamster db filename\n")
					(Common::Config::HamsterEnableRecoveryAlias, boo()->default_value(false), "Enable or disable hamster db recovery (default: false)\n")
					(Common::Config::HamsterEnableAutoRecoveryAlias, boo()->default_value(false), "Enable or disable hamster db auto-recovery (default: false)\n")
					(Common::Config::HamsterMaxTablesAlias, i32()->default_value(1024), "Hamster db table limit (default:1024)\n")
					(Common::Config::HamsterCacheSizeMBAlias, i32()->default_value(64), "Hamster db cache size [MB] (default:64)\n")
					(Common::Config::HamsterPageSizeKBAlias, i32()->default_value(64), "Hamster db page size [KB] (default:64)\n");

				alias( Common::Config::HamsterFilenameAlias, Common::Config::HamsterFilename );
				alias( Common::Config::HamsterEnableRecoveryAlias, Common::Config::HamsterEnableRecovery );
				alias( Common::Config::HamsterEnableAutoRecoveryAlias, Common::Config::HamsterEnableAutoRecovery );
				alias( Common::Config::HamsterMaxTablesAlias, Common::Config::HamsterMaxTables );
				alias( Common::Config::HamsterCacheSizeMBAlias, Common::Config::HamsterCacheSizeMB );
				alias( Common::Config::HamsterPageSizeKBAlias, Common::Config::HamsterPageSizeKB );

				file_desc().add_options()
					(Common::Config::HamsterFilename, str(), "Hamster db filename\n")
					(Common::Config::HamsterEnableRecovery, boo()->default_value(false), "Enable or disable hamster db recovery (default: false)\n")
					(Common::Config::HamsterEnableAutoRecovery, boo()->default_value(false), "Enable or disable hamster db auto-recovery (default: false)\n")
					(Common::Config::HamsterMaxTables, i32()->default_value(1024), "Hamster db table limit (default:1024)\n")
					(Common::Config::HamsterCacheSizeMB, i32()->default_value(64), "Hamster db cache size [MB] (default:64)\n")
					(Common::Config::HamsterPageSizeKB, i32()->default_value(64), "Hamster db page size [KB] (default:64)\n");

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

				int32_t timeoutMsec = defaultThriftBrokerTimeoutMsec;
				if( properties->has(thriftBrokerTimeout) ) {
					timeoutMsec = properties->get_i32( thriftBrokerTimeout );
				}
				else {
					properties->set( thriftBrokerTimeout, timeoutMsec );
				}

				properties->sync_aliases();

				std::string providerName = properties->get_str( Common::Config::ProviderName );
				if( providerName == Common::Config::ProviderHyper ) {
					if( !properties->defaulted(Common::Config::Uri) || properties->defaulted(hyperspace) ) {
						properties->set( hyperspace, get_uri(hyperspacePort, defaultHyperspacePort) );
					}
				}
				else if( providerName == Common::Config::ProviderThrift ) {
					if( !properties->defaulted(Common::Config::Uri) || properties->defaulted(thriftBroker) ) {
						properties->set( thriftBroker, get_uri(thriftBrokerPort, defaultThriftBrokerPort) );
					}
				}

#ifdef SUPPORT_HAMSTERDB

				else if( providerName == Common::Config::ProviderHamster ) {
					if( !properties->defaulted(Common::Config::Uri) || properties->defaulted(Common::Config::HamsterFilenameAlias) ) {
						std::string filename = get_uri();
						replace_all_recursive( filename, "//", "/" );
						if( filename.find("file:") != 0 ) {
							HT_THROW( Error::CONFIG_BAD_VALUE, "Invalid uri scheme, file://[drive][/path/]filename required" );
						}
						filename = filename.substr( 5 );
						boost::trim_left_if( filename, boost::is_any_of("/") );
						if( filename.empty() ) {
							HT_THROW( Error::CONFIG_BAD_VALUE, "Empty filename, file://[drive][/path/]filename required" );
						}
						replace_all_recursive( filename, "/", "\\" );
						properties->set( Common::Config::HamsterFilenameAlias, filename );
					}
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

			static std::string get_uri( const char* portProperty, uint16_t defaultPort ) {
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

		};

		PropertiesPtr Policies::initialProperties;
		std::string Policies::prefferedLoggingLevel;

	}

	Hypertable::RecMutex Context::mutex;
	Context::sessions_t Context::sessions;

#ifdef SUPPORT_HAMSTERDB

	Context::hamster_envs_t Context::hamsterEnvs;

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
			size_t remainingSessions;
			{
				ScopedRecLock lock( mutex );

#ifdef SUPPORT_HAMSTERDB

				hamsterEnvs.clear();

#endif

				remainingSessions = sessions.size();
			}
			if( !remainingSessions ) {
				Comm::destroy();
				Config::cleanup();
			}
			Logging::shutdown();
		}
		HT4C_RETHROW
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
			ScopedRecLock lock( mutex );
			switch( contextKind ) {
				case Common::CK_Hyper:
					return ht4c::Hyper::HyperClient::create( getConnectionManager(), getHyperspaceSession(), properties );
				case Common::CK_Thrift:
					return ht4c::Thrift::ThriftClient::create( getThriftClient() );

#ifdef SUPPORT_HAMSTERDB

				case Common::CK_Hamster:
					return getHamsterEnv()->createClient( );

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
			return contextKind == Common::CK_Hyper || contextKind == Common::CK_Thrift;
		default:
			break;
		}
		return false;
	}

	Context::~Context( ) {

#ifdef SUPPORT_HAMSTERDB

		if( hamsterEnv ) {
			ScopedRecLock lock( mutex );
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

		thriftClient = 0;
		if( session ) {
			unregisterSession( session );
		}
		session = 0;
		properties = 0;
	}

	ConnectionManagerPtr Context::getConnectionManager( ) {
		HT4C_TRY {
			if( !connMgr ) {
				connMgr = new ConnectionManager( getComm() );
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
					session = new Hyperspace::Session( getComm(), properties );
				}
				registerSession( session, getConnectionManager() );
			}
			return session;
		}
		HT4C_RETHROW
	}

	Hyperspace::Comm* Context::getComm( ) {
		HT4C_TRY {
			return Comm::instance();
		}
		HT4C_RETHROW
	}

	Hypertable::Thrift::ClientPtr Context::getThriftClient( ) {
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

	Hamster::HamsterEnvPtr Context::getHamsterEnv( ) {
		if( !hamsterEnv ) {
			ScopedRecLock lock( mutex );
			std::string filename = properties->get_str( Common::Config::HamsterFilenameAlias );
			hamster_envs_t::iterator it = hamsterEnvs.find( filename );
			if( it == hamsterEnvs.end() ) {
				Hamster::HamsterEnvConfig config;
				config.enableRecovery = properties->get_bool( Common::Config::HamsterEnableRecoveryAlias );
				config.enableAutoRecovery = properties->get_bool( Common::Config::HamsterEnableAutoRecoveryAlias );
				config.maxTables = properties->get_i32( Common::Config::HamsterMaxTablesAlias );
				config.cacheSizeMB = properties->get_i32( Common::Config::HamsterCacheSizeMBAlias );
				config.pageSizeKB = properties->get_i32( Common::Config::HamsterPageSizeKBAlias );

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

	Context::Context( Common::ContextKind _contextKind, Hypertable::PropertiesPtr _properties )
	: contextKind( _contextKind )
	, properties( _properties )
	{
	}

	Hyperspace::SessionPtr Context::findSession( Hypertable::PropertiesPtr properties, Hypertable::ConnectionManagerPtr& connMgr ) {
		connMgr = 0;
		ScopedRecLock lock( mutex );
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
			ScopedRecLock lock( mutex );
			std::pair<sessions_t::iterator, bool> r = sessions.insert( sessions_t::value_type(session, std::make_pair(connMgr, 1)) );
			if( !r.second ) {
				++(*r.first).second.second;
			}
		}
	}

	void Context::unregisterSession( Hyperspace::SessionPtr session ) {
		if( session ) {
			ScopedRecLock lock( mutex );
			sessions_t::iterator it = sessions.find( session );
			if( it != sessions.end() ) {
				if( --(*it).second.second == 0 ) {
					(*it).second.first->remove_all();
					sessions.erase( session );
				}
			}
		}
	}

	Hypertable::PropertiesPtr Context::initializeProperties( int argc, char *argv[], const Common::Properties& initialProperties, const char* loggingLevel ) {
		ScopedRecLock lock( Config::rec_mutex );
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
		Hypertable::PropertiesPtr properties = new Hypertable::Properties();
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
					throw std::bad_cast( ss.str().c_str() );
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
			boost::replace_all( _commandLine, ";", " --" );
			boost::trim( _commandLine );
			if( !_commandLine.empty() && _commandLine.front() != '-' ) {
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
		if( providerName == Common::Config::ProviderHyper ) {
			contextKind = Common::CK_Hyper;
		}
		else if( providerName == Common::Config::ProviderThrift ) {
			contextKind = Common::CK_Thrift;
		}

#ifdef SUPPORT_HAMSTERDB

		else if( providerName == Common::Config::ProviderHamster ) {
			contextKind = Common::CK_Hamster;
		}

#endif

		return contextKind;
	}

}
