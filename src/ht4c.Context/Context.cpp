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

#include "Context.h"
#include "Logging.h"

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
		static void on_init_error(Exception &e) {
			HT_ERROR_OUT << e << HT_END;
			throw e;
		}
	};

	Hypertable::RecMutex Context::mutex;
	Context::sessions_t Context::sessions;

	Context* Context::create( Common::ContextKind ctxKind, const char* host, uint16_t port, const Common::Properties& _prop ) {
		HT4C_TRY {
			Hypertable::PropertiesPtr prop = getProperties( 0, 0 );
			std::vector<std::string> names;
			_prop.names( names );
			for each( const std::string& name in names ) {
				boost::any any;
				if( getPropValue(prop, name, any) && any.type() != _prop.type(name) ) {
					try {
						prop->set( name, Common::Properties::convert(_prop.type(name), any.type(), _prop.get(name)), false );
					}
					catch( std::bad_cast& ) {
						throw std::bad_cast( format("Invalid property value for '%s'", name.c_str()).c_str() );
					}
				}
				else {
					prop->set( name, _prop.get(name), false );
				}
			}

			if( host && *host ) {
				if( ctxKind == Common::CK_Hyper ) {
					prop->set( "hyperspace", format("%s:%d", host, port ? port : (uint16_t)38040), false );
				}
				else if( ctxKind == Common::CK_Thrift ) {
					prop->set( "thrift-broker", format("%s:%d", host, port ? port : (uint16_t)38080), false );
				}
			}
			return new Context( ctxKind, prop );
		}
		HT4C_RETHROW
	}

	Context* Context::create( Common::ContextKind ctxKind, const char* commandLine, bool includesModuleFileName ) {
		HT4C_TRY {
			Hypertable::PropertiesPtr prop = getProperties( commandLine, includesModuleFileName );
			return new Context( ctxKind, prop );
		}
		HT4C_RETHROW
	}

	void Context::shutdown( ) {
		HT4C_TRY {
			size_t remaining_sessions;
			{
				ScopedRecLock lock( mutex );
				remaining_sessions = sessions.size();
			}
			if( !remaining_sessions ) {
				Comm::destroy();
				Config::cleanup();
			}
			Logging::shutdown();
		}
		HT4C_RETHROW
	}

	Common::Client* Context::createClient( ) {
		switch( getContextKind() ) {
			case Common::CK_Hyper:
				return ht4c::Hyper::HyperClient::create( getConnectionManager(), getHyperspaceSession(), getProperties() );
			case Common::CK_Thrift:
				return ht4c::Thrift::ThriftClient::create( getThriftClient() );
		}
		return 0;
	}

	Common::ContextKind Context::getContextKind( ) const {
		return ctxKind;
	}

	void Context::getProperties( Common::Properties& _prop ) const {
		_prop.clear();
		std::vector<String> names;
		getProperties()->get_names( names );
		for each( const String& name in names ) {
			_prop.insert( name, (*getProperties())[name] );
		}
	}

	Context::~Context( ) {
		thriftClient = 0;
		if( session ) {
			unregisterSession( session );
		}
		session = 0;
		prop = 0;
	}

	PropertiesPtr Context::getProperties() const {
		return prop;
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
				session = findSession( prop, connMgr );
				if( !session ) {
					session = new Hyperspace::Session( getComm(), prop );
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
				std::string host = prop->get_str("thrift-host");
				uint16_t port = prop->get_i16("thrift-port");
				int32_t timeoutMsec = 30000;
				if (prop->has("thrift-timeout")) {
					timeoutMsec = prop->get_i32("thrift-timeout");
				}
				else {
					 prop->set("thrift-timeout", timeoutMsec);
				}
				thriftClient = ht4c::Thrift::ThriftFactory::create( host, port, timeoutMsec );
			}
			return thriftClient;
		}
		HT4C_RETHROW
	}

	Context::Context( Common::ContextKind _ctxKind, Hypertable::PropertiesPtr _prop )
	: ctxKind( _ctxKind )
	, prop( _prop )
	{
		ScopedRecLock lock( rec_mutex );
		PropertiesPtr propExisting = Config::properties;
		Config::properties = prop;
		if( prop->defaulted("Hypertable.Verbose") && prop->defaulted("Hypertable.Silent") ) {
			prop->set( "Hypertable.Silent", true );
		}
		if( prop->defaulted("Hypertable.Logging.Level") ) {
			prop->set( "Hypertable.Logging.Level", Hypertable::String("notice") );
			Hypertable::Logger::set_level(Hypertable::Logger::Priority::NOTICE);
		}
		if( prop->defaulted("Hyperspace.Lease.Interval") ) {
			prop->set( "Hyperspace.Lease.Interval", 300000 );
		}
		if( prop->defaulted("Hyperspace.GracePeriod") ) {
			prop->set( "Hyperspace.GracePeriod", 300000 );
		}

		prop->sync_aliases();
		Logging::init();
		JoinedPolicyList::init();
		prop->sync_aliases();
		if( propExisting ) {
			Config::properties = propExisting;
		}
	}

	Hyperspace::SessionPtr Context::findSession( Hypertable::PropertiesPtr prop, Hypertable::ConnectionManagerPtr& connMgr ) {
		connMgr = 0;
		ScopedRecLock lock( mutex );
		if( ReactorRunner::handler_map && sessions.size() ) {
			Hypertable::Strings hosts = prop->get_strs("hs-host");
			if( hosts.size() && hosts.front().size() ) {
				Hypertable::InetAddr addr( hosts.front(), prop->get_i16("hs-port") );
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

	Hypertable::PropertiesPtr Context::getProperties(const char* commandLine, bool includesModuleFileName ) {
		int argc;
		char** argv = Context::commandLineToArgv( commandLine, argc, includesModuleFileName );
		Hypertable::PropertiesPtr prop = getProperties( argc, argv );
		if( argv ) free( argv );
		return prop;
	}

	Hypertable::PropertiesPtr Context::getProperties( int argc, char *argv[] ) {
		ScopedRecLock lock( rec_mutex );
		PropertiesPtr propExisting = Config::properties;
		Config::cleanup();
		init_with_policy<Policies>(argc, argv, 0);
		Hypertable::PropertiesPtr prop = Config::properties;
		if( propExisting ) {
			Config::properties = propExisting;
		}
		return prop;
	}

	char** Context::commandLineToArgv( const char* commandLine, int& argc, bool includesModuleFileName ) {
		char filename[MAX_PATH];
		uint32_t lenfn = 0;
		
		if( !includesModuleFileName ) {
			lenfn = ::GetModuleFileNameA( 0, filename, sizeof(filename) );
		}

		uint32_t len = strlen( commandLine ) + lenfn + 1;
		uint32_t i = ((len+2)/2)*sizeof(PVOID) + sizeof(PVOID);

		char** argv = (char**)malloc( i + (len+2)*sizeof(char) );
		char* _argv = (char*)(((uint8_t*)argv)+i);

		argc = 0;
		argv[argc] = _argv;
		bool inQM = false;
		bool inText = false;
		bool inSpace = true;
		i = 0;

		uint32_t j = 0;
		if( !includesModuleFileName ) {
			strcpy_s( _argv, (len+2)*sizeof(char), filename );
			j += lenfn + 1;
			argv[++argc] = _argv+j;
		}
		
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
		_argv[j] = '\0';
		argv[argc] = 0;

		return argv;
	}

	bool Context::getPropValue( PropertiesPtr prop, const std::string& name, boost::any& value ) {
		HT4C_TRY {
			if( prop && prop->has(name) ) {
				value = (*prop)[name];
				return true;
			}
			return false;
		}
		HT4C_RETHROW
	}

}
