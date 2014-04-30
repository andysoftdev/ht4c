/** -*- C++ -*-
 * Copyright (C) 2010-2014 Thalmann Software & Consulting, http://www.softdev.ch
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
#include "OdbcEnv.h"
#include "OdbcStm.h"
#include "OdbcFactory.h"
#include "OdbcClient.h"
#include "OdbcException.h"

namespace ht4c { namespace Odbc {
	using namespace Db;
	using namespace Db::Util;

	namespace {

		static std::set<OdbcEnv*> environments;
		static std::mutex mtxEnvironments;

	}

	void on_thread_exit() {
		std::lock_guard<std::mutex> lock( mtxEnvironments );
		for each( OdbcEnv* env in environments ) {
			env->onThreadExit();
		}
	}

	OdbcEnv::OdbcEnv( const std::string& _connectionString, const OdbcEnvConfig& config )
	: connectionString( _connectionString )
	, indexColumn( config.indexColumn )
	, indexColumnFamily( config.indexColumnFamily )
	, indexColumnQualifier( config.indexColumnQualifier )
	, indexTimestamp( config.indexTimestamp )
	{
		::InitializeCriticalSection( &mtxEnv );

		odbc::otl_connect::otl_initialize();
		odbc::otl_connect* db;

		HT4C_TRY {
			try {
				db = getDb();

				odbc::otl_cursor::direct_exec( *db, OdbcStm::sysDbCreate().c_str(), odbc::otl_exception::enabled );

				db->commit();

				{
					std::lock_guard<std::mutex> lock( mtxEnvironments );
					environments.insert( this );
				}
			}
			catch( odbc::otl_exception& e ) {
				// table already esists
				if( e.code != 2714 && strcmp(reinterpret_cast<const char*>(e.sqlstate), "42S01") != 0) {
					throw;
				}
			}
		}
		HT4C_ODBC_RETHROW
	}

	OdbcEnv::~OdbcEnv( ) {
			{
				std::lock_guard<std::mutex> lock( mtxEnvironments );
				environments.erase( this );
			}

		HT4C_TRY {
			{
				for( tables_t::iterator it = tables.begin(); it != tables.end(); ++it ) {
					for each( Db::Table* table in (*it).second ) {
						table->dispose();
					}
					(*it).second.clear();
				}
				tables.clear();
			}

			odbc::otl_connect* db;
			{
				for( connections_t::iterator it = connections.begin(); it != connections.end(); ++it ) {
					db = (*it).second;
					if( db ) {
						db->logoff();
						delete db;
					}
				}
				connections.clear();

				for( released_connections_t::iterator it = releasedConnections.begin(); it != releasedConnections.end(); ++it ) {
					db = (*it);
					if( db ) {
						db->logoff();
						delete db;
					}
				}
				releasedConnections.clear();
			}

			::DeleteCriticalSection( &mtxEnv );
		}
		HT4C_ODBC_RETHROW
	}

	Common::Client* OdbcEnv::createClient( ) {
		HT4C_TRY {
			return OdbcClient::create( new Db::Client(this) );
		}
		HT4C_ODBC_RETHROW
	}

	odbc::otl_connect* OdbcEnv::getDb() {
		odbc::otl_connect* db;
		{
			DWORD threadId = ::GetCurrentThreadId();
			OdbcEnvLock lock( this );
			connections_t::iterator it = connections.find( threadId );
			if( it != connections.end() ) {
				return (*it).second;
			}

			if( releasedConnections.size() ) {
				db = releasedConnections.back();
				releasedConnections.pop_back();
				connections.insert( std::make_pair(threadId, db) );
				return db;
			}

			db = new odbc::otl_connect();
			connections.insert( std::make_pair(threadId, db) );
		}
		setupConnection( db );
		return db;
	}

	void OdbcEnv::onThreadExit() {
		odbc::otl_connect* db;

		{
			DWORD threadId = ::GetCurrentThreadId();
			OdbcEnvLock lock( this );
			connections_t::iterator it = connections.find( threadId );
			if( it == connections.end() ) {
				return;
			}

			db = (*it).second;
			connections.erase( it );

			if( connections.size() + releasedConnections.size() < MaxConnections ) {
				releasedConnections.push_back( db );
				return;
			}
		}

		db->logoff();
		delete db;
	}

	void OdbcEnv::sysDbInsert( odbc::otl_connect* db, const char* name, int len, const void* value, int size, std::string* id ) {
		boost::uuids::random_generator gen;
		boost::uuids::uuid u = gen();

		std::string pk( "t" );
		pk.reserve(36);

		for( boost::uuids::uuid::const_iterator it = u.begin(); it != u.end(); ++it ) {
				pk += boost::uuids::detail::to_char(((*it) >> 4) & 0x0F);
				pk += boost::uuids::detail::to_char( (*it) & 0x0F );
		}

		odbc::otl_stream os( 1, OdbcStm::sysDbInsert().c_str(), *db );
		os << varbinary(pk)
			 << varbinary(name, len)
			 << varbinary(value, size);

		if( id ) {
			*id = pk;
		}
	}

	void OdbcEnv::sysDbUpdateKey( odbc::otl_connect* db, const std::string& id, const char* key, int len ) {
		odbc::otl_stream os( 1, OdbcStm::sysDbUpdateKey().c_str(), *db );
		os << varbinary(key, len) << varbinary(id);
	}

	void OdbcEnv::sysDbUpdateValue( odbc::otl_connect* db, const std::string& id, const void* value, int size ) {
		odbc::otl_stream os( 1, OdbcStm::sysDbUpdateValue().c_str(), *db );
		os << varbinary(value, size) << varbinary(id);
	}

	bool OdbcEnv::sysDbRead( odbc::otl_connect* db, const char* name, int len, Hypertable::DynamicBuffer& buf, std::string* id ) {
		buf.clear();

		varbinary v;
		if( id ) {
			odbc::otl_stream os( 1, OdbcStm::sysDbReadKeyAndValue().c_str(), *db );
			os << varbinary(name, len);

			if( os.eof() ) {
				return false;
			}

			varbinary pk;
			os >> pk >> v;
			*id = pk.c_str();
		}
		else {
			odbc::otl_stream os( 1, OdbcStm::sysDbReadValue().c_str(), *db );
			os << varbinary(name, len);

			if( os.eof() ) {
				return false;
			}

			os >> v;
		}

		if( v.len() ) {
			buf.add( v.v, v.len() );
		}

		return true;
	}

	bool OdbcEnv::sysDbExists( odbc::otl_connect* db, const char* name, int len, std::string* id ) {
		odbc::otl_stream os( 1, OdbcStm::sysDbExists().c_str(), *db );
		os << varbinary(name, len);

		if( os.eof() ) {
			return false;
		}

		if( id ) {
			varbinary pk;
			os >> pk;
			*id = pk.c_str();
		}

		return true;
	}

	bool OdbcEnv::sysDbDelete( odbc::otl_connect* db, const char* name, int len, std::string* id ) {
		if( !sysDbExists(db, name, len, id) ) {
			return false;
		}

		odbc::otl_stream os( 1, OdbcStm::sysDbDelete().c_str(), *db );
		os << varbinary(name, len);

		return true;
	}

	void OdbcEnv::sysDbCreateTable( odbc::otl_connect* db, const char* name, int len, const void* value, int size, std::string& id ) {
		sysDbInsert( db, name, len, value, size, &id );

		OdbcStm stm( id );
		odbc::otl_cursor::direct_exec( *db, stm.createTable().c_str(), odbc::otl_exception::enabled );

		if( indexColumn ) {
			odbc::otl_cursor::direct_exec(
				*db
			 , Hypertable::format("CREATE INDEX i_cfcq ON %s (cf, cq);", id.c_str()).c_str()
			 , odbc::otl_exception::enabled );
		}

		if( indexColumnFamily ) {
			odbc::otl_cursor::direct_exec(
				*db
			 , Hypertable::format("CREATE INDEX i_cf ON %s (cf);", id.c_str()).c_str()
			 , odbc::otl_exception::enabled );
		}

		if( indexColumnQualifier ) {
			odbc::otl_cursor::direct_exec(
				*db
			 , Hypertable::format("CREATE INDEX i_cq ON %s (cq);", id.c_str()).c_str()
			 , odbc::otl_exception::enabled );
		}

		if( indexTimestamp ) {
			odbc::otl_cursor::direct_exec(
				*db
			 , Hypertable::format("CREATE INDEX i_ts ON %s (ts);", id.c_str()).c_str()
			 , odbc::otl_exception::enabled );
		}
	}

	bool OdbcEnv::sysDbOpenTable( odbc::otl_connect* db, Db::Table* table, std::string& id ) {
		const char* key;
		int len = table->toKey( key );
		Hypertable::DynamicBuffer buf;
		if( sysDbRead(db, key, len, buf, &id) ) {
			table->fromRecord( buf );
			OdbcEnvLock lock( this );
			tables[id].insert( table );
			return true;
		}
		return false;
	}

	void OdbcEnv::sysDbDisposeTable( const std::string& id ) {
		OdbcEnvLock lock( this );
		tables.erase( id );
	}

	bool OdbcEnv::sysDbDeleteTable( odbc::otl_connect* db, const char* name, int len ) {
		std::string id;
		if( sysDbDelete(db, name, len, &id) ) {
			std::set<Db::Table*> t;
			{
				OdbcEnvLock lock( this );
				tables_t::iterator it = tables.find( id );
				if( it != tables.end() ) {
					t = (*it).second; // shallow copy, table->dispose() might call sysDbDisposeTable( id )
					tables.erase( id );
				}
			}

			for each( Db::Table* table in t ) {
				table->dispose();
			}

			OdbcStm stm( id );
			odbc::otl_cursor::direct_exec( *db, stm.deleteTable().c_str() );

			return true;
		}
		return false;
	}

	void OdbcEnv::setupConnection( odbc::otl_connect* db ) {
		db->set_connection_mode( odbc::OTL_MSSQL_2008_ODBC_CONNECT );
		db->set_stream_pool_size( 32 ); 
		////TODO db->set_fetch_scroll_mode( true );
		db->set_max_long_size( 64 * 1024 ); //TODO config param
		db->rlogon( connectionString.c_str() );
		db->auto_commit_off();
	}

} }
