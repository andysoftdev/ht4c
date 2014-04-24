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

#pragma once

#ifdef __cplusplus_cli
#error compile native
#endif

#include <mutex>
#include <map>

namespace odbc {

	class otl_connect;

}

namespace ht4c { namespace Common {

	class Client;

} }

namespace ht4c { namespace Odbc {

	namespace Db {

		class Table;

	}

	struct OdbcEnvConfig;

	/// <summary>
	/// Represents the Hypertable Odbc environment.
	/// </summary>
	class OdbcEnv : public Hypertable::ReferenceCount {

	public:

			OdbcEnv( const std::string& connectionString, const OdbcEnvConfig& config );
			virtual ~OdbcEnv( );

			Common::Client* createClient( );
			odbc::otl_connect* getDb();

			void onThreadExit();

			static void sysDbInsert( odbc::otl_connect* db, const char* name, int len, const void* value, int size, std::string* id = 0 );
			static void sysDbUpdateKey( odbc::otl_connect* db, const std::string& id, const char* key, int len );
			static void sysDbUpdateValue( odbc::otl_connect* db, const std::string& id, const void* value, int size );
			static bool sysDbRead( odbc::otl_connect* db, const char* name, int len, Hypertable::DynamicBuffer& buf, std::string* id = 0 );
			static bool sysDbExists( odbc::otl_connect* db, const char* name, int len, std::string* id = 0 );
			static bool sysDbDelete( odbc::otl_connect* db, const char* name, int len, std::string* id = 0 );

			void sysDbCreateTable( odbc::otl_connect* db, const char* name, int len, const void* value, int size, std::string& id );
			bool sysDbOpenTable( odbc::otl_connect* db, Db::Table* table, std::string& id );
			void sysDbDisposeTable( const std::string& id );
			bool sysDbDeleteTable( odbc::otl_connect* db, const char* name, int len );

			class Lock {

				public:

					inline Lock( OdbcEnv* _env )
					: env( _env ) {
						env->lock();
					}
					inline ~Lock( ) {
						env->unlock();
					}

				private:

					OdbcEnv* env;
			};
			friend class Lock;

		private:

			inline void lock( ) {
				::EnterCriticalSection( &mtxEnv );
			}
			inline void unlock( ) {
				::LeaveCriticalSection( &mtxEnv );
			}

			enum {
				MaxConnections = 8
			};

			void setupConnection( odbc::otl_connect* db );

			std::string connectionString;
			bool indexColumn;
			bool indexColumnFamily;
			bool indexColumnQualifier;
			bool indexTimestamp;

			typedef std::map<DWORD, odbc::otl_connect*> connections_t;
			connections_t connections;

			typedef std::vector<odbc::otl_connect*> released_connections_t;
			released_connections_t releasedConnections;

			typedef std::map<const std::string, std::set<Db::Table*>> tables_t;
			tables_t tables;

			CRITICAL_SECTION mtxEnv;
	};
	typedef boost::intrusive_ptr<OdbcEnv> OdbcEnvPtr;

	typedef OdbcEnv::Lock OdbcEnvLock;

} }
