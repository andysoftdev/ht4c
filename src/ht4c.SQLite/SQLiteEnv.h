/** -*- C++ -*-
 * Copyright (C) 2010-2015 Thalmann Software & Consulting, http://www.softdev.ch
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

struct sqlite3;
struct sqlite3_stmt;

namespace ht4c { namespace Common {

	class Client;

} }

namespace ht4c { namespace SQLite {

	namespace Db {

		class Table;

	}

	struct SQLiteEnvConfig;

	/// <summary>
	/// Represents the Hypertable sqlite environment.
	/// </summary>
	class SQLiteEnv : public Hypertable::ReferenceCount {

	public:

			SQLiteEnv( const std::string& filename, const SQLiteEnvConfig& config );
			virtual ~SQLiteEnv( );

			Common::Client* createClient( );
			inline sqlite3* getDb( ) const {
				return db;
			}

			void txBegin();
			void txCommit();
			void txRollback();

			void sysDbInsert( const char* name, int len, const void* value, int size, int64_t* rowid = 0 );
			void sysDbUpdateKey( int64_t rowid, const char* key, int len );
			void sysDbUpdateValue( int64_t rowid, const void* value, int size );
			bool sysDbRead( const char* name, int len, Hypertable::DynamicBuffer& buf, int64_t* rowid = 0 );
			bool sysDbExists( const char* name, int len, int64_t* rowid = 0 );
			bool sysDbDelete( const char* name, int len, int64_t* rowid = 0 );

			void sysDbCreateTable( const char* name, int len, const void* value, int size, int64_t& id );
			bool sysDbOpenTable( Db::Table* table, int64_t& id );
			bool sysDbRefreshTable( Db::Table* table );
			void sysDbRefreshTable( int64_t id );
			void sysDbDisposeTable( int64_t id );
			bool sysDbDeleteTable( const char* name, int len );

			class Lock {

				public:

					inline Lock( SQLiteEnv* _env )
					: env( _env ) {
						env->lock();
					}
					inline ~Lock( ) {
						env->unlock();
					}

				private:

					SQLiteEnv* env;
			};
			friend class Lock;

		private:

			inline void lock( ) {
				::EnterCriticalSection( &cs );
			}
			inline void unlock( ) {
				::LeaveCriticalSection( &cs );
			}

			sqlite3* db;
			bool tx;

			bool indexColumn;
			bool indexColumnFamily;
			bool indexColumnQualifier;
			bool indexTimestamp;

			sqlite3_stmt* stmtBegin;
			sqlite3_stmt* stmtCommit;
			sqlite3_stmt* stmtRollback;
			sqlite3_stmt* stmtInsert;
			sqlite3_stmt* stmtUpdateKey;
			sqlite3_stmt* stmtUpdateValue;
			sqlite3_stmt* stmtFind;
			sqlite3_stmt* stmtRead;
			sqlite3_stmt* stmtDelete;

			typedef std::unordered_map<int64_t, std::set<Db::Table*>> tables_t;
			tables_t tables;

			CRITICAL_SECTION cs;
	};
	typedef boost::intrusive_ptr<SQLiteEnv> SQLiteEnvPtr;

	typedef SQLiteEnv::Lock SQLiteEnvLock;

} }
