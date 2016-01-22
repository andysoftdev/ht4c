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
#include "SQLiteEnv.h"
#include "SQLiteFactory.h"
#include "SQLiteClient.h"
#include "SQLiteException.h"

namespace ht4c { namespace SQLite {
	using namespace Db;

	SQLiteEnv::SQLiteEnv( const std::string &filename, const SQLiteEnvConfig& config )
	: db( 0 )
	, tx( false )
	, indexColumn( config.indexColumn )
	, indexColumnFamily( config.indexColumnFamily )
	, indexColumnQualifier( config.indexColumnQualifier )
	, indexTimestamp( config.indexTimestamp )
	, stmtBegin( 0 )
	, stmtCommit( 0 )
	, stmtRollback( 0 )
	, stmtInsert( 0 )
	, stmtUpdateKey( 0 )
	, stmtUpdateValue( 0 )
	, stmtFind( 0 )
	, stmtRead( 0 )
	, stmtDelete( 0 )
	{
		::InitializeCriticalSection( &cs );
		HT4C_TRY {
			try {
				int st = sqlite3_open( filename.c_str(), &db );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				char* errmsg = 0;
				st = sqlite3_exec( db
												 , Hypertable::format(
														  "PRAGMA page_size=%d;"
														  "PRAGMA cache_size=%d;"
														  "PRAGMA journal_mode=TRUNCATE;"
														  "PRAGMA synchronous=%s;"
														  "PRAGMA temp_store=MEMORY;"
														  "CREATE TABLE IF NOT EXISTS "
														  "sys_db (id INTEGER PRIMARY KEY AUTOINCREMENT, k TEXT NOT NULL, v BLOB, UNIQUE(k));"
														, std::max(1, std::min(config.pageSizeKB, 64)) * 1024
														, std::max(1, config.cacheSizeMB) * -1024
														, config.synchronous ? "ON" : "OFF").c_str()
												 , 0, 0, &errmsg );

				HT4C_SQLITE_VERIFY( st, db, errmsg );

				st = sqlite3_prepare_v2( db, "BEGIN;", -1, &stmtBegin, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_prepare_v2( db, "COMMIT;", -1, &stmtCommit, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_prepare_v2( db, "ROLLBACK;", -1, &stmtRollback, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_prepare_v2( db, "INSERT INTO sys_db (k, v) VALUES(?, ?);", -1, &stmtInsert, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_prepare_v2( db, "UPDATE sys_db SET k=? WHERE id=?;", -1, &stmtUpdateKey, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_prepare_v2( db, "UPDATE sys_db SET v=? WHERE id=?;", -1, &stmtUpdateValue, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_prepare_v2( db, "SELECT id FROM sys_db WHERE k=?;", -1, &stmtFind, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_prepare_v2( db, "SELECT id, v FROM sys_db WHERE k=?;", -1, &stmtRead, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_prepare_v2( db, "DELETE FROM sys_db WHERE k=?;", -1, &stmtDelete, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );
			}
			catch( ... ) {
				if( db ) {
					sqlite3_close( db );
					db = 0;
				}
				throw;
			}
		}
		HT4C_SQLITE_RETHROW
	}

	SQLiteEnv::~SQLiteEnv( ) {
		::DeleteCriticalSection( &cs );
		HT4C_TRY {
			for( tables_t::iterator it = tables.begin(); it != tables.end(); ++it ) {
				for each( Db::Table* table in (*it).second ) {
					table->dispose();
				}
				(*it).second.clear();
			}
			tables.clear();

			Util::stmt_finalize( db, &stmtBegin );
			Util::stmt_finalize( db, &stmtCommit );
			Util::stmt_finalize( db, &stmtRollback );
			Util::stmt_finalize( db, &stmtInsert );
			Util::stmt_finalize( db, &stmtUpdateKey );
			Util::stmt_finalize( db, &stmtUpdateValue );
			Util::stmt_finalize( db, &stmtFind );
			Util::stmt_finalize( db, &stmtRead );
			Util::stmt_finalize( db, &stmtDelete );

			if( db ) {
				// deletes the journal
				char* errmsg = 0;
				int st = sqlite3_exec( db, "PRAGMA journal_mode=DELETE;BEGIN;COMMIT;", 0, 0, &errmsg );
				HT4C_SQLITE_VERIFY( st, db, errmsg );

				st = sqlite3_close( db );
				HT4C_SQLITE_VERIFY( st, db, 0 );
				db = 0;
			}
		}
		HT4C_SQLITE_RETHROW
	}

	Common::Client* SQLiteEnv::createClient( ) {
		HT4C_TRY {
			return SQLiteClient::create( new Db::Client(this) );
		}
		HT4C_SQLITE_RETHROW
	}

	void SQLiteEnv::txBegin() {
		if( !tx ) {
			int st = sqlite3_step( stmtBegin );
			HT4C_SQLITE_VERIFY( st, db, 0 );
			tx = true;
		}
	}

	void SQLiteEnv::txCommit() {
		if( tx ) {
			int st = sqlite3_step( stmtCommit );
			HT4C_SQLITE_VERIFY( st, db, 0 );
			tx = false;
		}
	}

	void SQLiteEnv::txRollback() {
		if( tx ) {
			int st = sqlite3_step( stmtRollback );
			HT4C_SQLITE_VERIFY( st, db, 0 );
			tx = false;
		}
	}

	void SQLiteEnv::sysDbInsert( const char* name, int len, const void* value, int size, int64_t* rowid ) {
		Util::StmtReset stmt( stmtInsert );

		int st = sqlite3_bind_text( stmtInsert, 1, name, len, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_bind_blob( stmtInsert, 2, value, size, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_step( stmtInsert );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		if( rowid ) {
			*rowid = sqlite3_last_insert_rowid( db );
		}
	}

	void SQLiteEnv::sysDbUpdateKey( int64_t rowid, const char* key, int len ) {
		Util::StmtReset stmt( stmtUpdateKey );

		int st = sqlite3_bind_text( stmtUpdateKey, 1, key, len, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_bind_int64( stmtUpdateKey, 2, rowid );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_step( stmtUpdateKey );
		HT4C_SQLITE_VERIFY( st, db, 0 );
	}

	void SQLiteEnv::sysDbUpdateValue( int64_t rowid, const void* value, int size ) {
		Util::StmtReset stmt( stmtUpdateValue );

		int st = sqlite3_bind_blob( stmtUpdateValue, 1, value, size, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_bind_int64( stmtUpdateValue, 2, rowid );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_step( stmtUpdateValue );
		HT4C_SQLITE_VERIFY( st, db, 0 );
	}

	bool SQLiteEnv::sysDbRead( const char* name, int len, Hypertable::DynamicBuffer& buf, int64_t* rowid ) {
		buf.clear();

		Util::StmtReset stmt( stmtRead );

		int st = sqlite3_bind_text( stmtRead, 1, name, len, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		if( (st = sqlite3_step(stmtRead)) == SQLITE_ROW ) {
			if( rowid ) {
				*rowid = sqlite3_column_int64( stmtRead, 0 );
			}
			if( sqlite3_column_type(stmtRead, 1) != SQLITE_NULL ) {
				const void* v = sqlite3_column_blob( stmtRead, 1 );
				if( v ) {
					buf.add( v, sqlite3_column_bytes(stmtRead, 1) );
				}
			}
			return true;
		}
		HT4C_SQLITE_VERIFY( st, db, 0 );
		return false;
	}

	bool SQLiteEnv::sysDbExists( const char* name, int len, int64_t* rowid ) {
		bool exists = false;

		Util::StmtReset stmt( stmtFind );

		int st = sqlite3_bind_text( stmtFind, 1, name, len, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_step( stmtFind );
		exists = st == SQLITE_ROW;
		HT4C_SQLITE_VERIFY( st, db, 0 );

		if( exists && rowid ) {
			*rowid = sqlite3_column_int64( stmtFind, 0 );
		}

		return exists;
	}

	bool SQLiteEnv::sysDbDelete( const char* name, int len, int64_t* rowid ) {
		if( !sysDbExists(name, len, rowid) ) {
			return false;
		}

		Util::StmtReset stmt( stmtDelete );

		int st = sqlite3_bind_text( stmtDelete, 1, name, len, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_step( stmtDelete );
		bool dropped = st == SQLITE_DONE;
		HT4C_SQLITE_VERIFY( st, db, 0 );
		return dropped;
	}

	void SQLiteEnv::sysDbCreateTable( const char* name, int len, const void* value, int size, int64_t& id ) {
		sysDbInsert( name, len, value, size, &id );

		char* errmsg = 0;
		int st = sqlite3_exec( db
												 , Hypertable::format("CREATE TABLE IF NOT EXISTS "
																							"t%lld (r TEXT NOT NULL, cf INTEGER NOT NULL, cq TEXT NOT NULL, ts INTEGER NOT NULL, v BLOB,"
																							"UNIQUE(r, cf, cq, ts));", id).c_str()
												 , 0, 0, &errmsg );

		HT4C_SQLITE_VERIFY( st, db, errmsg );

		if( indexColumn ) {
			st = sqlite3_exec( db
											 , Hypertable::format("CREATE INDEX IF NOT EXISTS i_cfcq ON t%lld (cf, cq);", id).c_str()
											 , 0, 0, &errmsg );

			HT4C_SQLITE_VERIFY( st, db, errmsg );
		}

		if( indexColumnFamily ) {
			st = sqlite3_exec( db
											 , Hypertable::format("CREATE INDEX IF NOT EXISTS i_cf ON t%lld (cf);", id).c_str()
											 , 0, 0, &errmsg );

			HT4C_SQLITE_VERIFY( st, db, errmsg );
		}

		if( indexColumnQualifier ) {
			st = sqlite3_exec( db
											 , Hypertable::format("CREATE INDEX IF NOT EXISTS i_cq ON t%lld (cq);", id).c_str()
											 , 0, 0, &errmsg );

			HT4C_SQLITE_VERIFY( st, db, errmsg );
		}

		if( indexTimestamp ) {
			st = sqlite3_exec( db
											 , Hypertable::format("CREATE INDEX IF NOT EXISTS i_ts ON t%lld (ts);", id).c_str()
											 , 0, 0, &errmsg );

			HT4C_SQLITE_VERIFY( st, db, errmsg );
		}
	}

	bool SQLiteEnv::sysDbOpenTable( Db::Table* table, int64_t& id ) {
		const char* key;
		int len = table->toKey( key );
		Hypertable::DynamicBuffer buf;
		if( sysDbRead(key, len, buf, &id) ) {
			table->fromRecord( buf );
			tables[id].insert( table );
			return true;
		}
		return false;
	}

	bool SQLiteEnv::sysDbRefreshTable( Db::Table* table ) {
		const char* key;
		int len = table->toKey( key );
		Hypertable::DynamicBuffer buf;
		if( sysDbRead(key, len, buf, 0) ) {
			table->fromRecord( buf );
			return true;
		}
		return false;
	}

	void SQLiteEnv::sysDbRefreshTable( int64_t id ) {
		tables_t::const_iterator it = tables.find( id );
		if( it != tables.end() ) {
			for each( Db::Table* table in (*it).second ) {
				table->refresh();
			}
		}
	}

	void SQLiteEnv::sysDbDisposeTable( int64_t id ) {
		tables.erase( id );
	}

	bool SQLiteEnv::sysDbDeleteTable( const char* name, int len ) {
		int64_t id;
		if( sysDbDelete(name, len, &id) ) {
			tables_t::iterator it = tables.find( id );
			if( it != tables.end() ) {
				std::set<Db::Table*> t( (*it).second ); // shallow copy, table->dispose() might call sysDbDisposeTable( id )
				for each( Db::Table* table in t ) {
					table->dispose();
				}
				tables.erase( id );
			}

			// finalize begin, commit and rollback otherwise the table might be locked
			Util::stmt_finalize( db, &stmtBegin );
			Util::stmt_finalize( db, &stmtCommit );
			Util::stmt_finalize( db, &stmtRollback );

			char* errmsg = 0;
			int dropped = sqlite3_exec( db, Hypertable::format("DROP TABLE t%d;", id).c_str(), 0, 0, &errmsg );

			// .. and prepare again
			int st = sqlite3_prepare_v2( db, "BEGIN;", -1, &stmtBegin, 0 );
			HT4C_SQLITE_VERIFY( st, db, 0 );

			st = sqlite3_prepare_v2( db, "COMMIT;", -1, &stmtCommit, 0 );
			HT4C_SQLITE_VERIFY( st, db, 0 );

			st = sqlite3_prepare_v2( db, "ROLLBACK;", -1, &stmtRollback, 0 );
			HT4C_SQLITE_VERIFY( st, db, 0 );

			HT4C_SQLITE_VERIFY( dropped, db, errmsg );
			return true;
		}
		return false;
	}

} }
