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

#include "Common/Compat.h"
#include "Common/ReferenceCount.h"
#include "Common/HRTimer.h"
#include "Hypertable/Lib/KeySpec.h"
#include "Hypertable/Lib/Canonicalize.h"
#include "Hypertable/Lib/Namespace.h"
#include "re2/re2.h"
#include "ht4c.Common/Utils.h"

#include "SQLiteDb.h"
#include "SQLiteException.h"

inline const char* CF( const char* columnFamily ) {
	return ht4c::Common::CF( columnFamily );
}

inline uint8_t FLAG( const char* columnFamily, const char* columnQualifier, uint8_t flag ) {
	return ht4c::Common::FLAG( columnFamily, columnQualifier, flag );
}

inline uint8_t FLAG_DELETE( const char* columnFamily, const char* columnQualifier ) {
	return ht4c::Common::FLAG_DELETE( columnFamily, columnQualifier );
}

inline uint64_t TIMESTAMP( uint64_t timestamp, uint8_t flag ) {
	return ht4c::Common::TIMESTAMP( timestamp, flag );
}

namespace ht4c { namespace SQLite { namespace Db { namespace Util {

	inline bool KeyHasClassifier( const void* key, char classifier ) {
		return key && *reinterpret_cast<const char*>(key) == classifier;
	}

	inline char* KeyToString( void* key ) {
    char* k = reinterpret_cast<char*>( key );
    k += KeyClassifiers::Length;
		return k;
	}

	inline const char* KeyToString( const void* key ) {
		const char* k = reinterpret_cast<const char*>( key );
    k += KeyClassifiers::Length;
		return k;
	}

	inline char* KeyToString( void* key, char classifier ) {
		return KeyHasClassifier(key, classifier) ? KeyToString( key ) : 0;
	}

	inline const char* KeyToString( const void* key, char classifier ) {
		return KeyHasClassifier(key, classifier) ? KeyToString( key ) : 0;
	}

	inline bool KeyStartWith( const void* key, char classifier, const std::string& preffix ) {
		const char* name = KeyToString( key, classifier );
		return name && strstr(name, preffix.c_str()) == name;
	}

	inline const char* CQ( const char* cq ) {
		return cq ? cq : "";
	}

	static void stmt_finalize( sqlite3* db, sqlite3_stmt** stmt ) {
		if( *stmt ) {
			sqlite3_clear_bindings( *stmt );
			sqlite3_reset( *stmt );
			int st = sqlite3_finalize(*stmt);
			HT4C_SQLITE_VERIFY( st, db, 0 );
			*stmt = 0;
		}
	}

	class StmtReset {

		public:

			explicit StmtReset( sqlite3_stmt* _stmt )
			: stmt( _stmt ) 
			{
			}

			~StmtReset( )
			{
				sqlite3_clear_bindings( stmt );
				sqlite3_reset( stmt );
			}

		private:

			sqlite3_stmt* stmt;
	};

	class StmtFinalize{

		public:

			StmtFinalize( sqlite3* _db, sqlite3_stmt** _stmt )
			: db( _db )
			, stmt( _stmt ) 
			{
			}

			~StmtFinalize( )
			{
				stmt_finalize( db, stmt );
			}

		private:

			sqlite3* db;
			sqlite3_stmt** stmt;
	};

	class Tx {

		public:

			Tx( SQLiteEnv* _env )
				: env( _env )
				, tx( false )
			{
				env->txBegin();
				tx = true;
			}

			void begin( ) {
				if( !tx ) {
					env->txBegin();
					tx = true;
				}
			}

			void commit( ) {
				if( tx ) {
					env->txCommit();
					tx = false;
				}
			}

			~Tx( )
			{
				if( tx ) {
					env->txRollback();
				}
			}

		private:

			SQLiteEnv* env;
			bool tx;
	};

} } } }
