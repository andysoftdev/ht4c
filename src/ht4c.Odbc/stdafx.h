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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "Common/ReferenceCount.h"
#include "Common/HRTimer.h"
#include "Hypertable/Lib/KeySpec.h"
#include "Hypertable/Lib/Namespace.h"
#include "re2/re2.h"
#include "ht4c.Common/Utils.h"
#include "ht4c.Common/KeyBuilder.h"

//#define OTL_ODBC_MULTI_MODE
#define OTL_ODBC_MSSQL_2008
#define OTL_STL
#define OTL_STREAM_POOLING_ON
//#define OTL_ODBC_USES_SQL_FETCH_SCROLL_WHEN_SPECIFIED_IN_OTL_CONNECT
#define OTL_EXPLICIT_NAMESPACES
#define OTL_BIGINT __int64
#include "otlv4.h"

class varbinary : public otl_long_string {

	public:

		varbinary()
			: otl_long_string()
		{
		}

		varbinary( const void* data, int len )
			: otl_long_string( data, len, len )
		{
		}

		varbinary( const char* data, int len )
			: otl_long_string( data, !len && data ? 1 : len, !len && data ? 1 : len )
		{
		}

		varbinary( const std::string& s )
			: otl_long_string( s.c_str(), s.length() + 1, s.length() + 1 )
		{
		}

		const char* c_str() {
			v[len()] = 0;
			return reinterpret_cast<const char*>( v );
		}
};

inline odbc::otl_stream& operator<<(odbc::otl_stream& s, const varbinary& v)
{
	if( v.v && v.len() ) {
		s.operator<<( v );
	}
	else {
		s.operator<<( otl_null() );
	}

	return s;
};

#include "OdbcDb.h"
#include "OdbcException.h"

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

namespace ht4c { namespace Odbc { namespace Db { namespace Util {

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

	inline std::string hex( const std::string& s ) {
		std::string h;
		h.reserve( 5 + s.length() * 2 );
		h += "0x";
		for( std::string::const_iterator it = s.begin(); it!=s.end(); ++it ) {
			h += boost::uuids::detail::to_char(((*it) >> 4) & 0x0F);
			h += boost::uuids::detail::to_char( (*it) & 0x0F );
		}

		h += "00";
		return h;
	}

	inline std::string hex( const char* sz ) {
		std::string s( sz );
		return hex( s );
	}

	inline std::string nhex( const std::string& s ) {
		std::string h;
		h.reserve( 3 + s.length() * 2 );
		h += "0x";
		for( std::string::const_iterator it = s.begin(); it!=s.end(); ++it ) {
			h += boost::uuids::detail::to_char(((*it) >> 4) & 0x0F);
			h += boost::uuids::detail::to_char( (*it) & 0x0F );
		}

		return h;
	}

	inline std::string nhex( const char* sz ) {
		std::string s( sz );
		return nhex( s );
	}

	inline std::string escape( const std::string& s ) {
		std::string escaped( s );
		boost::replace_all( escaped, "'", "''" );
		return escaped;
	}

	inline std::string escape( const char* sz ) {
		std::string escaped( sz );
		boost::replace_all( escaped, "'", "''" );
		return escaped;
	}

	inline odbc::otl_stream* newOdbcStream( ) {
		odbc::otl_stream* os = new odbc::otl_stream();
		os->set_commit( 0 );

		return os;
	}

	inline odbc::otl_stream* newOdbcStream( int bufferSize, const std::string& sql, odbc::otl_connect* db, bool lobStreamMode = false ) {
		odbc::otl_stream* os = new odbc::otl_stream( );

		if( lobStreamMode ) {
			os->set_lob_stream_mode( lobStreamMode );
		}

		os->open( lobStreamMode ? 1 : bufferSize, sql.c_str(), *db );
		os->set_commit( 0 );

		return os;
	}

	class Tx {

		public:

			Tx( odbc::otl_connect* _db )
				: db( _db )
				, tx( false )
			{
				tx = true;
			}

			~Tx( )
			{
				if( tx ) {
					db->rollback();
				}
			}

			void begin( ) {
				if( !tx ) {
					tx = true;
				}
			}

			void commit( ) {
				if( tx ) {
					db->commit();
					tx = false;
				}
			}

			odbc::otl_connect* getDb( ) const {
				return db;
			}

		private:

			odbc::otl_connect* db;
			bool tx;
	};

} } } }
