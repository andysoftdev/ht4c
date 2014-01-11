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

#include "ht4c.Common/Exception.h"

namespace ht4c { namespace SQLite {

	class error : public std::exception {

		public:

			error( int _st, const char* msg )
				: std::exception( msg )
				, st( _st )
			{
			}

			inline int get_errno( ) const {
				return st;
			}

		private:

			int st;
	};

} }

/// <summary>
/// SQlite status failed.
/// </summary>
#define HT4C_SQLITE_FAILED( st ) \
	( (st) != SQLITE_OK )

/// <summary>
/// SQlite status succeeded.
/// </summary>
#define HT4C_SQLITE_SUCCEEDED( st ) \
	( (st) == SQLITE_OK )

/// <summary>
/// Throws a hypertable exception on sqlite failure.
/// </summary>
#define HT4C_SQLITE_VERIFY( st, db, msg ) \
	if( st != SQLITE_OK && st < SQLITE_ROW ) { \
		std::string what( ((const char*)msg) && *((const char*)msg) ? (const char*)msg : sqlite3_errmsg(db) ); \
		if( msg ) sqlite3_free( msg ); \
		throw ht4c::SQLite::error( st, what.c_str() ); \
	}

/// <summary>
/// Defines the catch clause of ht4c sqlite try/catch blocks.
/// </summary>
/// <remarks>
/// Translates sqlite exceptions into ht4c exceptions.
/// </remarks>
#define HT4C_SQLITE_RETHROW \
		catch( ht4c::SQLite::error& e ) { \
		std::stringstream ss; \
		ss << e.what() << "\n\tat " << __FUNCTION__ << " (" << __FILE__ << ':' << __LINE__ << ')'; \
		throw ht4c::Common::HypertableException( Hypertable::Error::EXTERNAL, ss.str(), __LINE__, __FUNCTION__, __FILE__ ); \
	} \
	HT4C_RETHROW

/// <summary>
/// Throws a hypertable exception.
/// </summary>
#define HT4C_SQLITE_THROW( error, msg ) \
	throw Hypertable::Exception( error, msg, __LINE__, __FUNCTION__, __FILE__ );
