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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "SQLiteEnv.h"

namespace ht4c { namespace SQLite {

	class SQLiteEnv;

	/// <summary>
	/// Represents the Hypertable sqlite environment configuration.
	/// </summary>
	struct SQLiteEnvConfig {
		int cacheSizeMB;
		int pageSizeKB;
		bool writeAheadLog;
		bool synchronous;
		int autoVacuum; //0=None, 1=FULL, 2=INCREMENTAL
		bool uniqueRows;
		bool noCellRevisions;
		bool indexColumn;
		bool indexColumnFamily;
		bool indexColumnQualifier;
		bool indexTimestamp;

		SQLiteEnvConfig( )
			: cacheSizeMB( 64 )
			, pageSizeKB( 4 )
			, writeAheadLog( false )
			, synchronous( false )
			, autoVacuum( 0 )
			, uniqueRows( false )
			, noCellRevisions( false )
			, indexColumn( false )
			, indexColumnFamily( false )
			, indexColumnQualifier( false )
			, indexTimestamp( false )
		{
		}
	};

	/// <summary>
	/// Represents sqlite environment factory.
	/// </summary>
	class SQLiteFactory {

		public:

			/// <summary>
			/// Creates a new SQLiteEnv instance.
			/// </summary>
			/// <param name="filename">Database filename</param>
			/// <param name="config">Database configuration</param>
			/// <returns>New SQLiteEnv instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static SQLiteEnv* create( const std::string &filename, const SQLiteEnvConfig& config );
	};

} }
