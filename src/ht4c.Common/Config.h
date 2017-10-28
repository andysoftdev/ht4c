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
#pragma managed( push, off )
#endif

namespace ht4c { namespace Common {

	/// <summary>
	/// Represents various configuration and connection string options.
	/// </summary>
	class Config {

		public:

			/// <summary>
			/// Provider name.
			/// </summary>
			static const char* ProviderName;

			/// <summary>
			/// Provider name alias.
			/// </summary>
			static const char* ProviderNameAlias;

#ifdef SUPPORT_HYPERTABLE

			/// <summary>
			/// Provider name value - Hyper.
			/// </summary>
			static const char* ProviderHyper;

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

			/// <summary>
			/// Provider name value - Thrift.
			/// </summary>
			static const char* ProviderThrift;

#endif

#ifdef SUPPORT_HAMSTERDB

			/// <summary>
			/// Provider name value - Hamster.
			/// </summary>
			static const char* ProviderHamster;

#endif

#ifdef SUPPORT_SQLITEDB

			/// <summary>
			/// Provider name value - SQLite.
			/// </summary>
			static const char* ProviderSQLite;

#endif

#ifdef SUPPORT_ODBC

			/// <summary>
			/// Provider name value - ODBC.
			/// </summary>
			static const char* ProviderOdbc;

#endif

			/// <summary>
			/// Uri, hostname:port.
			/// </summary>
			static const char* Uri;

			/// <summary>
			/// Uri alias.
			/// </summary>
			static const char* UriAlias;

			/// <summary>
			/// Connection timeout [ms]
			/// </summary>
			static const char* ConnectionTimeout;

			/// <summary>
			/// Connection timeout [ms] alias.
			/// </summary>
			static const char* ConnectionTimeoutAlias;

#ifdef SUPPORT_HAMSTERDB

			/// <summary>
			/// Hamster db filename.
			/// </summary>
			static const char* HamsterFilename;

			/// <summary>
			/// Hamster db enable recovery.
			/// </summary>
			static const char* HamsterEnableRecovery;

			/// <summary>
			/// Hamster db enable auto recovery.
			/// </summary>
			static const char* HamsterEnableAutoRecovery;

			/// <summary>
			/// Hamster db cache size [MB].
			/// </summary>
			static const char* HamsterCacheSizeMB;

			/// <summary>
			/// Hamster db page size [KB], multiple of 64KB.
			/// </summary>
			static const char* HamsterPageSizeKB;

#endif

#ifdef SUPPORT_SQLITEDB

			/// <summary>
			/// SQLite db filename.
			/// </summary>
			static const char* SQLiteFilename;

			/// <summary>
			/// SQLite db cache size [MB].
			/// </summary>
			static const char* SQLiteCacheSizeMB;

			/// <summary>
			/// SQLite db page size [KB].
			/// </summary>
			static const char* SQLitePageSizeKB;

			/// <summary>
			/// SQLite synchronous.
			/// </summary>
			static const char* SQLiteSynchronous;

			/// <summary>
			/// SQLite unique rows.
			/// </summary>
			static const char* SQLiteUniqueRows;

			/// <summary>
			/// SQLite no cell revisions.
			/// </summary>
			static const char* SQLiteNoCellRevisions;  

			/// <summary>
			/// SQLite column index.
			/// </summary>
			static const char* SQLiteIndexColumn;

			/// <summary>
			/// SQLite column family index.
			/// </summary>
			static const char* SQLiteIndexColumnFamily;

			/// <summary>
			/// SQLite column qualifier index.
			/// </summary>
			static const char* SQLiteIndexColumnQualifier;

			/// <summary>
			/// SQLite timestamp index.
			/// </summary>
			static const char* SQLiteIndexTimestamp;

#endif

#ifdef SUPPORT_ODBC

			/// <summary>
			/// Odbc connection string.
			/// </summary>
			static const char* OdbcConnectionString;

			/// <summary>
			/// Odbc column index.
			/// </summary>
			static const char* OdbcIndexColumn;

			/// <summary>
			/// Odbc column family index.
			/// </summary>
			static const char* OdbcIndexColumnFamily;

			/// <summary>
			/// Odbc column qualifier index.
			/// </summary>
			static const char* OdbcIndexColumnQualifier;

			/// <summary>
			/// Odbc timestamp index.
			/// </summary>
			static const char* OdbcIndexTimestamp;


#endif

			/// <summary>
			/// Composable part catalog (only used for MEF composition).
			/// </summary>
			static const char* ComposablePartCatalogs;

	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif