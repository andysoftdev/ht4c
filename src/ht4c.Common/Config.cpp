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
#include "Config.h"

namespace ht4c { namespace Common { 

	const char* Config::ProviderName												= "Ht4n.Provider";
	const char* Config::ProviderNameAlias										= "Provider";
	const char* Config::ProviderHyper												= "Hyper";
	const char* Config::ProviderThrift											= "Thrift";

	const char* Config::Uri																	= "Ht4n.Uri";
	const char* Config::UriAlias														= "Uri";

	const char* Config::ConnectionTimeout										= "Ht4n.ConnectionTimeout";
	const char* Config::ConnectionTimeoutAlias							= "ConnectionTimeout";

	const char* Config::ComposablePartCatalogs							= "Ht4n.Composition.ComposablePartCatalogs";

#ifdef SUPPORT_HAMSTERDB

	const char* Config::ProviderHamster											= "Hamster";
	const char* Config::HamsterFilename											= "Ht4n.Hamster.Filename";
	const char* Config::HamsterEnableRecovery								= "Ht4n.Hamster.EnableRecovery";
	const char* Config::HamsterEnableAutoRecovery						= "Ht4n.Hamster.EnableAutoRecovery";
	const char* Config::HamsterCacheSizeMB									= "Ht4n.Hamster.CacheSizeMB";
	const char* Config::HamsterPageSizeKB										= "Ht4n.Hamster.PageSizeKB";

#endif

#ifdef SUPPORT_SQLITEDB

	const char* Config::ProviderSQLite											= "SQLite";
	const char* Config::SQLiteFilename											= "Ht4n.SQLite.Filename";
	const char* Config::SQLiteCacheSizeMB										= "Ht4n.SQLite.CacheSizeMB";
	const char* Config::SQLitePageSizeKB										= "Ht4n.SQLite.PageSizeKB";
	const char* Config::SQLiteSynchronous										= "Ht4n.SQLite.Synchronous";
	const char* Config::SQLiteIndexColumn										= "Ht4n.SQLite.Index.Column";
	const char* Config::SQLiteIndexColumnFamily							= "Ht4n.SQLite.Index.ColumnFamily";
	const char* Config::SQLiteIndexColumnQualifier					= "Ht4n.SQLite.Index.ColumnQualifier";
	const char* Config::SQLiteIndexTimestamp								= "Ht4n.SQLite.Index.Timestamp";

#endif

#ifdef SUPPORT_ODBC

	const char* Config::ProviderOdbc												= "Odbc";
	const char* Config::OdbcConnectionString								= "Ht4n.Odbc.ConnectionString";
	const char* Config::OdbcIndexColumn											= "Ht4n.Odbc.Index.Column";
	const char* Config::OdbcIndexColumnFamily								= "Ht4n.Odbc.Index.ColumnFamily";
	const char* Config::OdbcIndexColumnQualifier						= "Ht4n.Odbc.Index.ColumnQualifier";
	const char* Config::OdbcIndexTimestamp									= "Ht4n.Odbc.Index.Timestamp";

#endif

} }