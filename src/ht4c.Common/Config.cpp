/** -*- C++ -*-
 * Copyright (C) 2010-2012 Thalmann Software & Consulting, http://www.softdev.ch
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

	const char* Config::ProviderName												= "Hypertable.Client.Provider";
	const char* Config::ProviderNameAlias										= "Provider";
	const char* Config::ProviderHyper												= "Hyper";
	const char* Config::ProviderThrift											= "Thrift";

	const char* Config::Uri																	= "Hypertable.Client.Uri";
	const char* Config::UriAlias														= "Uri";

	const char* Config::ConnectionTimeout										= "Hypertable.Client.ConnectionTimeout";
	const char* Config::ConnectionTimeoutAlias							= "ConnectionTimeout";

	const char* Config::ComposablePartCatalogs							= "Hypertable.Composition.ComposablePartCatalogs";

#ifdef SUPPORT_HAMSTERDB

	const char* Config::ProviderHamster											= "Hamster";
	const char* Config::HamsterFilename											= "Hypertable.Client.Hamster.Filename";
	const char* Config::HamsterFilenameAlias								= "Hamster.Filename";
	const char* Config::HamsterEnableRecovery								= "Hypertable.Client.Hamster.EnableRecovery";
	const char* Config::HamsterEnableRecoveryAlias					= "Hamster.EnableRecovery";
	const char* Config::HamsterEnableAutoRecovery						= "Hypertable.Client.Hamster.EnableAutoRecovery";
	const char* Config::HamsterEnableAutoRecoveryAlias			= "Hamster.EnableAutoRecovery";
	const char* Config::HamsterMaxTables										= "Hypertable.Client.Hamster.MaxTables";
	const char* Config::HamsterMaxTablesAlias								= "Hamster.MaxTables";
	const char* Config::HamsterCacheSizeMB									= "Hypertable.Client.Hamster.CacheSizeMB";
	const char* Config::HamsterCacheSizeMBAlias							= "Hamster.CacheSizeMB";
	const char* Config::HamsterPageSizeKB										= "Hypertable.Client.Hamster.PageSizeKB";
	const char* Config::HamsterPageSizeKBAlias							= "Hamster.PageSizeKB";

#endif

#ifdef SUPPORT_SQLITEDB

	const char* Config::ProviderSQLite											= "SQLite";
	const char* Config::SQLiteFilename											= "Hypertable.Client.SQLite.Filename";
	const char* Config::SQLiteFilenameAlias									= "SQLite.Filename";
	const char* Config::SQLiteCacheSizeMB										= "Hypertable.Client.SQLite.CacheSizeMB";
	const char* Config::SQLiteCacheSizeMBAlias							= "SQLite.CacheSizeMB";
	const char* Config::SQLitePageSizeKB										= "Hypertable.Client.SQLite.PageSizeKB";
	const char* Config::SQLitePageSizeKBAlias								= "SQLite.PageSizeKB";
	const char* Config::SQLiteSynchronous										= "Hypertable.Client.SQLite.Synchronous";
	const char* Config::SQLiteSynchronousAlias							= "SQLite.Synchronous";

#endif

} }