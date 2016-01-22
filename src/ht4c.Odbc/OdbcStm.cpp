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
#include "OdbcStm.h"
#include "OdbcDb.h"

namespace ht4c { namespace Odbc {

	OdbcStm::OdbcStm( const std::string& _tableId )
	: tableId( _tableId )
	{
	}

	OdbcStm::OdbcStm( Db::Table* table )
	: tableId( table->getId() )
	{
	}

	std::string OdbcStm::sysDbCreate( ) {
		return "CREATE TABLE sys_db (id VARCHAR(37) PRIMARY KEY, k VARBINARY(512) NOT NULL, v VARBINARY(4096), UNIQUE(k));";
	}

	std::string OdbcStm::sysDbInsert( ) {
		return "INSERT INTO sys_db VALUES(:id<raw[37]>,:k<raw[512]>,:v<raw[4096]>);";
	}

	std::string OdbcStm::sysDbUpdateKey( ) {
		return "UPDATE sys_db SET k=:k<raw[512]> WHERE id=:id<raw[37]>;";
	}

	std::string OdbcStm::sysDbUpdateValue( ) {
		return "UPDATE sys_db SET v=:v<raw[4096]> WHERE id=:id<raw[37]>;";
	}

	std::string OdbcStm::sysDbReadKeyAndValue( ) {
		return "SELECT id:#1<raw[37]>, v:#2<raw[4096]> FROM sys_db WHERE k=:k<raw[512]>;";
	}

	std::string OdbcStm::sysDbReadValue( ) {
		return "SELECT v:#1<raw[4096]> FROM sys_db WHERE k=:k<raw[512]>;";
	}

	std::string OdbcStm::sysDbExists( ) {
		return "SELECT id:#1<raw[37]> FROM sys_db WHERE k=:k<raw[512]>;";
	}

	std::string OdbcStm::sysDbDelete( ) {
		return "DELETE FROM sys_db WHERE k=:k<raw[512]>;";
	}

	std::string OdbcStm::sysDbQueryKey( ) {
		return "SELECT k:#1<raw[512]> FROM sys_db WHERE k>:k<raw[512]> ORDER BY k;";
	}

	std::string OdbcStm::sysDbQueryKeyAndValue( ) {
		return "SELECT k:#1<raw[512]>, v:#2<raw[4096]> FROM sys_db WHERE k>:k<raw[512]> ORDER BY k;";
	}

	std::string OdbcStm::createTable( ) const {
		return Hypertable::format(
				"CREATE TABLE "
				"%s (r VARBINARY(512) NOT NULL, cf INTEGER NOT NULL, cq VARBINARY(512) NOT NULL, ts BIGINT NOT NULL, v VARBINARY(MAX),"
				"UNIQUE(r, cf, cq, ts));",
				tableId.c_str());
	}

	std::string OdbcStm::deleteTable( ) const {
		return Hypertable::format(
				"DROP TABLE %s;",
				tableId.c_str());
	}

	std::string OdbcStm::insert( ) const {

		/*
		Hypertable::format(
				"CREATE PROCEDURE I%s (@r VARBINARY(512), @cf int, @cq VARBINARY(512), @ts BIGINT, @v VARBINARY(MAX)) AS "
				"SET NOCOUNT ON;"
				"BEGIN TRY INSERT INTO %s (r, cf, cq, ts, v) VALUES(@r,@cf,@cq,@ts,@v); END TRY "
				"BEGIN CATCH UPDATE %s SET v=@v WHERE r=@r AND cf=@cf AND cq=@cq AND ts=@ts; END CATCH"
		 , id.c_str(), id.c_str(), id.c_str())
		 */
		//"EXECUTE I%s @r=:r<raw[512]>, @cf=:cf<int>, @cq=:cq<raw[512]>, @ts=:ts<bigint>, @v=:v<raw_long>;"
		//"INSERT INTO %s VALUES(:r<raw[512]>, :cf<int>, :cq<raw[512]>, :ts<bigint>, :v<raw_long]>)"

		return Hypertable::format(
				"MERGE INTO %s AS t "
				"USING (VALUES (:r<raw[512]>, :cf<int>, :cq<raw[512]>, :ts<bigint>, :v<raw_long>)) AS s(r, cf, cq, ts, v) "
				"ON (t.r=s.r AND t.cf=s.cf AND t.cq=s.cq AND t.ts=s.ts) "
				"WHEN NOT MATCHED THEN INSERT (r, cf, cq, ts, v) VALUES (s.r, s.cf, s.cq, s.ts, s.v) "
				"WHEN MATCHED THEN UPDATE SET v = s.v;",
				tableId.c_str());
	}

	std::string OdbcStm::deleteRow( ) const {
		return Hypertable::format(
				"DELETE FROM %s WHERE r=:r<raw[512]> AND ts>:ts<bigint>",
				tableId.c_str());
	}

	std::string OdbcStm::deleteColumnFamily( ) const {
		return Hypertable::format(
				"DELETE FROM %s WHERE r=:r<raw[512]> AND cf=:cf<int> AND ts>:ts<bigint>",
				tableId.c_str());
	}

	std::string OdbcStm::deleteCell( ) const {
		return Hypertable::format(
				"DELETE FROM %s WHERE r=:r<raw[512]> AND cf=:cf<int> AND cq=:cq<raw[512]> AND ts>:ts<bigint>",
				tableId.c_str());
	}

	std::string OdbcStm::deleteCellVersion( ) const {
		return Hypertable::format(
				"DELETE FROM %s WHERE r=:r<raw[512]> AND cf=:cf<int> AND cq=:cq<raw[512]> AND ts=:ts<bigint>",
				tableId.c_str());
	}

	std::string OdbcStm::deleteCutoffTime( ) const {
		return Hypertable::format(
				"DELETE FROM %s WHERE r=:r<raw[512]> AND cf=:cf<int> AND ts>:ts<bigint>",
				tableId.c_str());
	}

	std::string OdbcStm::select( const std::string& columns, const std::string& predicate ) const {
		return Hypertable::format(
				"SELECT %s FROM %s%s ORDER BY r, cf, cq, ts;", 
				columns.c_str(), 
				tableId.c_str(), 
				predicate.c_str());
	}

	std::string OdbcStm::selectRowInterval( const std::string& columns, const std::string& predicate ) const {
		return Hypertable::format(
				"SELECT %s FROM %s WHERE (r>=:r1<raw[512]> AND r<=:r2<raw[512]>)%s ORDER BY r, cf, cq, ts;",
				columns.c_str(), 
				tableId.c_str(), 
				predicate.c_str());
	}

	std::string OdbcStm::selectRowInterval( const std::string& columns, const std::string& predicate, bool startInclusive, bool endInclusive ) const {
		return Hypertable::format(
				"SELECT %s FROM %s WHERE (r>%s:r1<raw[512]> AND r<%s:r2<raw[512]>)%s ORDER BY r, cf, cq, ts;",
				columns.c_str(), 
				tableId.c_str(), 
				startInclusive ? "=" : "",
				endInclusive ? "=" : "",
				predicate.c_str());
	}

	std::string OdbcStm::selectRow( const std::string& columns, const std::string& predicate ) const {
		return Hypertable::format(
				"SELECT %s FROM %s WHERE r=:r<raw[512]>%s ORDER BY r, cf, cq, ts;",
				columns.c_str(), 
				tableId.c_str(), 
				predicate.c_str());
	}

	std::string OdbcStm::selectRowColumnFamilyInterval( const std::string& columns, const std::string& predicate ) const {
		return Hypertable::format(
				"SELECT %s FROM %s WHERE r=:r<raw[512]> AND cf>=:cf1<int> AND cf<=:cf2<int>%s ORDER BY r, cf, cq, ts;",
				columns.c_str(), 
				tableId.c_str(), 
				predicate.c_str());
	}

	std::string OdbcStm::selectRowColumnFamily( const std::string& columns, const std::string& predicate ) const {
		return Hypertable::format(
				"SELECT %s FROM %s WHERE r=:r<raw[512]> AND cf=:cf<int>%s ORDER BY r, cf, cq, ts;",
				columns.c_str(), 
				tableId.c_str(), 
				predicate.c_str());
	}

} }
