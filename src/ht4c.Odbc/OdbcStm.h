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

namespace ht4c { namespace Odbc {

	namespace Db {

		class Table;

	}

	/// <summary>
	/// Represents the odbc statements.
	/// </summary>
	class OdbcStm {

	public:

		OdbcStm( const std::string& tableId );
		OdbcStm( Db::Table* table );

		static std::string sysDbCreate( );
		static std::string sysDbInsert( );
		static std::string sysDbUpdateKey( );
		static std::string sysDbUpdateValue( );
		static std::string sysDbReadKeyAndValue( );
		static std::string sysDbReadValue( );
		static std::string sysDbExists( );
		static std::string sysDbDelete( );
		static std::string sysDbQueryKey( );
		static std::string sysDbQueryKeyAndValue( );

		std::string createTable( ) const;
		std::string deleteTable( ) const;

		std::string insert( ) const;
		std::string deleteRow( ) const;
		std::string deleteColumnFamily( ) const;
		std::string deleteCell( ) const;
		std::string deleteCellVersion( ) const;
		std::string deleteCutoffTime( ) const;

		std::string select( const std::string& columns, const std::string& predicate ) const;
		std::string selectRowInterval( const std::string& columns, const std::string& predicate ) const;
		std::string selectRowInterval( const std::string& columns, const std::string& predicate, bool startInclusive, bool endInclusive ) const;
		std::string selectRow( const std::string& columns, const std::string& predicate ) const;

		std::string selectRowColumnFamilyInterval( const std::string& columns, const std::string& predicate ) const;
		std::string selectRowColumnFamily( const std::string& columns, const std::string& predicate ) const;

	private:

		std::string tableId;
	};

} }
