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

#pragma once

#ifdef __cplusplus_cli
#pragma managed( push, off )
#endif

#include "Types.h"
#include "NamespaceListing.h"

namespace ht4c { namespace Common {

	class Client;
	class Table;
	class Cells;

	/// <summary>
	/// Abstract class represents Hypertable namespace.
	/// </summary>
	class Namespace {

		public:

			/// <summary>
			/// Destroys the Namespace instance.
			/// </summary>
			virtual ~Namespace( ) { }

			/// <summary>
			/// Returns the fully qualified namespace name.
			/// </summary>
			/// <returns>Fully qualified namespace name</returns>
			virtual std::string getName( ) const = 0;

			/// <summary>
			/// Creates a new table.
			/// </summary>
			/// <param name="name">Table name</param>
			/// <param name="schema">Xml table name</param>
			virtual void createTable( const char* name, const char* schema ) = 0;

			/// <summary>
			/// Creates a new table like another existing.
			/// </summary>
			/// <param name="name">Table name</param>
			/// <param name="like">Name of another existing table</param>
			virtual void createTableLike( const char* name, const char* like ) = 0;

			/// <summary>
			/// Alter an existing table.
			/// </summary>
			/// <param name="name">Table name</param>
			/// <param name="schema">Xml table name</param>
			virtual void alterTable( const char* name, const char* schema ) = 0;

			/// <summary>
			/// Rename an existing table.
			/// </summary>
			/// <param name="nameOld">Old table name</param>
			/// <param name="nameNew">New table name</param>
			virtual void renameTable( const char* nameOld, const char* nameNew ) = 0;

			/// <summary>
			/// Opens an existing table.
			/// </summary>
			/// <param name="name">Table name</param>
			/// <param name="force">If true any table caches will be bypassed</param>
			/// <returns>Opened table.</returns>
			virtual Table* openTable( const char* name, bool force = false ) = 0;

			/// <summary>
			/// Drops a table.
			/// </summary>
			/// <param name="name">Table name</param>
			/// <param name="ifExists">If true and the table does not exist the method won't fail</param>
			virtual void dropTable( const char* name, bool ifExists ) = 0;

			/// <summary>
			/// Checks if a table exists.
			/// </summary>
			/// <param name="name">Table name</param>
			/// <returns>true if the table exists</returns>
			virtual bool existsTable( const char* name ) = 0;

			/// <summary>
			/// Returns the xml schema for an existing table.
			/// </summary>
			/// <param name="name">Table name</param>
			/// <param name="withIds">Include the id's in the schema</param>
			/// <returns>xml schema</returns>
			virtual std::string getTableSchema( const char* name, bool withIds = false ) = 0;

			/// <summary>
			/// Gets the namespace listing.
			/// </summary>
			/// <param name="deep">If true all sub-namespaces and there containing tables/namespaces will be returned</param>
			/// <param name="nsListing">Receives the listing</param>
			virtual void getListing( bool deep, NamespaceListing& nsListing ) = 0;

			/// <summary>
			/// Executes a HQL command.
			/// </summary>
			/// <param name="hql">Single HQL command</param>
			virtual void exec( const char* hql ) = 0;

			/// <summary>
			/// Executes a HQL query.
			/// </summary>
			/// <param name="hql">Single HQL query</param>
			/// <returns>Resulting cells</returns>
			/// <remarks>To free the returned cells, use the delete operator.</remarks>
			virtual Cells* query( const char* hql ) = 0;

			/// <summary>
			/// Validates a table name.
			/// </summary>
			/// <param name="name">Table name</param>
			/// <remarks>Throws an exception if the table name is not valid.</remarks>
			static void validateTableName( const char* name );

		protected:

			/// <summary>
			/// Creates a new Namespace instance.
			/// </summary>
			Namespace( ) { }

		private:

			Namespace( const Namespace& ) { }
			Namespace& operator = ( const Namespace& ) { return *this; }
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif