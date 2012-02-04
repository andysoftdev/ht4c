/** -*- C++ -*-
 * Copyright (C) 2010-2012 Thalmann Software & Consulting, http://www.softdev.ch
 *
 * This file is part of ht4c.
 *
 * ht4c is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
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

namespace ht4c { namespace Common {

	/// <summary>
	/// Represents a namespace listing, provides tables and sub-namespace names on a namespace.
	/// </summary>
	class NamespaceListing {

		public:

			/// <summary>
			/// Represents a list of table names.
			/// </summary>
			typedef std::vector<std::string> tables_t;

			/// <summary>
			/// Represents a list of namespace listings.
			/// </summary>
			typedef std::vector<NamespaceListing> namespaces_t;

			/// <summary>
			/// Initializes a new instance of the NamespaceListing class.
			/// </summary>
			/// <param name="nsName">Namespace name</param>
			NamespaceListing( const std::string& nsName )
				: name(nsName)
			{
			}

			/// <summary>
			/// Destroys the NamespaceListing instance.
			/// </summary>
			virtual ~NamespaceListing( ) { }

			/// <summary>
			/// Returns the namespace name.
			/// </summary>
			/// <returns>Namespace name</returns>
			const std::string& getName( ) const {
				return name;
			}

			/// <summary>
			/// Returns a collection of sub-namespace listings belonging to this namespace.
			/// </summary>
			/// <returns>Collection of sub-namespace listings</returns>
			const namespaces_t& getNamespaces( ) const {
				return namespaces;
			}

			/// <summary>
			/// Returns a collection of table names belonging to this namespace.
			/// </summary>
			/// <returns>Collection of table names</returns>
			const tables_t& getTables( ) const {
				return tables;
			}

			/// <summary>
			/// Adds a sub-namespace listing to the collection of sub-namespace listings.
			/// </summary>
			/// <param name="nsListing">Sub-namespace listing to add</param>
			/// <returns>Sub-namespace listing added</returns>
			NamespaceListing& addNamespace( const NamespaceListing& nsListing ) {
				namespaces.push_back( nsListing );
				return namespaces.back();
			}

			/// <summary>
			/// Adds a table name to the collection of table names.
			/// </summary>
			/// <param name="table">Table name to add</param>
			void addTable( const std::string& table ) {
				tables.push_back( table );
			}

		private:

			std::string name;
			namespaces_t namespaces;
			tables_t tables;
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif