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

#include "OdbcClient.h"
#include "ht4c.Common/Namespace.h"

namespace Hypertable { namespace OdbcGen { 
	class NamespaceListing;
} }

namespace ht4c { namespace Odbc {

	class Client;
	class Table;

	/// <summary>
	/// Represents Hypertable namespace, using the Odbc API.
	/// </summary>
	class OdbcNamespace : public Common::Namespace {

		public:

			/// <summary>
			/// Creates a new OdbcNamespace instance.
			/// </summary>
			/// <param name="ns">Namespace</param>
			/// <param name="name">Namespace name</param>
			/// <returns>New OdbcNamespace instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Namespace* create( Db::NamespacePtr ns, const std::string& name );

			/// <summary>
			/// Destroys the OdbcNamespace instance.
			/// </summary>
			virtual ~OdbcNamespace( );

			/// <summary>
			/// Returns the Odbc namespace.
			/// </summary>
			/// <returns>Odbc namespace</returns>
			inline Db::NamespacePtr get( ) const {
				return ns;
			}

			#pragma region Common::Namespace methods

			virtual std::string getName( ) const;
			virtual void createTable( const char* name, const char* schema );
			virtual void createTableLike( const char* name, const char* like );
			virtual void alterTable( const char* name, const char* schema );
			virtual void renameTable( const char* nameOld, const char* nameNew );
			virtual Common::Table* openTable( const char* name, bool force );
			virtual void dropTable( const char* name, bool ifExists );
			virtual bool existsTable( const char* name );
			virtual std::string getTableSchema( const char* name, bool withIds = false );
			virtual void getListing( bool deep, ht4c::Common::NamespaceListing& nsListing );
			virtual void exec( const char* hql );
			virtual Common::Cells* query( const char* hql );

			#pragma endregion

		private:

			OdbcNamespace( Db::NamespacePtr ns, const std::string& name );

			OdbcNamespace( ) { }
			OdbcNamespace( const OdbcNamespace& ) { }
			OdbcNamespace& operator = ( const OdbcNamespace& ) { return *this; }

			void getListing( const std::vector<Db::NamespaceListing>& listing, ht4c::Common::NamespaceListing& nsListing );

			Db::NamespacePtr ns;
			std::string name;
	};

} }
