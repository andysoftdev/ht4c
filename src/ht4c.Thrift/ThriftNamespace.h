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
#error compile native
#endif

#include "ThriftClient.h"
#include "ht4c.Common/Namespace.h"

namespace Hypertable { namespace ThriftGen { 
	class NamespaceListing;
} }

namespace ht4c { namespace Thrift {

	class Client;
	class Table;

	/// <summary>
	/// Rrepresents Hypertable namespace, using the thrift API.
	/// </summary>
	class ThriftNamespace : public Common::Namespace {

		public:

			/// <summary>
			/// Creates a new ThriftNamespace instance.
			/// </summary>
			/// <param name="client">Thrift client</param>
			/// <param name="ns">Namespace</param>
			/// <param name="name">Namespace name</param>
			/// <returns>New ThriftNamespace instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Namespace* create( Hypertable::Thrift::ClientPtr client, const Hypertable::ThriftGen::Namespace& ns, const std::string& name );

			/// <summary>
			/// Destroys the ThriftNamespace instance.
			/// </summary>
			virtual ~ThriftNamespace( );

			/// <summary>
			/// Returns the thrift namespace.
			/// </summary>
			/// <returns>Thrift namespace</returns>
			inline Hypertable::ThriftGen::Namespace get( ) const {
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

			ThriftNamespace( Hypertable::Thrift::ClientPtr client, const Hypertable::ThriftGen::Namespace& ns, const std::string& name );

			ThriftNamespace( ) { }
			ThriftNamespace( const ThriftNamespace& ) { }
			ThriftNamespace& operator = ( const ThriftNamespace& ) { return *this; }

			void getListing( bool deep, const std::string& nsName, const std::vector<Hypertable::ThriftGen::NamespaceListing>& listing, ht4c::Common::NamespaceListing& nsListing );

			Hypertable::Thrift::ClientPtr client;
			Hypertable::ThriftGen::Namespace ns;
			std::string name;
	};

} }
