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

#include "ht4c.Common/Namespace.h"

namespace ht4c { namespace Common {
	class Table;
	class Cells;
} }

namespace Hypertable {
	class NamespaceListing;
}

namespace ht4c { namespace Hyper {

	/// <summary>
	/// Rrepresents Hypertable namespace, using the native hypertable API.
	/// </summary>
	/// <seealso cref="ht4c::Common::Namespace"/>
	class HyperNamespace : public Common::Namespace {

		public:

			/// <summary>
			/// Creates a new HyperNamespace instance.
			/// </summary>
			/// <param name="client">Hypertable client</param>
			/// <param name="ns">Hypertable namespace</param>
			/// <returns>New HyperNamespace instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Namespace* create( Hypertable::ClientPtr client, Hypertable::NamespacePtr ns );

			/// <summary>
			/// Returns the reference to the underlying Hypertable namespace.
			/// </summary>
			/// <returns>Reference to the underlying Hypertable namespace</returns>
			inline Hypertable::Namespace* get() {
				return ns.get();
			}

			/// <summary>
			/// Destroys the HyperNamespace instance.
			/// </summary>
			virtual ~HyperNamespace( );

			#pragma region Common::Namespace methods

			virtual std::string getName( ) const;
			virtual void createTable( const char* name, const char* schema );
			virtual void createTableLike( const char* name, const char* like );
			virtual void alterTable( const char* name, const char* schema );
			virtual void renameTable( const char* nameOld, const char* nameNew );
			virtual Common::Table* openTable( const char* name, bool force );
			virtual void dropTable( const char* name, bool ifExist );
			virtual bool existsTable( const char* name );
			virtual std::string getTableSchema( const char* name, bool withIds = false );
			virtual void getListing( bool deep, ht4c::Common::NamespaceListing& nsListing );
			virtual void exec( const char* hql );
			virtual Common::Cells* query( const char* hql );

			#pragma endregion

		private:

			HyperNamespace( Hypertable::ClientPtr client, Hypertable::NamespacePtr ns );

			HyperNamespace( ) { }
			HyperNamespace( const HyperNamespace& ) { }
			HyperNamespace& operator = ( const HyperNamespace& ) { return *this; }

			static void getListing( const std::vector<Hypertable::NamespaceListing>& listing, ht4c::Common::NamespaceListing& nsListing );

			Hypertable::NamespacePtr ns;
			Hypertable::ClientPtr client;
	};

} }
