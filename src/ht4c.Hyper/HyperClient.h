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

#include "ht4c.Common/Client.h"

#pragma warning( push, 3 )

#include "Common/Properties.h"
#include "AsyncComm/ConnectionManager.h"
#include "Hyperspace/Session.h"

#pragma warning( pop )

namespace ht4c { namespace Common {
	class Namespace;
} }

namespace ht4c { namespace Hyper {

	/// <summary>
	/// Represents Hypertable client, using the native hypertable API.
	/// </summary>
	/// <seealso cref="ht4c::Common::Client"/>
	class HyperClient : public Common::Client {

		public:

			/// <summary>
			/// Creates a new HyperClient instance.
			/// </summary>
			/// <param name="connMngr">Connection manager</param>
			/// <param name="session">Hyperspace session</param>
			/// <param name="properties">Configuraton properties</param>
			/// <returns>New HyperClient instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Common::Client* create( Hypertable::ConnectionManagerPtr connMngr, Hyperspace::SessionPtr session, Hypertable::PropertiesPtr properties );

			/// <summary>
			/// Destroys the HyperClient instance.
			/// </summary>
			virtual ~HyperClient( );

			#pragma region Common::Client methods

			virtual void createNamespace( const char* name, Common::Namespace* nsBase, bool createIntermediate, bool createIfNotExists );
			virtual Common::Namespace* openNamespace( const char* name, Common::Namespace* nsBase );
			virtual void dropNamespace( const char* name, Common::Namespace* nsBase, bool ifExists, bool dropTables, bool deep );
			virtual bool existsNamespace( const char* name, Common::Namespace* nsBase );

			#pragma endregion

		private:

			HyperClient( Hypertable::ConnectionManagerPtr connMngr, Hyperspace::SessionPtr session, Hypertable::PropertiesPtr properties );

			Hypertable::Namespace* getNamespace( Common::Namespace* ns );
			void drop( Hypertable::NamespacePtr ns, const std::vector<Hypertable::NamespaceListing>& listing, bool ifExists, bool dropTables );

			HyperClient( ) { }
			HyperClient( const HyperClient& ) { }
			HyperClient& operator = ( const HyperClient& ) { return *this; }

			Hypertable::ClientPtr client;
	};

} }
