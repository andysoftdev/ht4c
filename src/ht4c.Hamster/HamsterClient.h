/** -*- C++ -*-
 * Copyright (C) 2011 Andy Thalmann
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

#include "ht4c.Common/Client.h"

namespace ht4c { namespace Hamster {

	class Namespace;

	namespace Types {
		class Client;
	}

	/// <summary>
	/// Represents Hypertable client, using the hamster API.
	/// </summary>
	/// <seealso cref="ht4c::Common::Client"/>
	class HamsterClient : public Common::Client {

		public:

			/// <summary>
			/// Creates a new HamsterClient instance.
			/// </summary>
			/// <param name="client">Hamster client</param>
			/// <returns>New HamsterClient instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Common::Client* create( Db::ClientPtr client );

			/// <summary>
			/// Destroys the HamsterClient instance.
			/// </summary>
			virtual ~HamsterClient( );

			#pragma region Common::Client methods

			virtual void createNamespace( const char* name, Common::Namespace* nsBase, bool createIntermediate, bool createIfNotExists );
			virtual Common::Namespace* openNamespace( const char* name, Common::Namespace* nsBase );
			virtual void dropNamespace( const char* name, Common::Namespace* nsBase, bool ifExists, bool dropTables, bool deep );
			virtual bool existsNamespace( const char* name, Common::Namespace* nsBase );

			#pragma endregion

		private:

			HamsterClient( Db::ClientPtr client );

			std::string getNamespace( const char* name, Common::Namespace* nsBase );

			HamsterClient( ) { }
			HamsterClient( const HamsterClient& ) { }
			HamsterClient& operator = ( const HamsterClient& ) { return *this; }

			Db::ClientPtr client;
	};

} }
