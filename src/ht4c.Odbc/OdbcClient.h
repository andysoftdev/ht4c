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

namespace ht4c { namespace Odbc {

	class Namespace;

	namespace Types {
		class Client;
	}

	/// <summary>
	/// Represents Hypertable client, using the Odbc API.
	/// </summary>
	/// <seealso cref="ht4c::Common::Client"/>
	class OdbcClient : public Common::Client {

		public:

			/// <summary>
			/// Creates a new OdbcClient instance.
			/// </summary>
			/// <param name="client">Odbc client</param>
			/// <returns>New OdbcClient instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Common::Client* create( Db::ClientPtr client );

			/// <summary>
			/// Destroys the OdbcClient instance.
			/// </summary>
			virtual ~OdbcClient( );

			#pragma region Common::Client methods

			virtual void createNamespace( const char* name, Common::Namespace* nsBase, bool createIntermediate, bool createIfNotExists );
			virtual Common::Namespace* openNamespace( const char* name, Common::Namespace* nsBase );
			virtual void dropNamespace( const char* name, Common::Namespace* nsBase, bool ifExists, bool dropTables, bool deep );
			virtual bool existsNamespace( const char* name, Common::Namespace* nsBase );

			#pragma endregion

		private:

			OdbcClient( Db::ClientPtr client );

			std::string getNamespace( const char* name, Common::Namespace* nsBase );

			OdbcClient( ) { }
			OdbcClient( const OdbcClient& ) { }
			OdbcClient& operator = ( const OdbcClient& ) { return *this; }

			Db::ClientPtr client;
	};

} }
