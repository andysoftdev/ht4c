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

#include "ht4c.Common/Client.h"

namespace ht4c { namespace SQLite {

	class Namespace;

	namespace Types {
		class Client;
	}

	/// <summary>
	/// Represents Hypertable client, using the sqlite API.
	/// </summary>
	/// <seealso cref="ht4c::Common::Client"/>
	class SQLiteClient : public Common::Client {

		public:

			/// <summary>
			/// Creates a new SQLiteClient instance.
			/// </summary>
			/// <param name="client">SQLite client</param>
			/// <returns>New SQLiteClient instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Common::Client* create( Db::ClientPtr client );

			/// <summary>
			/// Destroys the SQLiteClient instance.
			/// </summary>
			virtual ~SQLiteClient( );

			#pragma region Common::Client methods

			virtual void createNamespace( const char* name, Common::Namespace* nsBase, bool createIntermediate, bool createIfNotExists );
			virtual Common::Namespace* openNamespace( const char* name, Common::Namespace* nsBase );
			virtual void dropNamespace( const char* name, Common::Namespace* nsBase, bool ifExists, bool dropTables, bool deep );
			virtual bool existsNamespace( const char* name, Common::Namespace* nsBase );
			virtual void optimize( );

			#pragma endregion

		private:

			SQLiteClient( Db::ClientPtr client );

			std::string getNamespace( const char* name, Common::Namespace* nsBase );

			SQLiteClient( ) { }
			SQLiteClient( const SQLiteClient& ) { }
			SQLiteClient& operator = ( const SQLiteClient& ) { return *this; }

			Db::ClientPtr client;
	};

} }
