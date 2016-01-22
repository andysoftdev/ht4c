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
#pragma managed( push, off )
#endif

#include "Types.h"
#include "ContextKind.h"

namespace ht4c { namespace Common {

	class Namespace;

	/// <summary>
	/// Abstract class represents Hypertable client.
	/// </summary>
	class Client {

		public:

			/// <summary>
			/// Destroys the Client instance.
			/// </summary>
			virtual ~Client( ) { }

			/// <summary>
			/// Creates a new namespace.
			/// </summary>
			/// <param name="name">Namespace name</param>
			/// <param name="nsBase">Base namespace, might be NULL</param>
			/// <param name="createIntermediate">If true, all intermediate namespaces will be created if not exist</param>
			/// <param name="createIfNotExists">If true, namespaces will be created if not exist</param>
			/// <remarks>
			/// Use '/' separator character to separate namespace names. Optionally specify nsBase
			/// to create the new namespace relative to an existing namespace.
			/// </remarks>
			virtual void createNamespace( const char* name, Namespace* nsBase, bool createIntermediate, bool createIfNotExists ) = 0;

			/// <summary>
			/// Opens an existig namespace.
			/// </summary>
			/// <param name="name">Namespace name</param>
			/// <param name="nsBase">Base namespace, might be NULL</param>
			/// <returns>Opened namespace</returns>
			/// <remarks>
			/// Use '/' separator character to separate namespace names. Optionally specify nsBase
			/// if the namespace to open is relative to an existing namespace.
			/// </remarks>
			virtual Namespace* openNamespace( const char* name, Namespace* nsBase ) = 0;

			/// <summary>
			/// Drops a namespace.
			/// </summary>
			/// <param name="name">Namespace name</param>
			/// <param name="nsBase">Base namespace, might be NULL</param>
			/// <param name="ifExists">If true and the namespace does not exist the method won't fail</param>
			/// <param name="dropTables">If true the method drops all tables within the namespace(s)</param>
			/// <param name="deep">If true it drops all sub-namespaces</param>
			/// <remarks>
			/// Use '/' separator character to separate namespace names. Optionally specify nsBase
			/// if the namespace to drop is relative to an existing namespace.
			/// </remarks>
			virtual void dropNamespace( const char* name, Namespace* nsBase, bool ifExists, bool dropTables, bool deep ) = 0;

			/// <summary>
			/// Checks if a namespace exists.
			/// </summary>
			/// <param name="name">Namespace name</param>
			/// <param name="nsBase">Base namespace, might be NULL</param>
			/// <returns>true if the namespace exists</returns>
			/// <remarks>
			/// Use '/' separator character to separate namespace names. Optionally specify nsBase
			/// if the namespace to check is relative to an existing namespace.
			/// </remarks>
			virtual bool existsNamespace( const char* name, Namespace* nsBase ) = 0;

		protected:

			/// <summary>
			/// Creates a new Client instance.
			/// </summary>
			Client( ) { }

		private:

			Client( const Client& ) { }
			Client& operator = ( const Client& ) { return *this; }
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif