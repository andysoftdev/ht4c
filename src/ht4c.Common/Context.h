/** -*- C++ -*-
 * Copyright (C) 2010-2013 Thalmann Software & Consulting, http://www.softdev.ch
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

#include "ContextKind.h"
#include "ContextFeature.h"

namespace ht4c { namespace Common {
	class Client;
	class Properties;
	class SessionStateSink;

	/// <summary>
	/// Abstract class represents a Hypertable context, handles connection to a Hypertable instance.
	/// </summary>
	class Context {

		public:

			/// <summary>
			/// Destroys the Context instance.
			/// </summary>
			virtual ~Context( ) { }

			/// <summary>
			/// Creates a new Hypertable client.
			/// </summary>
			/// <returns>New Client instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			virtual Common::Client* createClient( ) = 0;

			/// <summary>
			/// Returns all configuration parameters.
			/// </summary>
			/// <param name="properties">Receives the configuration parameters</param>
			/// <seealso cref="ht4c::Common::Properties"/>
			virtual void getProperties( Properties& properties ) const = 0;

			/// <summary>
			/// Returns true if the actual provider supports the feature specified, otherwise false.
			/// </summary>
			/// <param name="contextFeature">ContextFeature feature</param>
			/// <returns>true if the actual provider supports the feature specified, otherwise false.</returns>
			/// <seealso cref="ht4c::Common::ContextFeature"/>
			virtual bool hasFeature( ContextFeature contextFeature ) const = 0;

			/// <summary>
			/// Adds a session state transition callback.
			/// </summary>
			/// <param name="SessionStateSink">Log event callback to add</param>
			virtual void addSessionStateSink( SessionStateSink* SessionStateSink ) = 0;

			/// <summary>
			/// Removes a session state transition callback.
			/// </summary>
			/// <param name="SessionStateSink">Log event callback to remove</param>
			virtual void removeSessionStateSink( SessionStateSink* SessionStateSink ) = 0;
	};

} }

#ifdef __cplusplus_cli
#pragma managed( pop )
#endif