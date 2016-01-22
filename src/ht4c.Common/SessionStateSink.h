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

#include "SessionState.h"

namespace ht4c { namespace Common {

	/// <summary>
	/// Abstract class represents a callback for Hypertable session state transition.
	/// </summary>
	class SessionStateSink {

		public:

			/// <summary>
			/// Destroys the SessionStateSink instance.
			/// </summary>
			virtual ~SessionStateSink( ) { }

			/// <summary>
			/// Gets called if a Hypertable session state transition occured.
			/// </summary>
			/// <param name="oldSessionState">The old session state</param>
			/// <param name="newSessionState">The new session state</param>
			virtual void stateTransition( SessionState oldSessionState, SessionState newSessionState ) = 0;
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif