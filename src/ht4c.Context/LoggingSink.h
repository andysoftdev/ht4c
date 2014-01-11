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
#pragma managed( push, off )
#endif

#include <string>

namespace ht4c {

	/// <summary>
	/// Abstract class represents a callback for log events.
	/// </summary>
	class LoggingSink {

		public:

			/// <summary>
			/// Destroys the LoggingSink instance.
			/// </summary>
			virtual ~LoggingSink( ) { }

			/// <summary>
			/// Gets called if a log event occurs.
			/// </summary>
			/// <param name="priority">Log event priority</param>
			/// <param name="message">Log event message</param>
			virtual void logEvent( int priority, const std::string& message ) = 0;

			/// <summary>
			/// Gets called if a log event occurs.
			/// </summary>
			/// <param name="message">Log event message</param>
			virtual void logMessage( const std::string& message ) = 0;
	};

}


#ifdef __cplusplus_cli
#pragma managed( pop )
#endif