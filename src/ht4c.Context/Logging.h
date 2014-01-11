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

#include "LoggingSink.h"

namespace ht4c {

	/// <summary>
	/// Logging utility, redirects log events to a file and/or log callbacks.
	/// </summary>
	/// <remarks>Links between native and C++/CLI.</remarks>
	class Logging {

		public:

			/// <summary>
			/// Returns the log file name.
			/// </summary>
			/// <returns>Log file name</returns>
			static const std::string& getLogfile( );

			/// <summary>
			/// Sets the log file name.
			/// </summary>
			/// <param name="filename">Log file name</param>
			static void setLogfile( const char* filename );

			/// <summary>
			/// Adds a log event callback.
			/// </summary>
			/// <param name="loggingSink">Log event callback to add</param>
			static void addLoggingSink( LoggingSink* loggingSink );

			/// <summary>
			/// Removes a log event callback.
			/// </summary>
			/// <param name="loggingSink">Log event callback to remove</param>
			static void removeLoggingSink( LoggingSink* loggingSink );

			#ifndef __cplusplus_cli

			/// <summary>
			/// Initializes the logging.
			/// </summary>
			/// <remarks>Must be called before any other method.</remarks>
			/// <remarks>Pure native method.</remarks>
			static void init( );

			/// <summary>
			/// Shutdow the logging.
			/// </summary>
			/// <remarks>Pure native method.</remarks>
			static void shutdown( );

			#endif
	};

}


#ifdef __cplusplus_cli
#pragma managed( pop )
#endif