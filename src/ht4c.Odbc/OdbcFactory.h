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
#error compile native
#endif

#include "OdbcEnv.h"

namespace ht4c { namespace Odbc {

	class OdbcEnv;

	/// <summary>
	/// Represents the Hypertable Odbc environment configuration.
	/// </summary>
	struct OdbcEnvConfig {
		bool synchronous;
		bool indexColumn;
		bool indexColumnFamily;
		bool indexColumnQualifier;
		bool indexTimestamp;

		OdbcEnvConfig( )
			: indexColumn( false )
			, indexColumnFamily( false )
			, indexColumnQualifier( false )
			, indexTimestamp( false )
		{
		}
	};

	/// <summary>
	/// Represents Odbc environment factory.
	/// </summary>
	class OdbcFactory {

		public:

			/// <summary>
			/// Creates a new OdbcEnv instance.
			/// </summary>
			/// <param name="filename">The connection string</param>
			/// <param name="config">Database configuration</param>
			/// <returns>New OdbcEnv instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static OdbcEnv* create( const std::string &connectionString, const OdbcEnvConfig& config );
	};

} }
