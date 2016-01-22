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

#include "HamsterEnv.h"

namespace ht4c { namespace Hamster {

	class HamsterEnv;

	/// <summary>
	/// Represents the Hypertable hamster environment configuration.
	/// </summary>
	struct HamsterEnvConfig {
		bool enableRecovery;
		bool enableAutoRecovery;
		int cacheSizeMB;
		int pageSizeKB;

		HamsterEnvConfig( )
			: enableRecovery( false )
			, enableAutoRecovery( false )
			, cacheSizeMB( 64 )
			, pageSizeKB( 64 )
		{
		}
	};

	/// <summary>
	/// Represents hamster environment factory.
	/// </summary>
	class HamsterFactory {

		public:

			/// <summary>
			/// Creates a new HamsterEnv instance.
			/// </summary>
			/// <param name="filename">Database filename</param>
			/// <param name="config">Database configuration</param>
			/// <returns>New HamsterEnv instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static HamsterEnv* create( const std::string &filename, const HamsterEnvConfig& config );
	};

} }
