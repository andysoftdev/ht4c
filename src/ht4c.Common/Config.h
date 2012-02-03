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
#pragma managed( push, off )
#endif

namespace ht4c { namespace Common {

	/// <summary>
	/// Represents various configuration and connection string options.
	/// </summary>
	class Config {

		public:

			/// <summary>
			/// Provider name.
			/// </summary>
			static const char* ProviderName;

			/// <summary>
			/// Provider name alias.
			/// </summary>
			static const char* ProviderNameAlias;

			/// <summary>
			/// Provider name value - Hyper.
			/// </summary>
			static const char* ProviderHyper;

			/// <summary>
			/// Provider name value - Thrift.
			/// </summary>
			static const char* ProviderThrift;;

			/// <summary>
			/// Uri, hostname:port.
			/// </summary>
			static const char* Uri;

			/// <summary>
			/// Uri alias.
			/// </summary>
			static const char* UriAlias;

			/// <summary>
			/// Composable part catalog (only used for MEF composition).
			/// </summary>
			static const char* ComposablePartCatalogs;

	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif