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

#include "Types.h"

#ifdef __cplusplus_cli
#pragma managed( push, off )
#endif

namespace ht4c { namespace Common {

	/// <summary>
	/// Represents a key builder, generates base85 encoded GUID's.
	/// </summary>
	class KeyBuilder {

		public:

			/// <summary>
			/// Defines GUID and key sizes.
			/// </summary>
			enum {
			  sizeGuid = 16
			, sizeKey  = 20
			};

			/// <summary>
			/// Initializes a new instance of the KeyBuilder class.
			/// </summary>
			/// <remarks>Generates a new GUID</remarks>
			KeyBuilder( );

			/// <summary>
			/// Initializes a new instance of the KeyBuilder class, using the specified GUID.
			/// </summary>
			/// <param name="key">GUID to encode</param>
			explicit KeyBuilder( const uint8_t key[sizeGuid] );

			/// <summary>
			/// Destroys the KeyBuilder instance.
			/// </summary>
			virtual ~KeyBuilder( ) { }

			/// <summary>
			/// Returns the base85 encoded GUID.
			/// </summary>
			/// <returns>Base85 encoded GUID</returns>
			inline operator const char* () const {
				return c_str();
			}

			/// <summary>
			/// Returns the base85 encoded GUID.
			/// </summary>
			/// <returns>Base85 encoded GUID</returns>
			inline const char* c_str() const {
				return buf;
			}

			/// <summary>
			/// Decodes a base85 encoded GUID.
			/// </summary>
			/// <param name="cp">Base85 encoded GUID</param>
			/// <param name="cp">Receives the decoded GUID</param>
			static void decode( const char cp[sizeKey], uint8_t key[sizeGuid] );

		private:

			KeyBuilder( const KeyBuilder& ) { }
			KeyBuilder& operator = ( const KeyBuilder& ) { return *this; }

			static void createKey( char* cp );
			static void encodeKey( const uint8_t key[sizeGuid], char cp[sizeKey] );
			static void initialize( );

			char buf[sizeKey + 1];
			static char intToBase85[85];
			static uint32_t base85ToInt[256];
			static bool initialized;
	};

} }


#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif