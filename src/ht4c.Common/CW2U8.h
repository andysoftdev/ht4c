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

#include "Exception.h"

namespace ht4c { namespace Common {

	/// <summary>
	/// Converts a wide-char string into a utf8 string.
	/// </summary>
	class CW2U8 {

		public:

			/// <summary>
			/// Initializes a new instance of the CW2U8 class using a wide-char string.
			/// </summary>
			/// <param name="string">Wide char string.</param>
			inline explicit CW2U8( const wchar_t* wsz ) {
				if( wsz ) {
					cstr = ToUtf8( wsz, wcslen(wsz) );
				}
				else {
					cstr = 0;
				}
			}

			/// <summary>
			/// Destroys the CW2U8 instance.
			/// </summary>
			inline ~CW2U8( ) {
				if( cstr && cstr != cbuf ) {
					free( cstr );
				}
			}

			/// <summary>
			/// Gets the utf8 C string.
			/// </summary>
			/// <returns>The utf8 C string.</returns>
			inline operator const char* ( ) const {
				return c_str();
			}

			/// <summary>
			/// Gets the utf8 C string.
			/// </summary>
			/// <returns>The utf8 C string.</returns>
			inline const char* c_str( ) const {
				return cstr;
			}

		private:

			enum {
				SIZE = 64
			};

			char* ToUtf8( const wchar_t* wsz, int len ) {
				if( len ) {
					int cb = len < SIZE ? WideCharToMultiByte(CP_UTF8, 0, wsz, len, cbuf, SIZE, 0, 0) : 0;
					if( !cb ) {
						cb = WideCharToMultiByte( CP_UTF8, 0, wsz, len, 0, 0, 0, 0 );
						char* sz = static_cast<char*>( malloc(cb + 1) );
						if( !sz ) {
							throw std::bad_alloc();
						}
						cb = WideCharToMultiByte( CP_UTF8, 0, wsz, len, sz, cb, 0, 0);
						if( !cb ) {
							free( sz );
							throw HypertableException( ::GetLastError(), "MultiByteToWideChar" );
						}
						sz[cb] = 0;
						return sz;
					}
					else {
						cbuf[cb] = 0;
					}
				}
				else {
					*cbuf = 0;
				}
				return cbuf;
			}

			char cbuf[SIZE + 1];
			char* cstr;
	};


} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif