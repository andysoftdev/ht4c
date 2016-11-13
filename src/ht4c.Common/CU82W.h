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
	/// Converts a utf8 string into a wide-char string.
	/// </summary>
	class CU82W {

		public:

			/// <summary>
			/// Initializes a new instance of the CU82W class using a managed string.
			/// </summary>
			/// <param name="string">The utf8  string.</param>
			inline explicit CU82W( const char* sz ) {
				if( sz ) {
					cstr = ToWideChar( sz, strlen(sz) );
				}
				else {
					cstr = 0;
				}
			}

			/// <summary>
			/// Destroys the CU82W instance.
			/// </summary>
			inline ~CU82W( ) {
				if( cstr && cstr != wbuf ) {
					free( cstr );
				}
			}

			/// <summary>
			/// Gets the wide-char C string.
			/// </summary>
			/// <returns>The wide-char C string.</returns>
			inline operator const wchar_t* ( ) const {
				return c_str();
			}

			/// <summary>
			/// Gets the wide-char C string.
			/// </summary>
			/// <returns>The wide-char C string.</returns>
			inline const wchar_t* c_str( ) const {
				return cstr;
			}

		private:

			enum {
				SIZE = 64
			};

			wchar_t* ToWideChar( const char* sz, int len ) {
				if( len ) {
					int cc = len < SIZE ? MultiByteToWideChar(CP_UTF8, 0, sz, len, wbuf, SIZE) : 0;
					if( !cc ) {
						cc = MultiByteToWideChar( CP_UTF8, 0, sz, len, 0, 0 );
						wchar_t* wsz = static_cast<wchar_t*>( malloc((cc + 1) * sizeof(wchar_t)) );
						if( !wsz ) {
							throw std::bad_alloc();
						}
						cc = MultiByteToWideChar( CP_UTF8, 0, sz, len, wsz, cc );
						if( !cc ) {
							free( wsz );
							throw HypertableException( ::GetLastError(), "MultiByteToWideChar" );
						}

						wsz[cc] = 0;
						return wsz;
					}
					else {
						wbuf[cc] = 0;
					}
				}
				else {
					*wbuf = 0;
				}
				return wbuf;
			}

			wchar_t wbuf[SIZE + 1];
			wchar_t* cstr;
	};


} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif