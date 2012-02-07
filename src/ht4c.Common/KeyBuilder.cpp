/** -*- C++ -*-
 * Copyright (C) 2010-2012 Thalmann Software & Consulting, http://www.softdev.ch
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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "stdafx.h"
#include "KeyBuilder.h"
#include <Objbase.h>

#pragma comment( lib, "Ole32.lib" )

namespace ht4c { namespace Common {

	// base85 encoding, see also http://code.google.com/p/stringencoders
	char KeyBuilder::intToBase85[85];
	uint32_t KeyBuilder::base85ToInt[256];
	bool KeyBuilder::initialized = false;

	KeyBuilder::KeyBuilder( ) {
		createKey( buf );
	}

	KeyBuilder::KeyBuilder( const uint8_t key[sizeGuid] ) {
		encodeKey( (const uint8_t*)key, buf );
	}

	void KeyBuilder::decode( const char* cp, uint8_t key[sizeGuid] ) {
		if( !initialized ) {
			initialize();
		}
		uint32_t* pk = (uint32_t*)key;
		for( int i = 0; i < sizeGuid / 4; ++i ) {
			uint32_t v = 0;
			for( int j = 0; j < 5; ++j ) {
				uint32_t digit = base85ToInt[(uint32_t) *cp++];
				v = v * 85 + digit;
			}
			*pk++ = htonl( v );
		}
	}

	void KeyBuilder::createKey( char cp[sizeKey] ) {
		GUID guid;
		(void)::CoCreateGuid( &guid );
		encodeKey( (uint8_t*)&guid, cp );
	}

	void KeyBuilder::encodeKey( const uint8_t key[sizeGuid], char cp[sizeKey] ) {
		if( !initialized ) {
			initialize();
		}

		const uint32_t* pk = (const uint32_t*) key;
		for( int i = 0; i < sizeGuid / 4; ++i ) {
			uint32_t v = htonl( *pk++ );
			*(cp+4) = intToBase85[v % 85]; v /= 85;
			*(cp+3) = intToBase85[v % 85]; v /= 85;
			*(cp+2) = intToBase85[v % 85]; v /= 85;
			*(cp+1) = intToBase85[v % 85]; v /= 85;
			*cp = intToBase85[v];
			cp += 5;
		}
		*cp = 0;
	}

	void KeyBuilder::initialize( ) {
		int i = 0;
		int j = 0;
		for( i = 0 ; i < 256 ; i++ ) {
			base85ToInt[i] = 99;
		}

		// i < 33 or '!' is unprintable, 127 is an unprintable character
		for( i = '!', j = 0; j < 85 && i < 127; ++i ) {

			// You can have 8 restrictions in the following line.
			// Traditional postscript removes: ';', '&', '\', '"'
			// so that 'last' character is 'y' ('z' is special)
			// For web/cookie applications, I recommend those plus ','

			if( i == ';' || i == '&' || i == '\\' || i == '"' || i == ',' ) {
				continue;
			}
			intToBase85[j] = (char)i;
			base85ToInt[i] = (uint32_t)j;
			++j;
		}
		initialized = true;
	}


} }