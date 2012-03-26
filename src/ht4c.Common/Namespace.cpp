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
#include "Namespace.h"

#define HT4C_THROW( error, msg ) \
	throw Hypertable::Exception( error, msg, __LINE__, __FUNCTION__, __FILE__ );

namespace ht4c { namespace Common { 

	void Namespace::validateTableName( const char* name ) {
		if( name && *name ) {
			Hypertable::String basename = Hypertable::Filesystem::basename( name );
			if( basename.size() && basename[0] != '^' ) {
				return;
			}
		}

		HT4C_THROW( Hypertable::Error::SYNTAX_ERROR, "Invalid table name" );
	}

} }