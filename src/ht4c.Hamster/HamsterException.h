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

#include "ht4c.Common/Exception.h"

/// <summary>
/// Defines the catch clause of ht4c hamster try/catch blocks.
/// </summary>
/// <remarks>
/// Translates hamster exceptions into ht4c exceptions.
/// </remarks>
#define HT4C_HAMSTER_RETHROW \
	catch( hamsterdb::error& e ) { \
		std::stringstream ss; \
		ss << e.get_string() << "\n\tat " << __FUNCTION__ << " (" << __FILE__ << ':' << __LINE__ << ')'; \
		throw ht4c::Common::HypertableException( Hypertable::Error::EXTERNAL, ss.str(), __LINE__, __FUNCTION__, __FILE__ ); \
	} \
	HT4C_RETHROW

/// <summary>
/// Throws a hypertable exception.
/// </summary>
#define HT4C_HAMSTER_THROW( error, msg ) \
			throw Hypertable::Exception( error, msg, __LINE__, __FUNCTION__, __FILE__ );
