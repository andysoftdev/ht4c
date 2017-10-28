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

namespace ht4c { namespace Common {

	/// <summary>
	/// Specifies possible context values. The context kind defines the
	/// provider API to use for any operation within the context.
	/// </summary>
	enum ContextKind {

		/// <summary>
		/// Unknown context kind.
		/// </summary>
	  CK_Unknown = 0

#ifdef SUPPORT_HYPERTABLE

		/// <summary>
		/// Hypertable native protocol context kind.
		/// </summary>
	, CK_Hyper

#endif

#ifdef SUPPORT_HYPERTABLE_THRIFT

		/// <summary>
		/// Hypertable thrift API context kind.
		/// </summary>
	, CK_Thrift

#endif

#ifdef SUPPORT_HAMSTERDB

		/// <summary>
		/// Hypertable hamster API context kind.
		/// </summary>
	, CK_Hamster

#endif

#ifdef SUPPORT_SQLITEDB

		/// <summary>
		/// Hypertable SQLite API context kind.
		/// </summary>
	, CK_SQLite

#endif

#ifdef SUPPORT_ODBC

	/// <summary>
	/// Hypertable ODBC API context kind.
	/// </summary>
	, CK_ODBC

#endif

		/// <summary>
		/// Terminator.
		/// </summary>
	, CK_Last 
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif