/** -*- C++ -*-
 * Copyright (C) 2010-2015 Thalmann Software & Consulting, http://www.softdev.ch
 *
 * This file is part of ht4n.
 *
 * ht4n is free software; you can redistribute it and/or
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
	/// Declares extended context features, apart from the regular features.
	/// </summary>
	enum ContextFeature {

		/// <summary>
		/// Unknown provider feature.
		/// </summary>
		CF_Unknown

		/// <summary>
		/// Hypertable query language (HQL).
		/// </summary>
	, CF_HQL

		/// <summary>
		/// Asynchronous table mutator.
		/// </summary>
	, CF_AsyncTableMutator

		/// <summary>
		/// Periodic flush mutator.
		/// </summary>
	, CF_PeriodicFlushTableMutator

		/// <summary>
		/// Asynchronous table scanner.
		/// </summary>
	, CF_AsyncTableScanner

		/// <summary>
		/// Notify session state transition.
		/// </summary>
	, CF_NotifySessionStateChanged

		/// <summary>
		/// Counter columns.
		/// </summary>
	, CF_CounterColumn
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif