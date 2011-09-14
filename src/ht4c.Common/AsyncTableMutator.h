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

#include "TableMutator.h"

namespace ht4c { namespace Common {

	class Cells;

	/// <summary>
	/// Abstract class represents an asynchronous table mutator.
	/// </summary>
	class AsyncTableMutator : public TableMutator {

		public:

			/// <summary>
			/// Destroys the TableMutator instance.
			/// </summary>
			virtual ~AsyncTableMutator( ) { }

			/// <summary>
			/// Returns the mutator identifier.
			/// </summary>
			/// <returns>Asynchronous table mutator identifier</returns>
			virtual int64_t id( ) const = 0;

		protected:

			/// <summary>
			/// Creates a new TableMutator instance.
			/// </summary>
			AsyncTableMutator( ) { }

		private:

			AsyncTableMutator( const AsyncTableMutator& ) { }
			AsyncTableMutator& operator = ( const AsyncTableMutator& ) { return *this; }
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif