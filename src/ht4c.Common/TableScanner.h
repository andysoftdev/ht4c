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

	class Cell;

	/// <summary>
	/// Abstract class represents a synchronous table scanner.
	/// </summary>
	class TableScanner {

		public:

			/// <summary>
			/// Destroys the TableScanner instance.
			/// </summary>
			virtual ~TableScanner( ) { }

			/// <summary>
			/// Returns the next cell.
			/// </summary>
			/// <param name="cell">Receives the cell</param>
			/// <returns>true if succeeded, false if the scan has been completed</returns>
			virtual bool next( Cell*& cell ) = 0;

		protected:

			/// <summary>
			/// Creates a new TableScanner instance.
			/// </summary>
			TableScanner( ) { }

		private:

			TableScanner( const TableScanner& ) { }
			TableScanner& operator = ( const TableScanner& ) { return *this; }
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif