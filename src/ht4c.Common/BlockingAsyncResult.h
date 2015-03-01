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

#ifdef __cplusplus_cli
#pragma managed( push, off )
#endif

#include "Types.h"
#include "AsyncResult.h"

namespace ht4c { namespace Common {

	class AsyncResultSink;

	/// <summary>
	/// Abstract class represents results from asynchronous table scan operations.
	/// </summary>
	class BlockingAsyncResult : public AsyncResult {

		public:

			/// <summary>
			/// Returns true if all results have been consumed.
			/// </summary>
			virtual bool isEmpty( ) const = 0;

			/// <summary>
			/// Publish all cells to the asyncResultSink specified, blocks the calling thread
			/// till there is a result available unless asynchronous operations have completed or cancelled.
			/// </summary>
			/// <param name="asyncResultSink">Receives the cells available</param>
			/// <returns>true if all outstanding operations have been completed</returns>
			virtual bool getCells( AsyncResultSink* asyncResultSink ) = 0;

			/// <summary>
			/// Publish all cells to the asyncResultSink specified, blocks the calling thread
			/// till there is a result available unless asynchronous operations have completed, cancelled or
			/// a timeout occurs.
			/// </summary>
			/// <param name="asyncResultSink">Receives the cells available</param>
			/// <param name="timeoutMsec">Time out [ms]</param>
			/// <param name="timedOut">Set to true if the operation has been timed out</param>
			/// <returns>true if all outstanding operations have been completed</returns>
			virtual bool getCells( AsyncResultSink* asyncResultSink, uint32_t timeoutMsec, bool& timedOut ) = 0;

	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif