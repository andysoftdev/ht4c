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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "stdafx.h"
#include "Cells.h"
#include "Exception.h"

namespace ht4c { namespace Common {

	Cells* Cells::create( int reserve ) {
		HT4C_TRY {
			return new Cells( reserve );
		}
		HT4C_RETHROW
	}

	Cells::Cells( Hypertable::Cells* _cells )
	: cellsBuilder( )
	, cells( _cells )
	, initialCapacity( 0 )
	{
	}

	Cells::Cells( int reserve )
	: cellsBuilder( new Hypertable::CellsBuilder(reserve) )
	, cells( &cellsBuilder->get() )
	, initialCapacity( reserve )
	{
	}

	Cells::~Cells( )
	{
		cellsBuilder = 0;
		cells = 0;
	}

	void Cells::add( const Hypertable::Cell& _cell ) {
		Cell cell;
		cell.get() = _cell;
		cellsBuilder->add( cell.get(), true );
	}

	size_t Cells::size( ) const {
		return cells->size();
	}

	void Cells::add( const char* row, const char* columnFamily, const char* columnQualifier, uint64_t timestamp, const void* value, uint32_t valueLength, uint8_t flag ) {
		flag = FLAG( columnFamily, columnQualifier, flag );
		Cell cell( row
						 , CF(columnFamily)
						 , columnQualifier
						 , TIMESTAMP(timestamp, flag)
						 , value
						 , valueLength
						 , flag);

		cellsBuilder->add( cell.get(), true );
	}

	bool Cells::get( size_t n, Cell* cell ) const {
		if( cell && n >= 0 && n < cells->size() ) {
			cell->get() = (*cells)[n];
			return true;
		}
		return false;
	}

	void Cells::get_unchecked( size_t n, Cell* cell ) const {
		cell->get() = (*cells)[n];
	}

	void Cells::clear( ) {
		cellsBuilder = std::make_shared<Hypertable::CellsBuilder>( initialCapacity );
		cells = &cellsBuilder->get();
	}

} }