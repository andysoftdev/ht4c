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
#include "Exception.h"

namespace ht4c { namespace Common {

	HypertableException::HypertableException( const Hypertable::Exception& e, const std::string& msg )
	: std::runtime_error( msg )
	{
		eCode = e.code();
		eLine = e.line();
		if( e.func() ) eFunc = e.func();
		if( e.file() ) eFile = e.file();

		if( e.prev ) {
			std::stringstream ss;
			ss << *e.prev;
			eInner = new HypertableException( *e.prev, ss.str() );
		}
		else {
			eInner = 0;
		}
	}

	std::ostream& HypertableException::renderMessage( std::ostream &out, const Hypertable::Exception &e ) {
		std::stringstream ss;
		ss << e.message();
		std::string msg = ss.str();
		if( !msg.empty() ) {
			out << msg << " - ";
		}
		out <<  Hypertable::Error::get_text(e.code());
		if( e.func() && e.file() && e.line() ) {
			out << "\n\tat " << e.func() << " (" << e.file() << ':' << e.line() << ')';
		}

		int prev_code = e.code();

		for( Hypertable::Exception *prev = e.prev; prev; prev = prev->prev ) {
			out << "\n\tat " << (prev->func() ? prev->func() : "-") << " ("
				<< (prev->file() ? prev->file() : "-") <<':'<< prev->line() << "): "
				<< prev->message();

			if( prev->code() != prev_code ) {
				out << " - " << Hypertable::Error::get_text(prev->code());
				prev_code = prev->code();
			}
		}
		return out;
	}

	HypertableArgumentNullException::HypertableArgumentNullException( const std::string& arg, int l, const char *fn, const char *fl )
	: HypertableException( Hypertable::Error::EXTERNAL, arg, l, fn, fl )
	{
	}

	HypertableArgumentException::HypertableArgumentException( const std::string& msg, const std::string& arg, int l, const char *fn, const char *fl )
	: HypertableException( Hypertable::Error::EXTERNAL, msg, l, fn, fl )
	, eArg( arg )
	{
	}

} }