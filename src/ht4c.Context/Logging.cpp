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

#include "Logging.h"

#pragma warning( push, 3 )

#include "Common/Logger.h"
#include <log4cpp/LayoutAppender.hh>

#pragma warning( pop )

namespace {

	typedef std::set<ht4c::LoggingSink*> sinks_t;

	class logbuf : public std::streambuf
	{
		public:

			logbuf( )
				: fs( 0 )
				, ls( )
			{
				setp( buf, buf + size_t(size - 1) );
			}

			void setLogfile( std::fstream* _fs ) {
				fs = _fs;
			}

			void addSink( ht4c::LoggingSink* _ls ) {
				ls.insert( _ls );
			}

			void removeSink( ht4c::LoggingSink* _ls ) {
				ls.erase( _ls );
			}

			void removeAllSinks( ) {
				ls.clear( );
			}

			void append( const ::log4cpp::LoggingEvent& event ) {
				for each( ht4c::LoggingSink* _ls in ls ) {
					_ls->logEvent( event.priority, event.message );
				}
			}

		protected:

			void publish( bool flush ) {
				std::ptrdiff_t n = pptr() - pbase();
				pbump( -n );
				if( fs ) {
					fs->write( pbase(), n );
					if( flush ) {
						fs->flush();
					}
				}
				if( ls.size() ) {
					std::string message( pbase(), n );
					for each( ht4c::LoggingSink* _ls in ls ) {
						_ls->logMessage( message );
					}
				}
			}

		private:

			enum {
				size = 256
			};

			logbuf( const logbuf& );
			logbuf& operator= ( const logbuf& );

			int_type overflow( int_type ch ) {
				if( ch != traits_type::eof() ) {
					*pptr() = ch;
					pbump( 1 );
					publish( false );
				}
				return ch;
			}

			int sync( ) {
				publish( true );
				return 0;
			}

			char buf[size];
			std::fstream* fs;
			sinks_t ls;
	};

	class logstream : public std::ostream
	{
		public:

			logstream( )
				: std::ostream( &lb )
				, fs( 0 )
			{
			}

			virtual ~logstream( ) {
				if( fs ) {
					delete fs;
				}
			}

			const std::string& getLogfile( ) const {
				return fn;
			}

			void setLogfile( const char* filename ) {
				fn.clear();
				if( fs ) {
					delete fs;
					fs = 0;
				}
				if( filename && *filename ) {
					fs = new std::fstream( filename, std::ios_base::app, _SH_DENYNO );
					fn = filename;
				}
				lb.setLogfile( fs );
			}

			void addLoggingSink( ht4c::LoggingSink* loggingSink ) {
				lb.addSink( loggingSink );
			}

			void removeLoggingSink( ht4c::LoggingSink* loggingSink ) {
				lb.removeSink( loggingSink );
			}

			void removeAllLogsinks( ) {
				lb.removeAllSinks( );
			}

			void append( const ::log4cpp::LoggingEvent& event ) {
				lb.append( event );
			}

		private:

			logstream( const logstream& );
			logstream& operator= ( const logstream& );

			std::string fn;
			std::fstream* fs;
			logbuf lb;
	};

	class logappender : public ::log4cpp::LayoutAppender
	{
		public:

			logappender( logstream* _ls )
				: LayoutAppender( "ht4c::LoggingSink" )
				, ls( _ls )
			{
			}

			virtual ~logappender( ) {
				ls = 0;
			}

			virtual void close() {
				ls = 0;
			}

		protected:

			virtual void _append( const ::log4cpp::LoggingEvent& event ) {
				if( ls ) {
					ls->append( event );
				}
			}

		private:

			logstream* ls;
	};

	static logstream ls;
	static bool initialized = false;

}

namespace ht4c {
	using namespace Hypertable;

	const std::string& Logging::getLogfile( ) {
		return ls.getLogfile();
	}

	void Logging::setLogfile( const char* filename ) {
		ls.setLogfile( filename );
	}

	void Logging::addLoggingSink( LoggingSink* loggingSink ) {
		ls.addLoggingSink( loggingSink );
	}

	void Logging::removeLoggingSink( LoggingSink* loggingSink ) {
		ls.removeLoggingSink( loggingSink );
	}

	void Logging::init( ) {
		if( !initialized ) {
			Logger::initialize( System::exe_name
												, Logger::logger
												? Logger::logger->getPriority()
												: Logger::Priority::FATAL
												, true
												, ls);

			if( Logger::logger ) {
				Logger::logger->addAppender( new logappender(&ls) );
			}

			initialized = true;
		}
	}

	void Logging::shutdown( ) {
		if( initialized ) {
			ls.removeAllLogsinks();
		}
	}

}
