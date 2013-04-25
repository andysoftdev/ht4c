/** -*- C++ -*-
 * Copyright (C) 2010-2013 Thalmann Software & Consulting, http://www.softdev.ch
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

#pragma warning( pop )

namespace {

	class LogAppender : public Hypertable::Logger::LogSink
	{
		public:

			static LogAppender& instance() {
				return logAppender;
			}

			virtual ~LogAppender( ) {
				close();
			}

			const std::string& getLogfile( ) const {
				return fn;
			}

			void setLogfile( const char* filename ) {
				close();
				if( filename && *filename ) {
					file = fopen( filename, "w+tc" );
					if( file ) {
						Hypertable::Logger::get()->set_file( file );
						fn = filename;
					}
				}
			}

			void addLoggingSink( ht4c::LoggingSink* loggingSink ) {
				if( loggingSink ) {
					loggingSinks.insert( loggingSink );
				}
			}

			void removeLoggingSink( ht4c::LoggingSink* loggingSink ) {
				loggingSinks.erase( loggingSink );
			}

			void removeAllLogsinks( ) {
				loggingSinks.clear();
			}

			void close( ) {
				if( file ) {
					Hypertable::Logger::get()->close();
					file = 0;
				}
				fn.clear();
			}

		private:

			typedef std::set<ht4c::LoggingSink*> sinks_t;

			LogAppender( )
				: file( 0 )
			{
			}

			LogAppender( const LogAppender& );
			LogAppender& operator= ( const LogAppender& );

			virtual void log(int priority, const std::string& message, const std::string& entry) const {
				for each( ht4c::LoggingSink* ls in loggingSinks ) {
					ls->logEvent( priority, message );
					ls->logMessage( entry );
				}
			}

			static LogAppender logAppender;
			std::string fn;
			FILE* file;
			sinks_t loggingSinks;
	};

	LogAppender LogAppender::logAppender;
	static bool initialized = false;

}

namespace ht4c {
	using namespace Hypertable;

	const std::string& Logging::getLogfile( ) {
		return LogAppender::instance().getLogfile();
	}

	void Logging::setLogfile( const char* filename ) {
		LogAppender::instance().setLogfile( filename );
	}

	void Logging::addLoggingSink( LoggingSink* loggingSink ) {
		LogAppender::instance().addLoggingSink( loggingSink );
	}

	void Logging::removeLoggingSink( LoggingSink* loggingSink ) {
		LogAppender::instance().removeLoggingSink( loggingSink );
	}

	void Logging::init( ) {
		if( !initialized ) {
			Logger::initialize( System::exe_name );
			Logger::get()->set_level( Logger::Priority::ERROR );
			Logger::get()->add_sink(&LogAppender::instance());

			initialized = true;
		}
	}

	void Logging::shutdown( ) {
		if( initialized ) {
			LogAppender::instance().removeAllLogsinks();
			Logger::get()->remove_sink(&LogAppender::instance());
			LogAppender::instance().close();
		}
	}

}
