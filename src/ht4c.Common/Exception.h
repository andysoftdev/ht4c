/** -*- C++ -*-
 * Copyright (C) 2010-2012 Thalmann Software & Consulting, http://www.softdev.ch
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
#else

/// <summary>
/// Defines the start of ht4c try/catch block.
/// </summary>
#define HT4C_TRY \
	try

/// <summary>
/// Defines the catch clause of ht4c try/catch blocks.
/// </summary>
/// <remarks>Translates Hypertable and std exceptions into ht4c exceptions.</remarks>
#define HT4C_RETHROW \
	catch( Hypertable::Exception& e ) { \
		std::stringstream ss; \
		ht4c::Common::HypertableException::renderMessage( ss, e ); \
		throw ht4c::Common::HypertableException( e, ss.str() ); \
	} \
	catch( std::exception& e ) { \
		std::stringstream ss; \
		ss << e.what() << "\n\tat " << __FUNCTION__ << " (" << __FILE__ << ':' << __LINE__ << ')'; \
		throw ht4c::Common::HypertableException( Hypertable::Error::EXTERNAL, ss.str(), __LINE__, __FUNCTION__, __FILE__ ); \
	} \
	catch( ... ) { \
		std::stringstream ss; \
		ss << "Caught unknown exception\n\tat " << __FUNCTION__ << " (" << __FILE__ << ':' << __LINE__ << ')'; \
		throw ht4c::Common::HypertableException( Hypertable::Error::EXTERNAL, ss.str(), __LINE__, __FUNCTION__, __FILE__ ); \
	}

/// <summary>
/// Throws a Hypertable 'NotImplemented exception.
/// </summary>
#define HT4C_THROW_NOTIMPLEMENTED() \
	throw ht4c::Common::HypertableException( Hypertable::Error::NOT_IMPLEMENTED, __FUNCTION__ " not implemented", __LINE__, __FUNCTION__, __FILE__ );

/// <summary>
/// Throws an ArgumentNull exception.
/// </summary>
#define HT4C_THROW_ARGUMENTNULL( arg ) \
	throw ht4c::Common::HypertableArgumentNullException( arg, __LINE__, __FUNCTION__, __FILE__ );

/// <summary>
/// Throws an Argument exception.
/// </summary>
#define HT4C_THROW_ARGUMENT( mesg, arg ) \
	throw ht4c::Common::HypertableArgumentException( msg, arg, __LINE__, __FUNCTION__, __FILE__ );

#endif

#include "Types.h"

namespace ht4c { namespace Common {

	/// <summary>
	/// Represents a ht4c hypertable exception.
	/// </summary>
	/// <remarks>Links between native and C++/CLI.</remarks>
	class HypertableException : public std::runtime_error {

		public:

			/// <summary>
			/// Initializes a new empty instance of the HypertableException class.
			/// </summary>
			HypertableException( )
			: std::runtime_error( "" )
			{
				eCode = 0;
				eLine = 0;
				eInner = 0;
			}

			/// <summary>
			/// Initializes a new instance of the HypertableException.
			/// </summary>
			/// <param name="msg">Error message</param>
			explicit HypertableException( const std::string& msg )
			: std::runtime_error( msg )
			{
				eCode = 0;
				eLine = 0;
				eInner = 0;
			}

			/// <summary>
			/// Initializes a new instance of the HypertableException.
			/// </summary>
			/// <param name="code">Error code</param>
			/// <param name="msg">Error message</param>
			/// <param name="l">Error line</param>
			/// <param name="fn">Error function/method</param>
			/// <param name="fl">Source file</param>
			HypertableException( int code, const std::string& msg, int l = 0, const char *fn = 0, const char *fl = 0 )
			: std::runtime_error( msg )
			{
				eCode = code;
				eLine = l;
				if( fn ) eFunc = fn;
				if( fl ) eFile = fl;
				eInner = 0;
			}

			/// <summary>
			/// Initializes a new instance of the HypertableException.
			/// </summary>
			/// <param name="e">Other exception to copy</param>
			HypertableException( const HypertableException& e )
			: std::runtime_error(e)
			{
				eCode = e.code();
				eLine = e.line();
				eInner = e.inner() ? new HypertableException(*e.inner()) : 0;
			}

			#ifndef __cplusplus_cli

			/// <summary>
			/// Initializes a new instance of the HypertableException.
			/// </summary>
			/// <param name="e">Hypertable exception</param>
			/// <param name="msg">Error message</param>
			/// <remarks>Pure native constructor</remarks>
			HypertableException( const Hypertable::Exception& e, const std::string& msg );

			/// <summary>
			/// Render error message to a ostream object.
			/// </summary>
			/// <param name="out">ostream object</param>
			/// <param name="e">Hypertable exception</param>
			/// <returns>ostream object</returns>
			/// <remarks>Pure native method.</remarks>
			static std::ostream& renderMessage( std::ostream& out, const Hypertable::Exception& e );

			#endif

			/// <summary>
			/// Destroys the HypertableException instance.
			/// </summary>
			virtual ~HypertableException( ) {
				if( eInner ) delete eInner;
			}

			/// <summary>
			/// Returns the error code.
			/// </summary>
			/// <returns>Error code</returns>
			int code( ) const { 
				return eCode;
			}

			/// <summary>
			/// Returns the error line.
			/// </summary>
			/// <returns>Error line</returns>
			int line( ) const { 
				return eLine;
			}

			/// <summary>
			/// Returns the error function/method.
			/// </summary>
			/// <returns>Error function/method</returns>
			const std::string& func( ) const { 
				return eFunc;
			}

			/// <summary>
			/// Returns the error source filename.
			/// </summary>
			/// <returns>Error source filename</returns>
			const std::string& file() const { 
				return eFile;
			}

			/// <summary>
			/// Returns the inner excpetion, might be NULL.
			/// </summary>
			/// <returns>Inner exception</returns>
			const HypertableException* inner() const { 
				return eInner;
			}

		private:

			HypertableException& operator = ( const HypertableException& ) { return *this; }

			int eCode;
			int eLine;
			std::string eFunc;
			std::string eFile;

			HypertableException* eInner;
	};

	/// <summary>
	/// Represents a ht4c argument null exception.
	/// </summary>
	/// <remarks>Links between native and C++/CLI.</remarks>
	class HypertableArgumentNullException : public HypertableException {

		public:

			/// <summary>
			/// Initializes a new instance of the HypertableArgumentNullException.
			/// </summary>
			/// <param name="arg">Argument name</param>
			/// <param name="l">Error line</param>
			/// <param name="fn">Error function/method</param>
			/// <param name="fl">Source file</param>
			HypertableArgumentNullException( const std::string& arg, int l = 0, const char *fn = 0, const char *fl = 0 );
	};

	/// <summary>
	/// Represents a ht4c argument exception.
	/// </summary>
	/// <remarks>Links between native and C++/CLI.</remarks>
	class HypertableArgumentException : public HypertableException {

		public:

			/// <summary>
			/// Initializes a new instance of the HypertableArgumentNullException.
			/// </summary>
			/// <param name="msg">Error message</param>
			/// <param name="arg">Argument name</param>
			/// <param name="l">Error line</param>
			/// <param name="fn">Error function/method</param>
			/// <param name="fl">Source file</param>
			HypertableArgumentException( const std::string& msg, const std::string& arg, int l = 0, const char *fn = 0, const char *fl = 0 );

			/// <summary>
			/// Returns the argument name.
			/// </summary>
			/// <returns>Argument name</returns>
			const std::string& argument() const { 
				return eArg;
			}

		private:

			std::string eArg;
	};

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif