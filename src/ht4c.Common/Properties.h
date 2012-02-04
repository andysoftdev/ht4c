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
#endif

#include <string>
#include <map>
#ifndef __cplusplus_cli
#include <boost/any.hpp>
#endif

namespace ht4c { namespace Common {

	/// <summary>
	/// Represents a typed key/value map, used for configuration parameters.
	/// </summary>
	/// <remarks>Links between native and C++/CLI.</remarks>
	class Properties {

		public:

			/// <summary>
			/// Creates a new Properties instance.
			/// </summary>
			/// <returns>New Properties instance</returns>
			/// <remarks>To free the created instance, use the delete operator.</remarks>
			static Properties* create( );

			/// <summary>
			/// Destroys the Cell instance.
			/// </summary>
			virtual ~Properties() { }

			/// <summary>
			/// Remove all properties.
			/// </summary>
			void clear();

			/// <summary>
			/// Returns all property names.
			/// </summary>
			/// <param name="names">Receives all property names</param>
			void names( std::vector<std::string>& names ) const;

			/// <summary>
			/// Returns the type info for a particular property.
			/// </summary>
			/// <returns>Property type info</returns>
			/// <remarks>Returns typeid(void) if the property does not exist.</remarks>
			const type_info& type( const std::string& name ) const;

			/// <summary>
			/// Gets the property value for a particular property name.
			/// </summary>
			/// <param name="name">Property name</param>
			/// <param name="value">Receives the property value</param>
			/// <returns>true if succeeded</returns>
			bool get( const std::string& name, bool& value ) const;
			bool get( const std::string& name, uint16_t& value ) const;
			bool get( const std::string& name, int32_t& value ) const;
			bool get( const std::string& name, std::vector<int32_t>& value ) const;
			bool get( const std::string& name, int64_t& value ) const;
			bool get( const std::string& name, std::vector<int64_t>& value ) const;
			bool get( const std::string& name, double& value ) const;
			bool get( const std::string& name, std::vector<double>& value ) const;
			bool get( const std::string& name, std::string& value ) const;
			bool get( const std::string& name, std::vector<std::string>& value ) const;

			/// <summary>
			/// Adds or updates the property value for a particular property name.
			/// </summary>
			/// <param name="name">Property name</param>
			/// <param name="value">Property value</param>
			/// <returns>true if the value has been added, false idf the value has been updated</returns>
			bool addOrUpdate( const std::string& name, bool value );
			bool addOrUpdate( const std::string& name, uint16_t value );
			bool addOrUpdate( const std::string& name, int32_t value );
			bool addOrUpdate( const std::string& name, const std::vector<int32_t>& value );
			bool addOrUpdate( const std::string& name, int64_t value );
			bool addOrUpdate( const std::string& name, const std::vector<int64_t>& value );
			bool addOrUpdate( const std::string& name, double value );
			bool addOrUpdate( const std::string& name, const std::vector<double>& value );
			bool addOrUpdate( const std::string& name, const std::string& value );
			bool addOrUpdate( const std::string& name, const std::vector<std::string>& value );

			#ifndef __cplusplus_cli

			/// <summary>
			/// Gets the property value for a particular property name.
			/// </summary>
			/// <param name="name">Property name</param>
			/// <param name="value">Receives the property value</param>
			/// <returns>true if succeeded</returns>
			/// <remarks>Pure native method.</remarks>
			template < typename T > inline
			bool get( const std::string& name, T& value ) const {
				map_t::const_iterator it = map.find( name );
				if( it != map.end() ) {
					value = boost::any_cast<T>((*it).second);
					return true;
				}
				return false;
			}

			template < int32_t > inline
			bool get( const std::string& name, int32_t& value ) const {
				map_t::const_iterator it = map.find( name );
				if( it != map.end() ) {
					value = typeid((*it).second.value) == int32_t::typeid ? boost::any_cast<int32_t>((*it).second) : boost::any_cast<long>((*it).second);
					return true;
				}
				return false;
			}

			/// <summary>
			/// Gets the property value for a particular property name.
			/// </summary>
			/// <param name="name">Property name</param>
			/// <returns>Property value</returns>
			/// <remarks>Pure native method.</remarks>
			const boost::any get( const std::string& name ) const;

			/// <summary>
			/// Sets the property value for a particular property name.
			/// </summary>
			/// <param name="name">Property name</param>
			/// <param name="value">Property value</param>
			/// <returns>true if the value has been set</returns>
			/// <remarks>Pure native method.</remarks>
			bool addOrUpdate( const std::string& name, const boost::any& value );

			/// <summary>
			/// Converts a value from source type to destination type.
			/// </summary>
			/// <param name="src">Source type</param>
			/// <param name="dst">Destination type</param>
			/// <param name="value">Value</param>
			/// <returns>Converted value</returns>
			/// <remarks>Pure native method.</remarks>
			static boost::any convert( const std::type_info& src, const std::type_info& dst, const boost::any& value );

			#endif

		private:

			Properties() { }
			Properties( const Properties& ) { }
			Properties& operator = ( const Properties& ) { return *this; }

			#ifndef __cplusplus_cli

			typedef std::map<std::string, boost::any> map_t;
			map_t map;

			#endif
	};

} }

#ifdef __cplusplus_cli
#pragma managed( pop )
#endif