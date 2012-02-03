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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "stdafx.h"
#include "Properties.h"

namespace ht4c { namespace Common {

	Properties* Properties::create( ) {
		return new Properties;
	}

	void Properties::clear( ) {
		map.clear();
	}

	void Properties::names( std::vector<std::string>& names ) const {
		names.clear();
		names.reserve(map.size());
		for each( const map_t::value_type& item in map ) {
			names.push_back( item.first );
		}
	}

	const type_info& Properties::type( const std::string& name ) const {
		map_t::const_iterator it = map.find( name );
		return it != map.end() ? (*it).second.type() : typeid(void);
	}

	bool Properties::get( const std::string& name, bool& value ) const {
		return get<bool>( name, value );
	}

	bool Properties::get( const std::string& name, uint16_t& value ) const {
		return get<uint16_t>( name, value );
	}

	bool Properties::get( const std::string& name, int32_t& value ) const {
		return get<int32_t>( name, value );
	}

	bool Properties::get( const std::string& name, std::vector<int32_t>& value ) const {
		return get<std::vector<int32_t>>( name, value );
	}

	bool Properties::get( const std::string& name, int64_t& value ) const {
		return get<int64_t>( name, value );
	}

	bool Properties::get( const std::string& name, std::vector<int64_t>& value ) const {
		return get<std::vector<int64_t>>( name, value );
	}
	bool Properties::get( const std::string& name, double& value ) const {
		return get<double>( name, value );
	}

	bool Properties::get( const std::string& name, std::vector<double>& value ) const {
		return get<std::vector<double>>( name, value );
	}

	bool Properties::get( const std::string& name, std::string& value ) const {
		return get<std::string>( name, value );
	}

	bool Properties::get( const std::string& name, std::vector<std::string>& value ) const {
		return get<std::vector<std::string>>( name, value );
	}

	const boost::any Properties::get( const std::string& name ) const {
		map_t::const_iterator it = map.find( name );
		return it != map.end() ? (*it).second : boost::any();
	}

	bool Properties::addOrUpdate( const std::string& name, bool value ) {
		return addOrUpdate( name, boost::any(value) );
	}

	bool Properties::addOrUpdate( const std::string& name, uint16_t value ) {
		return addOrUpdate( name, boost::any(value) );
	}

	bool Properties::addOrUpdate( const std::string& name, int32_t value ) {
		return addOrUpdate( name, boost::any(value) );
	}

	bool Properties::addOrUpdate( const std::string& name, const std::vector<int32_t>& value ) {
		return addOrUpdate( name, boost::any(value) );
	}

	bool Properties::addOrUpdate( const std::string& name, int64_t value ) {
		return addOrUpdate( name, boost::any(value) );
	}

	bool Properties::addOrUpdate( const std::string& name, const std::vector<int64_t>& value ) {
		return addOrUpdate( name, boost::any(value) );
	}

	bool Properties::addOrUpdate( const std::string& name, double value ) {
		return addOrUpdate( name, boost::any(value) );
	}

	bool Properties::addOrUpdate( const std::string& name, const std::vector<double>& value ) {
		return addOrUpdate( name, boost::any(value) );
	}

	bool Properties::addOrUpdate( const std::string& name, const std::string& value ) {
		return addOrUpdate( name, boost::any(value) );
	}

	bool Properties::addOrUpdate( const std::string& name, const std::vector<std::string>& value ) {
		return addOrUpdate( name, boost::any(value) );
	}

	bool Properties::addOrUpdate( const std::string& name, const boost::any& value ) {
		std::pair<map_t::iterator, bool> r = map.insert( map_t::value_type(name, value) );
		if( !r.second ) {
			(*r.first).second = value;
		}
		return r.second;
	}

	boost::any Properties::convert( const std::type_info& src, const std::type_info& dst, const boost::any& value ) {
		#define CONVERT_ANY( s, d, v )													\
			if( src == typeid(s) && dst == typeid(d) ) {					\
				d _value = static_cast<d>( boost::any_cast<s>(v) );	\
				return boost::any( _value );												\
			}

		CONVERT_ANY( int8_t, uint16_t, value );
		CONVERT_ANY( int16_t, uint16_t, value );
		CONVERT_ANY( int32_t, uint16_t, value );
		CONVERT_ANY( int64_t, uint16_t, value );

		CONVERT_ANY( int8_t, int32_t, value );
		CONVERT_ANY( int16_t, int32_t, value );
		CONVERT_ANY( int64_t, int32_t, value );

		std::stringstream ss;
		ss << "Unsupported property type\n\tat " << __FUNCTION__ << " (" << __FILE__ << ':' << __LINE__ << ')';
		throw std::bad_cast( ss.str().c_str() );

		#undef CONVERT_ANY
	}

} }
