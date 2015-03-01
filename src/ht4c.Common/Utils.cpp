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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "stdafx.h"
#include "Utils.h"
#include "re2/re2.h"

#define HT4C_THROW( error, msg ) \
	throw Hypertable::Exception( error, msg, __LINE__, __FUNCTION__, __FILE__ );

namespace ht4c { namespace Common {

	CellFilterInfo::CellFilterInfo( )
	: cutoffTime(0)
	, maxVersions(0)
	, filterByExactQualifier(false)
	, filterByRegexpQualifier(false)
	, filterByPrefixQualifier(false)
	{
	}

	CellFilterInfo::CellFilterInfo( const CellFilterInfo& other ) {
		cutoffTime = other.cutoffTime;
		maxVersions = other.maxVersions;
		for each( re2::RE2* re in other.regexpQualifiers ) {
			regexpQualifiers.push_back( new re2::RE2(re->pattern()) );
		}
		exactQualifiers = other.exactQualifiers;
		for each( const std::string& q in exactQualifiers ) {
			exactQualifiersSet.insert( q.c_str() );
		}
		prefixQualifiers = other.prefixQualifiers;
		filterByExactQualifier = other.filterByExactQualifier;
		filterByRegexpQualifier = other.filterByRegexpQualifier;
		filterByPrefixQualifier = other.filterByPrefixQualifier;

		columnPredicates = other.columnPredicates;
	}

	CellFilterInfo& CellFilterInfo::operator = ( const CellFilterInfo& other ) {
		for each( re2::RE2* re in regexpQualifiers ) {
			delete re;
		}
		regexpQualifiers.clear();

		cutoffTime = other.cutoffTime;
		maxVersions = other.maxVersions;
		for each( re2::RE2* re in other.regexpQualifiers ) {
			regexpQualifiers.push_back( new re2::RE2(re->pattern()) );
		}
		exactQualifiers = other.exactQualifiers;
		for each( const std::string& q in exactQualifiers ) {
			exactQualifiersSet.insert( q.c_str() );
		}
		prefixQualifiers = other.prefixQualifiers;
		filterByExactQualifier = other.filterByExactQualifier;
		filterByRegexpQualifier = other.filterByRegexpQualifier;
		filterByPrefixQualifier = other.filterByPrefixQualifier;

		columnPredicates = other.columnPredicates;

		return *this;
	}

	CellFilterInfo::~CellFilterInfo() {
		for each( re2::RE2* re in regexpQualifiers ) {
			delete re;
		}
		regexpQualifiers.clear();

		for each( re2::RE2* re in regexpValueColumnPredicates ) {
			if( re ) {
				delete re;
			}
		}
		regexpValueColumnPredicates.clear();
	}

	bool CellFilterInfo::qualifierMatches( const char* qualifier, size_t qualifierLength ) {
		if( !filterByExactQualifier && !filterByRegexpQualifier && !filterByPrefixQualifier ) {
			return true;
		}
		// check exact match first
		if( exactQualifiersSet.find(qualifier) != exactQualifiersSet.end() ) {
			return true;
		}
		if (filterByPrefixQualifier) {
			int cmp;
			for each (const std::string& qstr in prefixQualifiers) {
				if (qstr.size() > qualifierLength)
					continue;
				if (0 == (cmp = memcmp(qstr.c_str(), qualifier, qstr.size())))
					return true;
				else if (cmp > 0)
					break;
			}
		}
		// check for regexp match
		for each( re2::RE2* re in regexpQualifiers ) {
			if( RE2::PartialMatch(qualifier, *re) ) {
				return true;
			}
		}
		return false;
	}

	void CellFilterInfo::addQualifier( const std::string& qualifier, bool isRegexp, bool isPrefix) {
		if( isRegexp ) {
			re2::RE2* regexp = new re2::RE2( qualifier );
			if( !regexp->ok() ) {
				HT4C_THROW( Hypertable::Error::BAD_SCAN_SPEC, Hypertable::format("Can't convert qualifier '%s' to regexp (%s)", qualifier, regexp->error_arg()).c_str() );
			}
			regexpQualifiers.push_back( regexp );
			filterByRegexpQualifier = true;
		}
		else if( isPrefix ) {
			prefixQualifiers.insert(qualifier);
			filterByPrefixQualifier = true;
		}
		else {
			std::pair<std::set<std::string>::iterator, bool> r = exactQualifiers.insert( qualifier );
			if( r.second ) {
				exactQualifiersSet.insert( (*r.first).c_str() );
			}
			filterByExactQualifier = true;
		}
	}

	bool CellFilterInfo::columnPredicateMatches( const void* value, uint32_t value_len ) {
		// Unfortunately Hypertable cannot distinguish between NULL and ""
		if( value == 0 ) {
			value = "";
		}
		int ncp = 0;
		for each( const Hypertable::ColumnPredicate& cp in columnPredicates ) {
			if( cp.value && value ) {
				switch (cp.operation) {
					case Hypertable::ColumnPredicate::EXACT_MATCH:
						if( cp.value_len == value_len && memcmp(cp.value, value, cp.value_len) == 0 ) {
							return true;
						}
						break;
					case Hypertable::ColumnPredicate::PREFIX_MATCH:
						if( cp.value_len <= value_len && memcmp(cp.value, value, cp.value_len) == 0 ) {
							return true;
						}
						break;
					case Hypertable::ColumnPredicate::REGEX_MATCH: {
						std::string str( reinterpret_cast<const char*>(value), value_len );
						if( RE2::PartialMatch(str, *regexpValueColumnPredicates[ncp]) ) {
							return true;
						}
						break;
					}
					default:
							break;
				}
			}
			else if( !cp.value && !value ) {
				return true;
			}
			++ncp;
		}
		return false;
	}

	void CellFilterInfo::addColumnPredicate( const Hypertable::ColumnPredicate& columnPredicate ) {
		columnPredicates.push_back( columnPredicate );

		if( columnPredicate.operation == Hypertable::ColumnPredicate::REGEX_MATCH ) {
			std::string pattern( reinterpret_cast<const char*>(columnPredicate.value), columnPredicate.value_len );
			regexpValueColumnPredicates.push_back( new re2::RE2(pattern) );
		}
		else {
			regexpValueColumnPredicates.push_back( 0 );
		}
	}

	ScanContext::ScanContext( const Hypertable::ScanSpec& _scanSpec, Hypertable::SchemaPtr _schema )
	: schema( _schema )
	, scanSpec( _scanSpec )
	, timeInterval( _scanSpec.time_interval )
	, rowRegexp( 0 )
	, valueRegexp( 0 )
	, keysOnly( scanSpec.keys_only )
	, rowOffset( scanSpec.row_offset )
	, cellOffset( scanSpec.cell_offset )
	, rowLimit( scanSpec.row_limit )
	, cellLimit( scanSpec.cell_limit )
	, cellLimitPerFamily( scanSpec.cell_limit_per_family )
	{
	}

	void ScanContext::initialize() {
		ZeroMemory( familyMask, sizeof(familyMask) );
		ZeroMemory( columnFamilies, sizeof(columnFamilies) );

		boost::xtime xtnow;
		boost::xtime_get( &xtnow, boost::TIME_UTC_ );
		int64_t now = ((int64_t)xtnow.sec * 1000000000LL) + (int64_t)xtnow.nsec;
		int32_t maxVersions = scanSpec.max_versions;

		if( scanSpec.columns.size() > 0 ) {
			std::string family;
			const char* qualifierCstr;
			size_t qualifierLength;
			bool hasQualifier, isRegexp, isPrefix;

			for each( const char* cfstr in scanSpec.columns ) {
				std::string qualifier;

				Hypertable::ScanSpec::parse_column( cfstr, family, &qualifierCstr, &qualifierLength, &hasQualifier, &isRegexp, &isPrefix );
				Hypertable::ColumnFamilySpec* cf = schema->get_column_family( family.c_str() );

				if( cf == 0 ) {
					HT4C_THROW( Hypertable::Error::RANGESERVER_INVALID_COLUMNFAMILY, Hypertable::format("Invalid column family '%s'", cfstr).c_str() );
				}
				uint8_t id = cf->get_id();
				if( id == 0 ) {
					HT4C_THROW( Hypertable::Error::RANGESERVER_SCHEMA_INVALID_CFID, Hypertable::format("Bad id for column family '%s'", cf->get_name().c_str()).c_str() );
				}
				if( cf->get_option_counter() ) {
					HT4C_THROW( Hypertable::Error::BAD_SCAN_SPEC, "Counters are not yet supported" );
				}

				columnFamilies[id] = cf;
				familyMask[id] = true;

				CellFilterInfo& cfi = familyInfo[id];

				if( hasQualifier ) {
					qualifier = std::string( qualifierCstr, qualifierLength );
					cfi.addQualifier( qualifier, isRegexp, isPrefix );
				}
				if( cf->get_option_ttl() == 0 ) {
					cfi.cutoffTime = Hypertable::TIMESTAMP_MIN;
				}
				else {
					cfi.cutoffTime = now - ((int64_t)cf->get_option_ttl() * 1000000000LL);
				}

				if( maxVersions == 0 ) {
					cfi.maxVersions = cf->get_option_max_versions();
				}
				else {
					if( cf->get_option_max_versions() == 0 ) {
						cfi.maxVersions = maxVersions;
					}
					else {
						cfi.maxVersions = maxVersions < cf->get_option_max_versions() ?  maxVersions : cf->get_option_max_versions();
					}
				}

				initialColumn( cf, hasQualifier, isRegexp, isPrefix, qualifier );
			}
		}
		else {
			Hypertable::AccessGroupSpecs& aglist = schema->get_access_groups();

			// ROW_DELETE records have 0 column family, so this allows them to pass through
			familyMask[0] = true;
			for( Hypertable::AccessGroupSpecs::iterator ag = aglist.begin(); ag != aglist.end(); ++ag ) {
				for( Hypertable::ColumnFamilySpecs::iterator cf = (*ag)->columns().begin(); cf != (*ag)->columns().end(); ++cf ) {
					uint8_t id =  (*cf)->get_id();
					if( id == 0 ) {
						HT4C_THROW( Hypertable::Error::RANGESERVER_SCHEMA_INVALID_CFID, Hypertable::format("Bad id for column family '%s'", (*cf)->get_name().c_str()).c_str() );
					}
					if( (*cf)->get_deleted() ) {
						familyMask[id] = false;
						continue;
					}
					if( (*cf)->get_option_counter() ) {
						HT4C_THROW( Hypertable::Error::BAD_SCAN_SPEC, "Counters are not yet supported" );
					}
					columnFamilies[id] = *cf;
					familyMask[id] = true;

					CellFilterInfo& cfi = familyInfo[id];

					if( (*cf)->get_option_ttl() == 0 ) {
						cfi.cutoffTime = Hypertable::TIMESTAMP_MIN;
					}
					else {
						cfi.cutoffTime = now- ((int64_t)(*cf)->get_option_ttl() * 1000000000LL);
					}

					if( maxVersions == 0 ) {
						cfi.maxVersions = (*cf)->get_option_max_versions();
					}
					else {
						if( (*cf)->get_option_max_versions() == 0 ) {
							cfi.maxVersions = maxVersions;
						}
						else {
							cfi.maxVersions = (maxVersions < (*cf)->get_option_max_versions()) ? maxVersions : (*cf)->get_option_max_versions();
						}
					}
				}
			}
		}

		if( scanSpec.scan_and_filter_rows ) {
			for each( const Hypertable::RowInterval& ri in scanSpec.row_intervals ) {
				if( ri.start && *ri.start ) {
					rowset.insert( ri.start );
				}
				if( ri.end && *ri.end && ri.start != ri.end ) {
					rowset.insert( ri.end );
				}
			}
		}

		for each( const Hypertable::ColumnPredicate& cp in scanSpec.column_predicates ) {
			if( cp.column_family && *cp.column_family ) {
				Hypertable::ColumnFamilySpec* cf = schema->get_column_family( cp.column_family );

				if( cf == 0 ) {
					HT4C_THROW( Hypertable::Error::RANGESERVER_INVALID_COLUMNFAMILY, Hypertable::format("Invalid column family '%s'", cp.column_family).c_str() );
				}
				uint8_t id =  cf->get_id();
				if( id == 0 ) {
					HT4C_THROW( Hypertable::Error::RANGESERVER_SCHEMA_INVALID_CFID, Hypertable::format("Bad id for column family '%s'", cf->get_name().c_str()).c_str() );
				}
				if( cf->get_option_counter() ) {
					HT4C_THROW( Hypertable::Error::BAD_SCAN_SPEC, "Counters are not supported" );
				}

				CellFilterInfo& cfi = familyInfo[id];

				if( cp.operation & Hypertable::ColumnPredicate::VALUE_MATCH ) {
					cfi.addColumnPredicate( cp );
				}
				else if( cp.operation & Hypertable::ColumnPredicate::QUALIFIER_MATCH ) {
					std::string qualifier( reinterpret_cast<const char*>(cp.column_qualifier), cp.column_qualifier_len );
					cfi.addQualifier( 
						  qualifier
						, (cp.operation & Hypertable::ColumnPredicate::QUALIFIER_REGEX_MATCH) > 0
						, (cp.operation & Hypertable::ColumnPredicate::QUALIFIER_PREFIX_MATCH) > 0 );
				}
			}
		}

		if( scanSpec.row_regexp && *scanSpec.row_regexp ) {
			rowRegexp = new re2::RE2( scanSpec.row_regexp );
			if( !rowRegexp->ok() ) {
				HT4C_THROW( Hypertable::Error::BAD_SCAN_SPEC, Hypertable::format("Can't convert row regexp %s to regexp (%s)", scanSpec.row_regexp, rowRegexp->error_arg()).c_str() );
			}
		}

		if( scanSpec.value_regexp && *scanSpec.value_regexp ) {
			valueRegexp = new re2::RE2( scanSpec.value_regexp );
			if( !valueRegexp->ok() ) {
				HT4C_THROW( Hypertable::Error::BAD_SCAN_SPEC, Hypertable::format("Can't convert value regexp %s to regexp (%s)", scanSpec.value_regexp, valueRegexp->error_arg()).c_str() );
			}
		}

	}

	ScanContext::~ScanContext() {
		if( rowRegexp ) {
			delete rowRegexp;
			rowRegexp = 0;
		}
		if( valueRegexp ) {
			delete valueRegexp;
			valueRegexp = 0;
		}
	}

} }