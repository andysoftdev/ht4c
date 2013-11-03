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
		searchBufColumnPredicates = other.searchBufColumnPredicates;
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
		searchBufColumnPredicates = other.searchBufColumnPredicates;

		return *this;
	}

	CellFilterInfo::~CellFilterInfo() {
		for each( re2::RE2* re in regexpQualifiers ) {
			delete re;
		}
		regexpQualifiers.clear();
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

	void CellFilterInfo::addQualifier(const char* qualifier, bool isRegexp, bool isPrefix) {
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
					case Hypertable::ColumnPredicate::CONTAINS:
							if( cp.value_len <= value_len ) {
								while( searchBufColumnPredicates.size() < columnPredicates.size() ) {
									searchBufColumnPredicates.push_back( search_buf_t() );
								}
								search_buf_t& buf = searchBufColumnPredicates[ncp];
								if( Common::memfind( static_cast<const uint8_t*>(value)
																	 , value_len
																	 , reinterpret_cast<const uint8_t*>(cp.value)
																	 , cp.value_len
																	 , buf.shift
																	 ,&buf.repeat) != 0 ) {
									return true;
								}
							}
							break;
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
		ZeroMemory( familyMask, sizeof(familyMask) );
		ZeroMemory( columnFamilies, sizeof(columnFamilies) );

		boost::xtime xtnow;
		boost::xtime_get( &xtnow, boost::TIME_UTC_ );
		int64_t now = ((int64_t)xtnow.sec * 1000000000LL) + (int64_t)xtnow.nsec;
		uint32_t maxVersions = scanSpec.max_versions;

		if( scanSpec.columns.size() > 0 ) {
			std::string family, qualifier;
			bool hasQualifier, isRegexp, isPrefix;

			for each( const char* cfstr in scanSpec.columns ) {
				Hypertable::ScanSpec::parse_column( cfstr, family, qualifier, &hasQualifier, &isRegexp, &isPrefix );
				Hypertable::Schema::ColumnFamily* cf = schema->get_column_family( family.c_str() );

				if( cf == 0 ) {
					HT4C_THROW( Hypertable::Error::RANGESERVER_INVALID_COLUMNFAMILY, Hypertable::format("Invalid column family '%s'", cfstr).c_str() );
				}
				if( cf->id == 0 ) {
					HT4C_THROW( Hypertable::Error::RANGESERVER_SCHEMA_INVALID_CFID, Hypertable::format("Bad id for column family '%s'", cf->name.c_str()).c_str() );
				}
				if( cf->counter ) {
					HT4C_THROW( Hypertable::Error::BAD_SCAN_SPEC, "Counters are not yet supported" );
				}

				columnFamilies[cf->id] = cf;
				familyMask[cf->id] = true;
				if( hasQualifier ) {
					familyInfo[cf->id].addQualifier( qualifier.c_str(), isRegexp, isPrefix );
				}
				if( cf->ttl == 0 ) {
					familyInfo[cf->id].cutoffTime = Hypertable::TIMESTAMP_MIN;
				}
				else {
					familyInfo[cf->id].cutoffTime = now - ((int64_t)cf->ttl * 1000000000LL);
				}

				if( maxVersions == 0 ) {
					familyInfo[cf->id].maxVersions = cf->max_versions;
				}
				else {
					if( cf->max_versions == 0 ) {
						familyInfo[cf->id].maxVersions = maxVersions;
					}
					else {
						familyInfo[cf->id].maxVersions = maxVersions < cf->max_versions ?  maxVersions : cf->max_versions;
					}
				}

				initialColumn( cf, hasQualifier, isRegexp, isPrefix, qualifier );
			}
		}
		else {
			Hypertable::Schema::AccessGroups& aglist = schema->get_access_groups();

			// ROW_DELETE records have 0 column family, so this allows them to pass through
			familyMask[0] = true;
			for( Hypertable::Schema::AccessGroups::iterator ag = aglist.begin(); ag != aglist.end(); ++ag ) {
				for( Hypertable::Schema::ColumnFamilies::iterator cf = (*ag)->columns.begin(); cf != (*ag)->columns.end(); ++cf ) {
					if( (*cf)->id == 0 ) {
						HT4C_THROW( Hypertable::Error::RANGESERVER_SCHEMA_INVALID_CFID, Hypertable::format("Bad id for column family '%s'", (*cf)->name.c_str()).c_str() );
					}
					if( (*cf)->deleted ) {
						familyMask[(*cf)->id] = false;
						continue;
					}
					if( (*cf)->counter ) {
						HT4C_THROW( Hypertable::Error::BAD_SCAN_SPEC, "Counters are not yet supported" );
					}
					columnFamilies[(*cf)->id] = *cf;
					familyMask[(*cf)->id] = true;
					if( (*cf)->ttl == 0 ) {
						familyInfo[(*cf)->id].cutoffTime = Hypertable::TIMESTAMP_MIN;
					}
					else {
						familyInfo[(*cf)->id].cutoffTime = now- ((int64_t)(*cf)->ttl * 1000000000LL);
					}

					if( maxVersions == 0 ) {
						familyInfo[(*cf)->id].maxVersions = (*cf)->max_versions;
					}
					else {
						if( (*cf)->max_versions == 0 ) {
							familyInfo[(*cf)->id].maxVersions = maxVersions;
						}
						else {
							familyInfo[(*cf)->id].maxVersions = (maxVersions < (*cf)->max_versions) ? maxVersions : (*cf)->max_versions;
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
				Hypertable::Schema::ColumnFamily* cf = schema->get_column_family( cp.column_family );

				if( cf == 0 ) {
					HT4C_THROW( Hypertable::Error::RANGESERVER_INVALID_COLUMNFAMILY, Hypertable::format("Invalid column family '%s'", cp.column_family).c_str() );
				}
				if( cf->id == 0 ) {
					HT4C_THROW( Hypertable::Error::RANGESERVER_SCHEMA_INVALID_CFID, Hypertable::format("Bad id for column family '%s'", cf->name.c_str()).c_str() );
				}
				if( cf->counter ) {
					HT4C_THROW( Hypertable::Error::BAD_SCAN_SPEC, "Counters are not supported" );
				}

				familyInfo[cf->id].addColumnPredicate( cp );
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