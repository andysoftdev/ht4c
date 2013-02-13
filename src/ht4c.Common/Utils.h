/** -*- C++ -*-
 * Copyright (C) 2010-2013 Thalmann Software & Consulting, http://www.softdev.ch
 *
 * This file is part of ht4n.
 *
 * ht4n is free software; you can redistribute it and/or
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

namespace re2 {
	class RE2;
}

namespace ht4c { namespace Common {

	/// <summary>
	/// Returns the column family if not NULL or empty.
	/// </summary>
	inline const char* CF( const char* columnFamily ) {
		return  columnFamily && *columnFamily
					? columnFamily
					: 0;
	}

	/// <summary>
	/// Searches for a pattern in a block of memory using the Boyer- Moore-Horspool-Sunday algorithm.
	/// </summary>
	inline const uint8_t* memfind(
		const uint8_t* block,        // Block containing data
		size_t         block_size,   // Size of block in bytes
		const uint8_t* pattern,      // Pattern to search for
		size_t         pattern_size, // Size of pattern block
		size_t*        shift,        // Shift table (search buffer)
		bool*          repeat_find)  // true: search buffer already init
		{
		assert(block);
		assert(pattern);
		assert(shift);
		if (block == 0 || pattern == 0 || shift == 0)
			return 0;

		//  Pattern must be smaller or equal in size to string
		if (block_size < pattern_size)
			return 0;

		if (pattern_size == 0)
			return block;

		//  Build the shift table unless we're continuing a previous search.
		//  The shift table determines how far to shift before trying to match
		//  again, if a match at this point fails.  If the byte after where the
		//  end of our pattern falls is not in our pattern, then we start to
		//  match again after that byte; otherwise we line up the last occurence 
		//  of that byte in our pattern under that byte, and try match again.

		if (!repeat_find || !*repeat_find) {
			for (size_t byte_nbr = 0; byte_nbr < 256; byte_nbr++)
				shift[byte_nbr] = pattern_size + 1;
			for (size_t byte_nbr = 0; byte_nbr < pattern_size; byte_nbr++)
				shift[(uint8_t) pattern[byte_nbr]] = pattern_size - byte_nbr;

			if (repeat_find)
				*repeat_find = true;
		}

		// Search for the block, each time jumping up by the amount
		// computed in the shift table
		const uint8_t* limit = block + (block_size - pattern_size + 1);
		assert(limit > block);

		for (const uint8_t* match_base = block;
			match_base < limit;
			match_base += shift[*(match_base + pattern_size)]) {
			const uint8_t* match_ptr  = match_base;
			size_t match_size = 0;
			// Compare pattern until it all matches, or we find a difference
			while (*match_ptr++ == pattern[match_size++]) {
				assert(match_size <= pattern_size && match_ptr == (match_base + match_size));

				// If we found a match, return the start address
				if (match_size >= pattern_size)
					return match_base;
			}
		}
		return 0;
	}

	#ifdef HYPERTABLE_KEYSPEC_H

	/// <summary>
	/// Returns the adjusted keyspec flag, according to the specified column family/qualifier.
	/// </summary>
	inline uint8_t FLAG( const char* columnFamily, const char* columnQualifier, uint8_t flag ) {
		if( flag != Hypertable::FLAG_INSERT ) {
			if( flag >= Hypertable::FLAG_DELETE_COLUMN_FAMILY ) {
				if( !CF(columnFamily) ) {
					return Hypertable::FLAG_DELETE_ROW;
				}
				else if( flag >= Hypertable::FLAG_DELETE_CELL && !columnQualifier ) {
					return Hypertable::FLAG_DELETE_COLUMN_FAMILY;
				}
			}
		}
		return flag;
	}

	/// <summary>
	/// Returns the appropriate keyspec DELETE flag, according to the specified column family/qualifier.
	/// </summary>
	inline uint8_t FLAG_DELETE( const char* columnFamily, const char* columnQualifier ) {
		return !CF(columnFamily)
					? Hypertable::FLAG_DELETE_ROW
					: columnQualifier
					? Hypertable::FLAG_DELETE_CELL
					: Hypertable::FLAG_DELETE_COLUMN_FAMILY;
	}

	/// <summary>
	/// Returns the adjusted timestamp, according to the specified timestamp and keyspec flag.
	/// </summary>
	inline uint64_t TIMESTAMP( uint64_t timestamp, uint8_t flag ) {
		return  timestamp == 0
					? Hypertable::AUTO_ASSIGN
					: flag == Hypertable::FLAG_INSERT || flag == Hypertable::FLAG_DELETE_CELL_VERSION
					? timestamp
					: timestamp + 1;
	}

	/// <summary>
	/// Cell filter info.
	/// </summary>
	class CellFilterInfo {

		public:

			CellFilterInfo( );
			CellFilterInfo( const CellFilterInfo& other );
			CellFilterInfo& operator = (const CellFilterInfo& other);
			virtual ~CellFilterInfo( );

			bool qualifierMatches( const char* qualifier, size_t qualifierLength );
			void addQualifier( const char* qualifier, bool isRegexp, bool isPrefix );
			inline bool hasQualifierFilter() const {
				return filterByExactQualifier || filterByRegexpQualifier || filterByPrefixQualifier;
			}
			inline bool hasQualifierRegexpFilter( ) const {
				return filterByRegexpQualifier;
			}
			inline bool hasQualifierPrefixFilter( ) const {
				return filterByPrefixQualifier;
			}

			bool columnPredicateMatches( const void* value, uint32_t value_len );
			inline void addColumnPredicate( const Hypertable::ColumnPredicate& columnPredicate ) {
				columnPredicates.push_back( columnPredicate );
			}
			inline bool hasColumnPredicateFilter( ) const {
				return columnPredicates.size() > 0;
			}

			int64_t cutoffTime;
			uint32_t maxVersions;

		private:

			struct LtCstr {
				bool operator( ) ( const char* s1, const char* s2 ) const {
					return strcmp( s1, s2 ) < 0;
				}
			};

			std::vector<re2::RE2*> regexpQualifiers;
			std::set<std::string> exactQualifiers;
			typedef std::set<const char*, LtCstr> QualifierSet;
			QualifierSet exactQualifiersSet;
			std::set<std::string> prefixQualifiers;
			std::vector<Hypertable::ColumnPredicate> columnPredicates;
			struct search_buf_t {
				size_t shift[256];
				bool repeat;

				search_buf_t( )
					: repeat( false )
				{
				}
			};
			std::vector<search_buf_t> searchBufColumnPredicates;
			bool filterByExactQualifier;
			bool filterByRegexpQualifier;
			bool filterByPrefixQualifier;
	};

	/// <summary>
	/// Regexp cache.
	/// </summary>
	class RegexpCache {

		public:

			RegexpCache( )
			: lastFamily(-1)
			, lastRowMatch(false)
			, lastColumnMatch(false)
			{
			}

			void checkRow(const char* rowkey, bool *cached, bool *match) {
				*match = lastRowMatch;
				*cached = !strcmp( rowkey, lastRow.c_str() );
			}

			void checkColumn(int family, const char* qualifier, bool *cached, bool *match) {
				*match = lastColumnMatch;
				*cached = lastFamily == family && !strcmp( qualifier, lastQualifier.c_str() );
			}

			void setRow( const char* rowkey, bool match ) {
				lastRow = rowkey;
				lastRowMatch = match;
			}

			void setColumn( int family, const char* qualifier, bool match ) {
				lastFamily = family;
				lastQualifier = qualifier;
				lastColumnMatch = match;
			}

		private:

			std::string lastRow;
			std::string lastQualifier;
			int lastFamily;
			bool lastRowMatch;
			bool lastColumnMatch;
	};

	/// <summary>
	/// Scan context.
	/// </summary>
	class ScanContext {

		public:

			enum {
				MAX_CF = 256
			};

			struct LtCstr {
				bool operator( ) ( const char* s1, const char* s2 ) const {
					return strcmp( s1, s2 ) < 0;
				}
			};

			ScanContext( const Hypertable::ScanSpec& scanSpec, Hypertable::SchemaPtr schema );
			virtual ~ScanContext( );

			Hypertable::SchemaPtr schema;
			const Hypertable::ScanSpec& scanSpec;
			std::pair<int64_t, int64_t> timeInterval;
			bool familyMask[ScanContext::MAX_CF];
			const Hypertable::Schema::ColumnFamily* columnFamilies[MAX_CF];
			CellFilterInfo familyInfo[ScanContext::MAX_CF];
			re2::RE2* rowRegexp;
			re2::RE2* valueRegexp;
			typedef std::set<const char*, LtCstr, Hypertable::CstrAlloc> CstrRowSet;
			CstrRowSet rowset;
			bool keysOnly;
			int rowOffset;
			int cellOffset;
			int rowLimit;
			int cellLimit;
			int cellLimitPerFamily;

		protected:

		 virtual void initialColumn( Hypertable::Schema::ColumnFamily* cf, bool hasQualifier, bool isRegexp, bool isPrefix, const std::string& qualifier ) { }
	};

	#endif

} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif