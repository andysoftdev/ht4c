/** -*- C++ -*-
 * Copyright (C) 2010-2016 Thalmann Software & Consulting, http://www.softdev.ch
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

#include <assert.h>
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

	#if defined(HYPERTABLE_KEYSPEC_H) && defined(Hypertable_Lib_ScanSpec_h) && defined(HYPERTABLE_CELLPREDICATE_H)

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
			void addQualifier( const std::string& qualifier, bool isRegexp, bool isPrefix );
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
			void addColumnPredicate( const Hypertable::ColumnPredicate& columnPredicate );
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
			std::vector<re2::RE2*> regexpValueColumnPredicates;
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

			virtual void initialize( );

			Hypertable::SchemaPtr schema;
			const Hypertable::ScanSpec& scanSpec;
			std::pair<int64_t, int64_t> timeInterval;
			bool familyMask[ScanContext::MAX_CF];
			const Hypertable::ColumnFamilySpec* columnFamilies[MAX_CF];
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

		 virtual void initialColumn( Hypertable::ColumnFamilySpec* cf, bool hasQualifier, bool isRegexp, bool isPrefix, const std::string& qualifier ) { }
	};

	#endif

	/// <summary>
	/// The fast numeric formatters, very much inspired from http://cppformat.github.io/ */
	/// </summary>
	class NumericFormatterDigits {
	protected:
		static const wchar_t DIGITSW[];
	};

	template<class T, class C>
	class NumericFormatter : public NumericFormatterDigits {
	public:

		/// <summary>
		// Returns the number of characters written to the output buffer.
		/// </summary>
		size_t size() const { return buf - s + BUFFER_SIZE - 1; }

		/// <summary>
		/// Returns a pointer to the output buffer content. No terminating null character is appended.
		/// </summary>
		const C *data() const { return str; }

		/// <summary>
		/// Returns a pointer to the output buffer content with terminating null character appended.
		/// </summary>
		const C *c_str() const {
			return s;
		}

		/// <summary>
		/// Appends the converted number to the buffer specified, returns the forwarded pointer.
		/// </summary>
		C* append_to(C* p) const {
			memcpy(p, s, size() * sizeof(C));
			return p + size();
		}

	protected:

		NumericFormatter() {
			buf[BUFFER_SIZE - 1] = 0;
		}

		void format_unsigned(T value, C preffix = 0) {
			s = buf + BUFFER_SIZE - 1;
			while (value >= 100) {
				// Integer division is slow so do it for a group of two digits instead
				// of for every digit. The idea comes from the talk by Alexandrescu
				// "Three Optimization Tips for C++". See speed-test for a comparison.
				unsigned index = (value % 100) * 2;
				value /= 100;
				*--s = DIGITSW[index + 1];
				*--s = DIGITSW[index];
			}
			if (value < 10) {
				*--s = static_cast<wchar_t>(L'0' + value);
				if( preffix ) {
					*--s = preffix;
				}
				return;
			}
			unsigned index = static_cast<unsigned>(value * 2);
			*--s = DIGITSW[index + 1];
			*--s = DIGITSW[index];

			if( preffix ) {
				*--s = preffix;
			}
		}

		void format_signed(T value, C preffix = 0) {
			if (value >= 0)
				format_unsigned(value, preffix);
			else {
				format_unsigned(-value);
				*--s = L'-';
				if( preffix ) {
					*--s = preffix;
				}
			}
		}

	private:
		enum { BUFFER_SIZE = std::numeric_limits<T>::digits10 + 4 };
		C buf[BUFFER_SIZE];
		C* s;
	};

	template<class T>
	class NumericSignedFormatter : public NumericFormatter<T, wchar_t> {
	public:
		explicit NumericSignedFormatter(T value, wchar_t preffix = 0) {
			NumericFormatter<T, wchar_t>::format_signed(value, preffix);
		}
	};

	template<class T>
	class NumericUnsignedFormatter : public NumericFormatter<T, wchar_t> {
	public:
		explicit NumericUnsignedFormatter(T value, wchar_t preffix = 0) {
			NumericFormatter<T, wchar_t>::format_unsigned(value, preffix);
		}
	};

	typedef NumericUnsignedFormatter<uint8_t> UInt8Formatter;
	typedef NumericUnsignedFormatter<uint16_t> UInt16Formatter;
	typedef NumericUnsignedFormatter<uint32_t> UInt32Formatter;
	typedef NumericUnsignedFormatter<uint64_t> UInt64Formatter;

	typedef NumericSignedFormatter<int8_t> Int8Formatter;
	typedef NumericSignedFormatter<int16_t> Int16Formatter;
	typedef NumericSignedFormatter<int32_t> Int32Formatter;
	typedef NumericSignedFormatter<int64_t> Int64Formatter;


} }

#ifdef __cplusplus_cli
#pragma managed ( pop )
#endif