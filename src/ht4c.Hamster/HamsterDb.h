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

#pragma once

#ifdef __cplusplus_cli
#error compile native
#endif

#include "ham/hamsterdb.hpp"
#include "HamsterEnv.h"

namespace ht4c { namespace Hamster { namespace Db {

	class Client;
	typedef boost::intrusive_ptr<Client> ClientPtr;

	class Namespace;
	typedef boost::intrusive_ptr<Namespace> NamespacePtr;

	class Table;
	typedef boost::intrusive_ptr<Table> TablePtr;

	class Mutator;
	typedef boost::intrusive_ptr<Mutator> MutatorPtr;

	class MutatorAsync;
	typedef boost::intrusive_ptr<MutatorAsync> MutatorAsyncPtr;

	class Scanner;
	typedef boost::intrusive_ptr<Scanner> ScannerPtr;

	class ScannerAsync;
	typedef boost::intrusive_ptr<ScannerAsync> ScannerAsyncPtr;

	struct NamespaceListing {
		std::string name;
		bool isNamespace;
		std::vector<NamespaceListing> subEntries;
	};

	namespace KeyClassifiers {
		enum {
			Length = 1
		};

		const char NamespaceListing = '0';
	}

	class Client : public Hypertable::ReferenceCount {

		public:

			explicit Client( HamsterEnvPtr env );
			virtual ~Client( );

			inline HamsterEnv* getEnv( ) const {
				return env.get();
			}
			void createNamespace( const std::string& name, bool createIntermediate );
			Db::NamespacePtr openNamespace( const std::string& name );
			bool namespaceExists( const char* name );
			inline bool namespaceExists( const std::string& name ) {
				return namespaceExists( name.c_str() );
			}
			void dropNamespace( const std::string& name, bool ifExists );

		private:

			void createNamespace( const std::string& name );

			HamsterEnvPtr env;
			ham::db* sysdb;
	};

	class Namespace : public Hypertable::ReferenceCount {

		public:

			Namespace( ClientPtr client, const std::string& name );
			virtual ~Namespace( );

			inline HamsterEnv* getEnv( ) const {
				return env;
			}
			const char* getName( ) const;
			void getNamespaceListing( bool deep, std::vector<Db::NamespaceListing>& listing );
			void drop( const std::string& nsName, const std::vector<Db::NamespaceListing>& listing, bool ifExists, bool dropTables );
			void createTable( const std::string& name, const std::string& schema );
			Db::TablePtr openTable( const std::string& name );
			void alterTable( const std::string& name, const std::string& schema );
			bool tableExists( const std::string& name );
			void getTableId( const std::string& name, std::string& id );
			void getTableSchema( const std::string& name, bool withIds, std::string& schema );
			void renameTable( const std::string& name, const std::string& newName );
			void dropTable( const std::string& name, bool ifExists );
			void toKey( ham::key& key );

		private:

			ClientPtr client;
			HamsterEnv* env;
			ham::db* sysdb;
			std::string keyName;
	};

	class Table : public Hypertable::ReferenceCount {

		public:

			Table( NamespacePtr ns, const std::string& name );
			Table( NamespacePtr ns, const std::string& name, const std::string& schema, uint16_t id, ham::db* db );
			Table( NamespacePtr ns, const std::string& name, const Table& other );
			virtual ~Table( );

			inline HamsterEnv* getEnv( ) const {
				return ns->getEnv();
			}
			inline NamespacePtr getNamespace( ) {
				return ns;
			}
			inline const std::string& getName( ) const {
				return name;
			}
			const char* getFullName( ) const;
			inline const std::string& getSchemaSpec( ) const {
				return schemaSpec;
			}
			void getTableSchema( bool withIds, std::string& schema );
			Db::MutatorPtr createMutator( int32_t flags, int32_t flushInterval );
			Db::ScannerPtr createScanner( const Hypertable::ScanSpec& scanSpec, uint32_t flags );
			Hypertable::SchemaPtr getSchema( );
			inline uint16_t getId( ) const {
				return id;
			}
			inline ham::db* getDb( ) const {
				return db;
			}
			void toKey( ham::key& key );
			void toRecord( Hypertable::DynamicBuffer& buf, ham::record& record );
			void fromRecord( ham::record& record );
			void open( );
			void dispose( );

		private:

			void init( );

			Db::NamespacePtr ns;
			std::string name;
			std::string schemaSpec;
			Hypertable::SchemaPtr schema;
			std::string keyName;
			uint16_t id;
			ham::db* db;
	};

	class Mutator : public Hypertable::ReferenceCount {

		public:

			Mutator( Db::TablePtr table, int32_t flags, int32_t flushInterval );
			virtual ~Mutator( );

			inline HamsterEnv* getEnv( ) const {
				return table->getEnv();
			}
			void set( Hypertable::KeySpec& keySpec, const void* value, uint32_t valueLength );
			void set( const Hypertable::Cells& cells );
			void del( Hypertable::KeySpec& keySpec );
			void flush( );
			inline bool ignoreUnknownColumnFamily( ) const {
					return (flags & Hypertable::Table::MUTATOR_FLAG_IGNORE_UNKNOWN_CFS) > 0;
			}

		private:

			void insert( Hypertable::Key& key, const void* value, uint32_t valueLength );
			void set( Hypertable::Key& key, const void* value, uint32_t valueLength );
			void del( Hypertable::Key& key );
			void toKey( const Hypertable::Key& key, ham::key& k );
			void toKey( bool ignoreUnknownColumnFamily
								, Hypertable::Schema* schema
								, const char* row
								, const char* columnFamily
								, const char* columnQualifier
								, int64_t timestamp
								, int64_t revision
								, uint8_t flag
								, Hypertable::Key& fullKey
								, bool& unknownColumnFamily );

			inline void toKey( bool ignoreUnknownColumnFamily
											 , Hypertable::Schema* schema
											 , const Hypertable::KeySpec& key
											 , Hypertable::Key& fullKey
											 , bool& unknownColumnFamily )
			{
				toKey( ignoreUnknownColumnFamily
						 , schema
						 , reinterpret_cast<const char*>(key.row)
						 , key.column_family
						 , key.column_qualifier
						 , key.timestamp
						 , key.revision
						 , key.flag
						 , fullKey
						 , unknownColumnFamily );
			}

			inline void toKey( bool ignoreUnknownColumnFamily
											 , Hypertable::Schema* schema
											 , const Hypertable::Cell& cell
											 , Hypertable::Key& fullKey
											 , bool& unknownColumnFamily )
			{
				toKey( ignoreUnknownColumnFamily
						 , schema
						 , cell.row_key
						 , cell.column_family
						 , cell.column_qualifier
						 , cell.timestamp
						 , cell.revision
						 , cell.flag
						 , fullKey
						 , unknownColumnFamily );
			}

			Db::TablePtr table;
			int32_t flags;
			int32_t flushInterval;
			Hypertable::DynamicBuffer buf;
			ham::db* db;
			Hypertable::Schema* schema;
	};

	class MutatorAsync : public Hypertable::ReferenceCount {

		public:

			explicit MutatorAsync( Db::TablePtr table );
			virtual ~MutatorAsync( );

			inline HamsterEnv* getEnv( ) const {
				return table->getEnv();
			}
			inline void flush( ) { }

		private:

			Db::TablePtr table;
			ham::db* db;
			Hypertable::Schema* schema;
	};

	class Scanner : public Hypertable::ReferenceCount {

		public:

			Scanner( Db::TablePtr table, const Hypertable::ScanSpec& scanSpec, uint32_t flags );
			virtual ~Scanner( );

			inline HamsterEnv* getEnv( ) const {
				return table->getEnv();
			}
			bool nextCell( Hypertable::Cell& cell );

		private:

			class CellFilterInfo {

				public:

					CellFilterInfo( );
					CellFilterInfo( const CellFilterInfo& other );
					CellFilterInfo& operator = (const CellFilterInfo& other);
					~CellFilterInfo( );

					bool qualifierMatches( const char *qualifier );
					void addQualifier( const char *qualifier, bool is_regexp );
					inline bool hasQualifierFilter() const {
						return filterByExactQualifier || filterByRegexpQualifier;
					}
					inline bool hasQualifierRegexpFilter( ) const {
						return filterByRegexpQualifier;
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
					std::vector<std::string> exactQualifiers;
					typedef std::set<const char *, LtCstr> QualifierSet;
					QualifierSet exactQualifiersSet;
					bool filterByExactQualifier;
					bool filterByRegexpQualifier;
			};

			class ScanContext {

				public:

					enum {
						MAX_CF = 256
					};

					ScanContext( const Hypertable::ScanSpec& scanSpec, Hypertable::SchemaPtr schema );
					~ScanContext( );

					Hypertable::SchemaPtr schema;
					const Hypertable::ScanSpec& scanSpec;
					std::pair<int64_t, int64_t> timeInterval;
					bool familyMask[ScanContext::MAX_CF];
					const Hypertable::Schema::ColumnFamily* columnFamilies[MAX_CF];
					CellFilterInfo familyInfo[ScanContext::MAX_CF];
					re2::RE2* rowRegexp;
					re2::RE2* valueRegexp;
					typedef std::set<const char *, LtCstr, Hypertable::CstrAlloc> CstrRowSet;
					CstrRowSet rowset;
					bool keysOnly;
					int rowOffset;
					int cellOffset;
					int rowLimit;
					int cellLimit;
					int cellLimitPerFamily;
			};

			class RegexpCache {

				public:

				RegexpCache( )
				: lastFamily(-1)
				, lastRowMatch(false)
				, lastColumnMatch(false)
				{
				}

				void checkRow(const char *rowkey, bool *cached, bool *match) {
					*match = lastRowMatch;
					*cached = !strcmp( rowkey, lastRow.c_str() );
				}

				void checkColumn(int family, const char *qualifier, bool *cached, bool *match) {
					*match = lastColumnMatch;
					*cached = lastFamily == family && !strcmp( qualifier, lastQualifier.c_str() );
				}

				void setRow( const char *rowkey, bool match ) {
					lastRow = rowkey;
					lastRowMatch = match;
				}

				void setColumn( int family, const char *qualifier, bool match ) {
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

			class Reader {

				public:

					Reader( ham::cursor* cursor, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec );
					virtual ~Reader();

					bool nextCell( Hypertable::Cell& cell );

				protected:

					virtual bool moveNext( ham::key& k );
					virtual bool filterRow( ham::key& k, const char* row );
					virtual const Hypertable::Schema::ColumnFamily* filterCell( ham::key& k, const Hypertable::Key& key );
					virtual void limitReached( ) {
						eos = true;
					}
					bool getCell( const Hypertable::Key& key, const Hypertable::Schema::ColumnFamily& cf, Hypertable::Cell& cell );

					ham::cursor* cursor;
					ScanContext* scanContext;
					int rowCount;
					int cellCount;
					int cellPerFamilyCount;

				private:

					bool checkCellLimits( const Hypertable::Key& key );

					Hypertable::DynamicBuffer prevKey;
					int prevColumnFamilyCode;
					RegexpCache regexpCache;
					int revsLimit;
					int revsCount;
					bool eos;
			};

			class ReaderScanAndFilter : public Reader {

				public:

					ReaderScanAndFilter( ham::cursor* cursor, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec );

				protected:

					virtual bool moveNext( ham::key& k );
					virtual bool filterRow( ham::key& k, const char* row );

				private:

					bool nextRow;
					Hypertable::DynamicBuffer buf;
			};

			class ReaderRowIntervals : public Reader {

				public:

					ReaderRowIntervals( ham::cursor* cursor, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec );

				protected:

					virtual bool moveNext( ham::key& k );
					virtual bool filterRow( ham::key& k, const char* row );
					virtual void limitReached( ) {
						it++;
						rowIntervalDone = true;
					}

				private:

					Hypertable::DynamicBuffer buf;
					const Hypertable::ScanSpec& scanSpec;
					Hypertable::RowIntervals::const_iterator it;
					int cmpStart;
					int cmpEnd;
					bool rowIntervalDone;
			};

			class ReaderCellIntervals : public Reader {

				public:

					ReaderCellIntervals( ham::cursor* cursor, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec );

				protected:

					virtual bool moveNext( ham::key& k );
					virtual bool filterRow( ham::key& k, const char* row );
					virtual const Hypertable::Schema::ColumnFamily* filterCell( ham::key& k, const Hypertable::Key& key );
					virtual void limitReached( ) {
						it++;
						cellIntervalDone = true;
					}

				private:

					const Hypertable::ScanSpec& scanSpec;
					Hypertable::CellIntervals::const_iterator it;
					uint8_t startColumnFamilyCode;
					std::string startColumnQualifierBuf;
					const char* startColumnQualifier;
					uint8_t endColumnFamilyCode;
					std::string endColumnQualifierBuf;
					const char* endColumnQualifier;
					int cmpStart;
					int cmpEnd;
					int cmpStartRow;
					int cmpEndRow;
					bool cellIntervalDone;
					Hypertable::DynamicBuffer buf;
			};

			Db::TablePtr table;
			int32_t flags;
			ham::db* db;
			ham::cursor cursor;
			Reader* reader;
			Hypertable::ScanSpecBuilder scanSpec;
	};

	class ScannerAsync : public Hypertable::ReferenceCount {

		public:

			explicit ScannerAsync( Db::TablePtr table );
			virtual ~ScannerAsync( );

			inline HamsterEnv* getEnv( ) const {
				return table->getEnv();
			}

		private:

			Db::TablePtr table;
			ham::db* db;
	};

} } }