/** -*- C++ -*-
 * Copyright (C) 2010-2014 Thalmann Software & Consulting, http://www.softdev.ch
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
			hamsterdb::db* sysdb;
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
			void toKey( hamsterdb::key& key );

		private:

			ClientPtr client;
			HamsterEnv* env;
			hamsterdb::db* sysdb;
			std::string keyName;
	};

	class Table : public Hypertable::ReferenceCount {

		public:

			Table( NamespacePtr ns, const std::string& name );
			Table( NamespacePtr ns, const std::string& name, const std::string& schema, uint16_t id, hamsterdb::db* db );
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
			inline hamsterdb::db* getDb( ) const {
				return db;
			}
			void toKey( hamsterdb::key& key );
			void toRecord( Hypertable::DynamicBuffer& buf, hamsterdb::record& record );
			void fromRecord( hamsterdb::record& record );
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
			hamsterdb::db* db;
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

		private:

			void insert( Hypertable::Key& key, const void* value, uint32_t valueLength );
			void set( Hypertable::Key& key, const void* value, uint32_t valueLength );
			void del( Hypertable::Key& key );
			void toKey( const Hypertable::Key& key, hamsterdb::key& k );
			void toKey( Hypertable::Schema* schema
								, const char* row
								, const char* columnFamily
								, const char* columnQualifier
								, int64_t timestamp
								, int64_t revision
								, uint8_t flag
								, Hypertable::Key& fullKey );

			inline void toKey( Hypertable::Schema* schema
											 , const Hypertable::KeySpec& key
											 , Hypertable::Key& fullKey )
			{
				toKey( schema
						 , reinterpret_cast<const char*>(key.row)
						 , key.column_family
						 , key.column_qualifier
						 , key.timestamp
						 , key.revision
						 , key.flag
						 , fullKey );
			}

			inline void toKey( Hypertable::Schema* schema
											 , const Hypertable::Cell& cell
											 , Hypertable::Key& fullKey )
			{
				toKey( schema
						 , cell.row_key
						 , cell.column_family
						 , cell.column_qualifier
						 , cell.timestamp
						 , cell.revision
						 , cell.flag
						 , fullKey );
			}

			Db::TablePtr table;
			int32_t flags;
			int32_t flushInterval;
			Hypertable::DynamicBuffer buf;
			hamsterdb::db* db;
			Hypertable::Schema* schema;
			enum {
				MAX_CF = 256
			};
			bool timeOrderAsc[MAX_CF];
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
			hamsterdb::db* db;
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

			typedef Common::CellFilterInfo CellFilterInfo;
			typedef Common::RegexpCache RegexpCache;
			typedef Common::ScanContext ScanContext;

			class Reader {

				public:

					Reader( hamsterdb::cursor* cursor, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec );
					virtual ~Reader();

					bool nextCell( Hypertable::DynamicBuffer& buf, Hypertable::Key& key, Hypertable::Cell& cell );

				protected:

					virtual bool moveNext( hamsterdb::key& k );
					virtual bool filterRow( hamsterdb::key& k, const char* row );
					virtual const Hypertable::Schema::ColumnFamily* filterCell( hamsterdb::key& k, const Hypertable::Key& key );
					virtual void limitReached( ) {
						eos = true;
					}
					bool getCell( Hypertable::DynamicBuffer& buf, const Hypertable::Key& key, const Hypertable::Schema::ColumnFamily& cf, Hypertable::Cell& cell );

					hamsterdb::cursor* cursor;
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

					ReaderScanAndFilter( hamsterdb::cursor* cursor, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec );

				protected:

					virtual bool moveNext( hamsterdb::key& k );
					virtual bool filterRow( hamsterdb::key& k, const char* row );

				private:

					bool nextRow;
					Hypertable::DynamicBuffer buf;
			};

			class ReaderRowIntervals : public Reader {

				public:

					ReaderRowIntervals( hamsterdb::cursor* cursor, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec );

				protected:

					virtual bool moveNext( hamsterdb::key& k );
					virtual bool filterRow( hamsterdb::key& k, const char* row );
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

					ReaderCellIntervals( hamsterdb::cursor* cursor, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec );

				protected:

					virtual bool moveNext( hamsterdb::key& k );
					virtual bool filterRow( hamsterdb::key& k, const char* row );
					virtual const Hypertable::Schema::ColumnFamily* filterCell( hamsterdb::key& k, const Hypertable::Key& key );
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
					enum {
						MAX_CF = 256
					};
					bool timeOrderAsc[MAX_CF];
					Hypertable::DynamicBuffer buf;
			};

			Db::TablePtr table;
			int32_t flags;
			hamsterdb::db* db;
			hamsterdb::cursor cursor;
			Hypertable::DynamicBuffer buf;
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
			hamsterdb::db* db;
	};

} } }
