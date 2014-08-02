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

namespace odbc {
	class otl_connect;
}

#include "OdbcEnv.h"

namespace ht4c { namespace Odbc { namespace Db {

	namespace Util {

		class Tx;

	}

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
			friend class Namespace;

			explicit Client( OdbcEnvPtr env );
			virtual ~Client( );

			inline OdbcEnv* getEnv( ) const {
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

			void dropNamespace( Util::Tx& tx, const std::string& name, bool ifExists );
			void createNamespace( const std::string& name );

			inline odbc::otl_connect* getDb( ) {
				return getEnv()->getDb();
			}

			OdbcEnvPtr env;
	};

	class Namespace : public Hypertable::ReferenceCount {

		public:

			Namespace( ClientPtr client, const std::string& name );
			virtual ~Namespace( );

			inline OdbcEnv* getEnv( ) const {
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
			bool nameExists( bool& isTable, std::string* rowid = 0 );
			int toKey( const char*& psz );

		private:

			bool getNamespaceListing( odbc::otl_stream& os, bool deep, std::vector<Db::NamespaceListing>& listing, varbinary& k, varbinary& v );
			void drop( Util::Tx& tx, const std::string& nsName, const std::vector<Db::NamespaceListing>& listing, bool ifExists, bool dropTables );
			void dropTable( Util::Tx& tx, const std::string& name, bool ifExists );

			inline odbc::otl_connect* getDb( ) {
				return getEnv()->getDb();
			}

			ClientPtr client;
			OdbcEnv* env;
			std::string keyName;
	};

	class Table : public Hypertable::ReferenceCount {

		public:

			Table( NamespacePtr ns, const std::string& name );
			Table( NamespacePtr ns, const std::string& name, const std::string& schema, const std::string& id );
			Table( NamespacePtr ns, const std::string& name, const Table& other );
			virtual ~Table( );

			inline OdbcEnv* getEnv( ) const {
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
			bool nameExists( bool& isTable, std::string* rowid = 0 );
			inline const char* getId( ) const {
				return id.c_str();
			}
			int toKey( const char*& psz );
			void toRecord( Hypertable::DynamicBuffer& buf );
			void fromRecord( Hypertable::DynamicBuffer& buf );
			void open( );
			void refresh( );
			void dispose( );

		private:

			void init( );

			inline odbc::otl_connect* getDb( ) {
				return getEnv()->getDb();
			}

			Db::NamespacePtr ns;
			std::string name;
			std::string schemaSpec;
			Hypertable::SchemaPtr schema;
			std::string keyName;
			std::string id;
			OdbcEnv* env;
			bool opened;
	};

	class Mutator : public Hypertable::ReferenceCount {

		public:

			Mutator( Db::TablePtr table, int32_t flags, int32_t flushInterval );
			virtual ~Mutator( );

			inline OdbcEnv* getEnv( ) const {
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
			void toKey( Hypertable::Schema* schema
								, const char* row
								, int rowLen
								, const char* columnFamily
								, const char* columnQualifier
								, int columnQualifierLen
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
						 , key.row_len
						 , key.column_family
						 , key.column_qualifier
						 , key.column_qualifier_len
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
						 , cell.row_key ? strlen(cell.row_key) : 0
						 , cell.column_family
						 , cell.column_qualifier
						 , cell.column_qualifier ? strlen(cell.column_qualifier) : 0
						 , cell.timestamp
						 , cell.revision
						 , cell.flag
						 , fullKey );
			}

			inline odbc::otl_connect* getDb( ) {
				return db ? db : db = getEnv()->getDb();
			}

			Db::TablePtr table;
			int32_t flags;
			int32_t flushInterval;
			odbc::otl_connect* db;
			odbc::otl_stream* os;
			odbc::otl_stream* os_sm;
			odbc::otl_stream* os_del_row;
			odbc::otl_stream* os_del_cf;
			odbc::otl_stream* os_del_cell;
			odbc::otl_stream* os_del_cell_version;
			OdbcEnv* env;
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

			inline OdbcEnv* getEnv( ) const {
				return table->getEnv();
			}
			inline void flush( ) { }

		private:

			inline odbc::otl_connect* getDb( ) {
				return db ? db : db = getEnv()->getDb();
			}

			Db::TablePtr table;
			odbc::otl_connect* db;
			Hypertable::Schema* schema;
	};

	class Scanner : public Hypertable::ReferenceCount {

		public:

			Scanner( Db::TablePtr table, const Hypertable::ScanSpec& scanSpec, uint32_t flags );
			virtual ~Scanner( );

			inline OdbcEnv* getEnv( ) const {
				return table->getEnv();
			}
			bool nextCell( Hypertable::Cell& cell );

		private:

			typedef Common::CellFilterInfo CellFilterInfo;
			typedef Common::RegexpCache RegexpCache;

			class ScanContext : public Common::ScanContext {

				public:

					ScanContext( const Hypertable::ScanSpec& scanSpec, Hypertable::SchemaPtr schema );
					virtual void initialize( );

					std::string predicate;
					std::string cfPredicate;
					std::string qPredicate;
					std::string columns;

			protected:

				virtual void initialColumn( Hypertable::ColumnFamilySpec* cf, bool hasQualifier, bool isRegexp, bool isPrefix, const std::string& qualifier );
			};

			class Reader {

				public:

					Reader(odbc::otl_connect* db, const std::string& tableId, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec);
					virtual ~Reader();

					virtual void stmtPrepare( );
					bool nextCell( Hypertable::Cell& cell );

				protected:

					virtual bool moveNext( );
					virtual bool filterRow( const char* row );
					virtual const Hypertable::ColumnFamilySpec* filterCell( const Hypertable::Key& key );
					virtual void limitReached( ) {
						eos = true;
					}
					bool getCell( const Hypertable::Key& key, const Hypertable::ColumnFamilySpec& cf, Hypertable::Cell& cell );

					odbc::otl_connect* db;
					odbc::otl_stream* os;
					std::string tableId;
					ScanContext* scanContext;
					int rowCount;
					int cellCount;
					int cellPerFamilyCount;
					enum {
						MAX_CF = 256
					};
					bool timeOrderAsc[MAX_CF];

					varbinary r;
					varbinary cq;
					varbinary v;

				private:

					bool checkCellLimits( const Hypertable::Key& key );

					Hypertable::DynamicBuffer currkey;
					Hypertable::DynamicBuffer prevKey;
					int prevColumnFamilyCode;
					RegexpCache regexpCache;
					int revsLimit;
					int revsCount;
					bool eos;
			};

			class ReaderScanAndFilter : public Reader {

				public:

					ReaderScanAndFilter(odbc::otl_connect* db, const std::string& tableId, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec);

					virtual void stmtPrepare( );
			};

			class ReaderRowIntervals : public Reader {

				public:

					ReaderRowIntervals(odbc::otl_connect* db, const std::string& tableId, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec);

					virtual void stmtPrepare( );

				protected:

					virtual bool moveNext( );
					virtual void limitReached( ) {
						it++;
						rowIntervalDone = true;
					}

				private:

					const Hypertable::ScanSpec& scanSpec;
					Hypertable::RowIntervals::const_iterator it;
					bool rowIntervalDone;
			};

			class ReaderCellIntervals : public Reader {

				public:

					ReaderCellIntervals(odbc::otl_connect* db, const std::string& tableId, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec);

					virtual void stmtPrepare( );

				protected:

					virtual bool moveNext( );
					virtual bool filterRow( const char* row );
					virtual const Hypertable::ColumnFamilySpec* filterCell( const Hypertable::Key& key );
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
			};

			inline odbc::otl_connect* getDb( ) {
				return db ? db : db = getEnv()->getDb();
			}

			Db::TablePtr table;
			int32_t flags;
			odbc::otl_connect* db;
			Reader* reader;
			Hypertable::ScanSpecBuilder scanSpec;
	};

	class ScannerAsync : public Hypertable::ReferenceCount {

		public:

			explicit ScannerAsync( Db::TablePtr table );
			virtual ~ScannerAsync( );

			inline OdbcEnv* getEnv( ) const {
				return table->getEnv();
			}

		private:

			inline odbc::otl_connect* getDb( ) {
				return db ? db : db = getEnv()->getDb();
			}

			Db::TablePtr table;
			odbc::otl_connect* db;
	};

} } }
