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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "stdafx.h"
#include "SQLiteException.h"

namespace ht4c { namespace SQLite { namespace Db {

	Client::Client( SQLiteEnvPtr _env )
	: env( _env )
	, db( _env->getDb() )
	{
	}

	Client::~Client( ) {
		db = 0;
		env = 0;
	}

	void Client::createNamespace( const std::string& name, bool createIntermediate ) {
		if( name.empty() ) { // root "/" always exists
			HT4C_SQLITE_THROW( Hypertable::Error::NAMESPACE_EXISTS, "Root namespace '/' always exists" );
		}

		char* ns_tmp = strdup( name.c_str() );
		try {
			Util::Tx tx( env.get() );

			if( createIntermediate ) {
				int created = 0;
				char* ptr = ns_tmp;
				while( (ptr = strchr(ptr, '/')) != 0 ) {
					*ptr = 0;
					if( created > 0 || !namespaceExists(ns_tmp) ) {
						createNamespace( ns_tmp );
						++created;
					}
					*ptr++ = '/';
				}
				if( created > 0 || !namespaceExists(ns_tmp) ) {
					createNamespace( ns_tmp );
				}
				free( ns_tmp );
			}
			else {
				char* ptr = strrchr( ns_tmp, '/' );
				if( ptr ) {
					*ptr = 0;
					if( !namespaceExists(ns_tmp) ) {
						HT4C_SQLITE_THROW( Hypertable::Error::NAMESPACE_DOES_NOT_EXIST, Hypertable::format("Namespace '%s' does not exist", ns_tmp).c_str() );
					}
				}
				createNamespace( name );
			}

			tx.commit();
		}
		catch( error& e ) {
			std::string name( ns_tmp );
			free( ns_tmp );

			if( e.get_errno() == SQLITE_CONSTRAINT ) {
				HT4C_SQLITE_THROW( Hypertable::Error::NAMESPACE_EXISTS, Hypertable::format("Namespace '%s' already exists", name.c_str()).c_str() );
			}

			throw;
		}
		catch( ... ) {
			free( ns_tmp );

			throw;
		}
	}

	Db::NamespacePtr Client::openNamespace( const std::string& name ) {
		Db::NamespacePtr ns( new Db::Namespace(this, name) );
		if( name.empty() ) { // root "/" always exists
			return ns;
		}

		if( !namespaceExists(name) ) {
			HT4C_SQLITE_THROW( Hypertable::Error::NAMESPACE_DOES_NOT_EXIST, Hypertable::format("Namespace '%s' does not exist", name.c_str()).c_str() );
		}
		return ns;
	}

	bool Client::namespaceExists( const char* name ) {
		if( !name || !*name ) { // root "/" always exists
			return true;
		}

		bool exists = false;
		Db::Namespace ns( this, name );
		const char* key;
		int len = ns.toKey( key );
		return env->sysDbExists( key, len );
	}

	void Client::dropNamespace( const std::string& name, bool ifExists ) {
		Util::Tx tx( env.get() );
		dropNamespace( tx, name, ifExists );
		tx.commit();
	}

	void Client::dropNamespace( Util::Tx& /*tx*/, const std::string& name, bool ifExists ) {
		if( name.empty() ) { // root "/" always exists
			HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_DIR_NOT_EMPTY, "Cannot drop root namepsace '/'" );
		}

		Db::Namespace ns( this, name );
		const char* key;
		int len = ns.toKey( key );

		{
			sqlite3_stmt* stmtFindGt;
			int st = sqlite3_prepare_v2( db, "SELECT k FROM sys_db WHERE k>? ORDER BY k;", -1, &stmtFindGt, 0 );
			HT4C_SQLITE_VERIFY( st, db, 0 );

			Util::StmtFinalize stmt( db, &stmtFindGt );

			st = sqlite3_bind_text( stmtFindGt, 1, key, len, 0 );
			HT4C_SQLITE_VERIFY( st, db, 0 );

			st = sqlite3_step( stmtFindGt );
			HT4C_SQLITE_VERIFY( st, db, 0 );
			if( st == SQLITE_ROW ) {
				key = reinterpret_cast<const char*>( sqlite3_column_text(stmtFindGt, 0) );
				if( Util::KeyStartWith(key, KeyClassifiers::NamespaceListing, name + "/") ) {
					HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_DIR_NOT_EMPTY, Hypertable::format("Namespace '%s' is not empty", name.c_str()).c_str() );
				}
			}
		}

		// Drop
		len = ns.toKey( key );
		bool dropped = env->sysDbDelete( key, len );
		if( !ifExists && !dropped ) {
			HT4C_SQLITE_THROW( Hypertable::Error::NAMESPACE_DOES_NOT_EXIST, Hypertable::format("Namespace '%s' does not exist", name.c_str()).c_str() );
		}
	}

	void Client::createNamespace( const std::string& name ) {
		Db::Namespace ns( this, name );
		const char* key;
		int len = ns.toKey( key );
		env->sysDbInsert( key, len, 0 , 0 );
	}

	Namespace::Namespace( ClientPtr _client, const std::string& name )
	: client( _client )
	, env( _client->getEnv() )
	, db( env->getDb() )
	, keyName( ) {
		keyName.reserve( name.size() + 1 );
		keyName += KeyClassifiers::NamespaceListing;
		keyName += name;
	}

	Namespace::~Namespace( ) {
		db = 0;
		env = 0;
		client = 0;
	}

	const char* Namespace::getName( ) const {
    const char* name = keyName.c_str();
    name += KeyClassifiers::Length;
		return name;
	}

	void Namespace::drop( const std::string& nsName, const std::vector<Db::NamespaceListing>& listing, bool ifExists, bool dropTables ) {
		Util::Tx tx( env );
		drop( tx, nsName, listing, ifExists, dropTables );
		tx.commit();
	}

	void Namespace::getNamespaceListing( bool deep, std::vector<Db::NamespaceListing>& listing ) {
		listing.clear();

		sqlite3_stmt* stmtFindGt;
		int st = sqlite3_prepare_v2( db, "SELECT k, v FROM sys_db WHERE k>? ORDER BY k;", -1, &stmtFindGt, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		Util::StmtFinalize stmt( db, &stmtFindGt );

		const char* key;
		int len = toKey( key );

		st = sqlite3_bind_text( stmtFindGt, 1, key, len, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		if( (st = sqlite3_step(stmtFindGt)) == SQLITE_ROW ) {
			key = reinterpret_cast<const char*>( sqlite3_column_text(stmtFindGt, 0) );

			std::string tmp( getName() );
			if( !tmp.empty() ) {
				tmp += "/";
			}

			int len = tmp.size();
			for( const char* ptr = Util::KeyToString(key, KeyClassifiers::NamespaceListing); ptr && strstr(ptr, tmp.c_str()) == ptr; ptr = Util::KeyToString(key, KeyClassifiers::NamespaceListing) ) {
				ptr += len;
				if( !strchr(ptr, '/') ) {
					Db::NamespaceListing nsl;
					nsl.name = ptr;
					nsl.isNamespace = sqlite3_column_type( stmtFindGt, 1 ) == SQLITE_NULL;
					listing.push_back( nsl );

					if( deep ) {
						Db::Namespace ns( client, tmp + ptr );
						ns.getNamespaceListing( true, listing.back().subEntries );
					}
				}

				if( (st = sqlite3_step(stmtFindGt)) != SQLITE_ROW ) {
					break;
				}
				key = reinterpret_cast<const char*>( sqlite3_column_text(stmtFindGt, 0) );
			}
		}
		HT4C_SQLITE_VERIFY( st, db, 0 );
	}

	void Namespace::createTable( const std::string& name, const std::string& _schema ) {
		if( name.empty() ) {
			HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_BAD_PATHNAME, "Empty table name" );
		}

		Util::Tx tx( env );

		// Table already exists?
		Db::Table table( this, name );
		const char* key;
		int len = table.toKey( key );
		if( env->sysDbExists(key, len) ) {
			HT4C_SQLITE_THROW( Hypertable::Error::NAME_ALREADY_IN_USE, Hypertable::format("Name '%s' already in use", name.c_str()).c_str() );
		}

		// Namespace exists?
		len = toKey( key );
		if( !env->sysDbExists(key, len) ) {
			HT4C_SQLITE_THROW( Hypertable::Error::NAMESPACE_DOES_NOT_EXIST, Hypertable::format("Namespace '%s' does not exist", getName()).c_str() );
		}

		// Validate schema
		Hypertable::SchemaPtr schema = Hypertable::Schema::new_instance(_schema, _schema.length());
		if( !schema->is_valid() ) {
			HT4C_SQLITE_THROW( Hypertable::Error::MASTER_BAD_SCHEMA, Hypertable::format("Invalid table schema '%s'", _schema.c_str()).c_str() );
		}

		if( schema->need_id_assignment() ) {
			schema->assign_ids();
		}

		std::string finalschema;
		schema->render( finalschema, true );

		Db::Table newTable( this, name, finalschema, 0 );
		len = newTable.toKey( key );
		Hypertable::DynamicBuffer buf;
		newTable.toRecord( buf );
		int64_t id;
		env->sysDbCreateTable( key, len, buf.base, buf.fill(), id );

		tx.commit();
	}

	Db::TablePtr Namespace::openTable( const std::string& name ) {
		Util::Tx tx( env );

		Db::TablePtr table = new Db::Table( this, name );
		table->open( );

		tx.commit();

		return table;
	}

	void Namespace::alterTable( const std::string& name, const std::string& _schema ) {
		if( name.empty() ) {
			HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_BAD_PATHNAME, "Empty table name" );
		}

		Util::Tx tx( env );

		// Table already exists?
		Db::Table table( this, name );
		const char* key;
		int len = table.toKey( key );
		int64_t rowid;
		if( !env->sysDbExists(key, len, &rowid) ) {
			HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_FILE_NOT_FOUND, Hypertable::format("Table '%s' does not exist", name.c_str()).c_str() );
		}

		// Validate schema
		Hypertable::SchemaPtr schema = Hypertable::Schema::new_instance(_schema, _schema.length());
		if( !schema->is_valid() ) {
			HT4C_SQLITE_THROW( Hypertable::Error::MASTER_BAD_SCHEMA, Hypertable::format("Invalid table schema '%s'", _schema.c_str()).c_str() );
		}

		if( schema->need_id_assignment() ) {
			schema->assign_ids();
		}

		std::string finalschema;
		schema->render( finalschema, true );

		Db::Table alterTable( this, name, finalschema, rowid );
		len = alterTable.toKey( key );
		Hypertable::DynamicBuffer buf;
		alterTable.toRecord( buf );
		env->sysDbUpdateValue( alterTable.getId(), buf.base, buf.fill() );

		tx.commit();
	}

	bool Namespace::tableExists( const std::string& name ) {
		if( name.empty() ) {
			return true;
		}

		Db::Table table( this, name );
		const char* key;
		int len = table.toKey( key );
		return env->sysDbExists( key, len );
	}

	void Namespace::getTableId( const std::string& name, std::string& id ) {
		Db::Table table( this, name );
		const char* key;
		int len = table.toKey( key );
		int64_t rowid;
		if( !env->sysDbExists(key, len, &rowid) ) {
			HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_FILE_NOT_FOUND, Hypertable::format("Table '%s' does not exist", name.c_str()).c_str() );
		}
		id = Hypertable::format( "%lld", rowid );
	}

	void Namespace::getTableSchema( const std::string& name, bool withIds, std::string& _schema ) {
		_schema.clear();

		Db::Table table( this, name );
		const char* key;
		int len = table.toKey( key );
		Hypertable::DynamicBuffer buf;
		if( !env->sysDbRead(key, len, buf) ) {
			HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_FILE_NOT_FOUND, Hypertable::format("Table '%s' does not exist", name.c_str()).c_str() );
		}

		table.fromRecord( buf );

		if( withIds ) {
			_schema = table.getSchemaSpec();
		}
		else {
			Hypertable::SchemaPtr schema = table.getSchema();
			schema->render( _schema, false );
		}
	}

	void Namespace::renameTable( const std::string& name,const std::string& newName ) {
		if( name.empty() ) {
			HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_BAD_PATHNAME, "Empty table name" );
		}
		if( newName.empty() ) {
			HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_BAD_PATHNAME, "Empty new table name" );
		}

		Util::Tx tx( env );

		{
			// New table already exists?
			Db::Table table( this, newName );
			const char* key;
			int len = table.toKey( key );
			if( env->sysDbExists(key, len) ) {
				HT4C_SQLITE_THROW( Hypertable::Error::NAME_ALREADY_IN_USE, Hypertable::format("Name '%s' already in use", newName.c_str()).c_str() );
			}
		}

		Db::Table table( this, name );
		const char* key;
		int len = table.toKey( key );
		int64_t rowid;
		if( !env->sysDbExists(key, len, &rowid) ) {
			HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_FILE_NOT_FOUND, Hypertable::format("Table '%s' does not exist", name.c_str()).c_str() );
		}

		Db::Table tableNew( this, newName, table );
		len = tableNew.toKey( key );
		env->sysDbUpdateKey( rowid, key, len );

		tx.commit();
	}

	void Namespace::dropTable( const std::string& name, bool ifExists ) {
		Util::Tx tx( env );
		dropTable( tx, name, ifExists );
		tx.commit();
	}

	void Namespace::drop( Util::Tx& tx, const std::string& nsName, const std::vector<Db::NamespaceListing>& listing, bool ifExists, bool dropTables ) {
		for( std::vector<Db::NamespaceListing>::const_iterator it = listing.begin(); it != listing.end(); ++it ) {
			if( (*it).isNamespace ) {
				std::string nsSubName = nsName + "/" + (*it).name;
				Db::NamespacePtr nsSub = client->openNamespace( nsSubName );
				nsSub->drop( tx, nsSubName, (*it).subEntries, ifExists, dropTables );
				client->dropNamespace( tx, nsSubName, ifExists );
			}
			else if( dropTables ) {
				dropTable( tx, (*it).name, true );
			}
		}
	}

	void Namespace::dropTable( Util::Tx& /*tx*/, const std::string& name, bool ifExists ) {
		if( name.empty() ) {
			HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_BAD_PATHNAME, "Empty table name" );
		}

		// Drop
		Db::Table table( this, name );
		const char* key;
		int len = table.toKey( key );
		if( !env->sysDbDeleteTable(key, len) ) {
			if( !ifExists ) {
				HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_FILE_NOT_FOUND, Hypertable::format("Table '%s' does not exist", name.c_str()).c_str() );
			}
		}
	}

	int Namespace::toKey( const char*& psz ) {
		psz = keyName.c_str();
		return keyName.size() + 1;
	}

	Table::Table( NamespacePtr _ns, const std::string& _name )
	: ns( _ns )
	, name( _name )
	, id( 0 )
	, env( _ns->getEnv() )
	, db( _ns->getEnv()->getDb() ) {
		init();
	}

	Table::Table( NamespacePtr _ns, const std::string& _name, const std::string& _schema, int64_t _id )
	: ns( _ns )
	, name( _name )
	, schemaSpec( _schema )
	, id( _id )
	, env( _ns->getEnv() )
	, db( 0 ) {
		init();
	}

	Table::Table( NamespacePtr _ns, const std::string& _name, const Table& other )
	: ns( _ns )
	, name( _name )
	, schemaSpec( other.schemaSpec )
	, schema( other.schema )
	, id( other.id )
	, env( _ns->getEnv() )
	, db( 0 ) {
		init();
	}

	Table::~Table( ) {
		dispose();
	}

	const char* Table::getFullName( ) const {
    const char* name = keyName.c_str();
    name += KeyClassifiers::Length;
		return name;
	}

	void Table::getTableSchema( bool withIds, std::string& _schema ) {
		 getNamespace()->getTableSchema( getName(), withIds, _schema );
	}

	Db::MutatorPtr Table::createMutator( int32_t flags, int32_t flushInterval ) {
		if( !id ) {
			HT4C_SQLITE_THROW( Hypertable::Error::TABLE_NOT_FOUND, Hypertable::format("Invalid identifier for table '%s'", getFullName()).c_str() );
		}
		if( !db ) {
			HT4C_SQLITE_THROW( Hypertable::Error::TABLE_NOT_FOUND, Hypertable::format("Table '%s' already disposed", getFullName()).c_str() );
		}
		return new Db::Mutator( this, flags, flushInterval );
	}

	Db::ScannerPtr Table::createScanner( const Hypertable::ScanSpec& scanSpec, uint32_t flags) {
		if( !id ) {
			HT4C_SQLITE_THROW( Hypertable::Error::TABLE_NOT_FOUND, Hypertable::format("Invalid identifier for table '%s'", getFullName()).c_str() );
		}
		if( !db ) {
			HT4C_SQLITE_THROW( Hypertable::Error::TABLE_NOT_FOUND, Hypertable::format("Table '%s' already disposed", getFullName()).c_str() );
		}
		return new Db::Scanner( this, scanSpec, flags );
	}

	Hypertable::SchemaPtr Table::getSchema() {
		if( !schema ) {
			schema = Hypertable::Schema::new_instance( schemaSpec, schemaSpec.length() );
		}
		return schema;
	}

	int Table::toKey( const char*& psz ) {
		psz = keyName.c_str(); 
		return keyName.size() + 1;
	}

	void Table::toRecord( Hypertable::DynamicBuffer& buf ) {
		if( schemaSpec.empty() ) {
			HT4C_SQLITE_THROW( Hypertable::Error::MASTER_BAD_SCHEMA, Hypertable::format("Undefined schema for table '%s'", getFullName()).c_str() );
		}

		size_t schema_len = schemaSpec.size() + 1;
		buf.clear();
		buf.reserve( schema_len );
		buf.add( schemaSpec.c_str(), schema_len );
	}

	void Table::fromRecord( Hypertable::DynamicBuffer& buf ) {
		if( buf.fill() ) {
			schemaSpec = (const char*)buf.base;
		}
		else {
			schemaSpec.empty();
			dispose();
		}
	}

	void Table::open( ) {
		dispose();
		if( !env->sysDbOpenTable(this, id) ) {
			HT4C_SQLITE_THROW( Hypertable::Error::HYPERSPACE_FILE_NOT_FOUND, Hypertable::format("Table '%s' does not exist", name.c_str()).c_str() );
		}
		db = env->getDb();
	}

	void Table::dispose( ) {
		if( db ) {
			env->sysDbDisposeTable( id );
			db = 0;
		}
		id = 0;
	}

	void Table::init( ) {
		db = env->getDb();
		keyName.reserve( strlen(ns->getName()) + 1 + name.size() + 1 );
		keyName += KeyClassifiers::NamespaceListing;
		keyName += ns->getName();
		keyName += "/";
		keyName += name;
	}

	Mutator::Mutator( Db::TablePtr _table, int32_t _flags, int32_t _flushInterval )
	: table( _table )
	, flags( _flags )
	, flushInterval( _flushInterval )
	, db( _table->getEnv()->getDb() )
	, env( _table->getEnv() )
	, schema( _table->getSchema().get() )
	, stmtInsert( 0 )
	, stmtDeleteRow( 0 )
	, stmtDeleteCf( 0 )
	, stmtDeleteCell( 0 )
	, stmtDeleteCellVersion( 0 ) {

		int st = sqlite3_prepare_v2( db, Hypertable::format("INSERT OR REPLACE INTO t%lld (r, cf, cq, ts, v) VALUES(?, ?, ?, ?, ?);", table->getId()).c_str(), -1, &stmtInsert, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_prepare_v2( db, Hypertable::format("DELETE FROM t%lld WHERE r=? AND ts>?;", table->getId()).c_str(), -1, &stmtDeleteRow, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_prepare_v2( db, Hypertable::format("DELETE FROM t%lld WHERE r=? AND cf=? AND ts>?;", table->getId()).c_str(), -1, &stmtDeleteCf, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_prepare_v2( db, Hypertable::format("DELETE FROM t%lld WHERE r=? AND cf=? AND cq=? AND ts>?;", table->getId()).c_str(), -1, &stmtDeleteCell, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_prepare_v2( db, Hypertable::format("DELETE FROM t%lld WHERE r=? AND cf=? AND cq=? AND ts=?;", table->getId()).c_str(), -1, &stmtDeleteCellVersion, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );
	}

	Mutator::~Mutator( ) {
		Util::stmt_finalize( db, &stmtInsert );
		Util::stmt_finalize( db, &stmtDeleteRow );
		Util::stmt_finalize( db, &stmtDeleteCf );
		Util::stmt_finalize( db, &stmtDeleteCell );
		Util::stmt_finalize( db, &stmtDeleteCellVersion );

		schema = 0;
		db = 0;
		table = 0;
	}

	void Mutator::set( Hypertable::KeySpec& keySpec, const void* value, uint32_t valueLength ) {
		keySpec.sanity_check();

		Hypertable::Key key;
		bool unknownColumnFamily;
		toKey( ignoreUnknownColumnFamily(), schema, keySpec, key, unknownColumnFamily );
		if( ignoreUnknownColumnFamily() && unknownColumnFamily ) {
			return;
		}

		set( key, value, valueLength );
	}

	void Mutator::set( const Hypertable::Cells& cells ) {
		bool ignoreUnknownColumnFamily( ignoreUnknownColumnFamily() );
		bool unknownColumnFamily;

		for each( const Hypertable::Cell& cell in cells ) {
			cell.sanity_check();

			Hypertable::Key key;
			toKey( ignoreUnknownColumnFamily
					 , schema
					 , cell
					 , key
					 , unknownColumnFamily );

			if( !(ignoreUnknownColumnFamily && unknownColumnFamily) ) {
				set( key, cell.value, cell.value_len );
			}
		}
	}

	void Mutator::del( Hypertable::KeySpec& keySpec ) {
		keySpec.sanity_check();

		Hypertable::Key key;
		bool unknownColumnFamily;
		toKey( ignoreUnknownColumnFamily(), schema, keySpec, key, unknownColumnFamily );
		if( ignoreUnknownColumnFamily() && unknownColumnFamily ) {
			return;
		}

		if( key.flag == Hypertable::FLAG_INSERT ) {
			HT4C_SQLITE_THROW( Hypertable::Error::BAD_KEY, Hypertable::format("Invalid delete flag '%d'", key.flag).c_str() );
		}

		del( key );
	}

	void Mutator::flush( ) {
		env->txCommit();
	}

	void Mutator::insert( Hypertable::Key& key, const void* value, uint32_t valueLength ) {
		Util::StmtReset stmt( stmtInsert );

		int st = sqlite3_bind_text( stmtInsert, 1, key.row, key.row_len, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_bind_int( stmtInsert, 2, key.column_family_code );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_bind_text( stmtInsert, 3, Util::CQ(key.column_qualifier), key.column_qualifier_len, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_bind_int64( stmtInsert, 4, ~key.timestamp ); // ascending
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_bind_blob( stmtInsert, 5, value, valueLength, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		st = sqlite3_step( stmtInsert );
		HT4C_SQLITE_VERIFY( st, db, 0 );
	}

	void Mutator::toKey( bool ignoreUnknownColumnFamily
										 , Hypertable::Schema* schema
										 , const char *row
										 , int rowLen
										 , const char *columnFamily
										 , const char *columnQualifier
										 , int columnQualifierLen
										 , int64_t timestamp
										 , int64_t revision
										 , uint8_t flag
										 , Hypertable::Key &fullKey
										 , bool &unknownColumnFamily )
	{
		unknownColumnFamily = false;

		if( flag > Hypertable::FLAG_DELETE_ROW ) {
			if( !columnFamily ) {
				HT4C_SQLITE_THROW( Hypertable::Error::BAD_KEY, "Column family not specified" );
			}

			Hypertable::Schema::ColumnFamily* cf = schema->get_column_family( columnFamily );
			if( !cf ) {
				unknownColumnFamily = true;
				if( ignoreUnknownColumnFamily ) {
					return;
				}
				HT4C_SQLITE_THROW( Hypertable::Error::BAD_KEY, Hypertable::format("Bad column family '%s'", columnFamily).c_str() );
			}
			fullKey.column_family_code = (uint8_t)cf->id;
		}
		else {
			fullKey.column_family_code = 0;
		}

		if( timestamp == Hypertable::AUTO_ASSIGN ) {
			static Hypertable::HRTimer timer;
			static int64_t prevTimestamp = Hypertable::TIMESTAMP_NULL;
			boost::xtime now;
			boost::xtime_get( &now, boost::TIME_UTC );
			timestamp = ((int64_t)now.sec * 1000000000LL) + (int64_t)now.nsec;
			if( prevTimestamp >= timestamp ) {
				int64_t elapsed = timer.peek_ns( true );
				timestamp = elapsed ? prevTimestamp + elapsed : prevTimestamp + 1;
			}
			else {
				timer.reset();
			}
			prevTimestamp = timestamp;
		}

		fullKey.row = row;
		fullKey.row_len = rowLen;
		fullKey.column_qualifier = columnQualifier;
		fullKey.column_qualifier_len = columnQualifierLen;
		fullKey.timestamp = timestamp;
		fullKey.revision = revision;
		fullKey.flag = flag;
	}

	void Mutator::set( Hypertable::Key& key, const void* value, uint32_t valueLength ) {
		env->txBegin();

		if( key.flag == Hypertable::FLAG_INSERT ) {
			insert( key, value, valueLength );
		}
		else {
			del( key );
		}
	}

	void Mutator::del( Hypertable::Key& key ) {
		int st;

		switch( key.flag ) {
			case Hypertable::FLAG_DELETE_ROW: {
				Util::StmtReset stmt( stmtDeleteRow );

				st = sqlite3_bind_text( stmtDeleteRow, 1, key.row, key.row_len, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_bind_int64( stmtDeleteRow, 2, ~key.timestamp );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_step( stmtDeleteRow );
				HT4C_SQLITE_VERIFY( st, db, 0 );
				break;
			}

			case Hypertable::FLAG_DELETE_COLUMN_FAMILY: {
				Util::StmtReset stmt( stmtDeleteCf );

				st = sqlite3_bind_text( stmtDeleteCf, 1, key.row, key.row_len, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_bind_int( stmtDeleteCf, 2, key.column_family_code );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_bind_int64( stmtDeleteCf, 3, ~key.timestamp );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_step( stmtDeleteCf );
				HT4C_SQLITE_VERIFY( st, db, 0 );
				break;
			}

			case Hypertable::FLAG_DELETE_CELL: {
				Util::StmtReset stmt( stmtDeleteCell );

				st = sqlite3_bind_text( stmtDeleteCell, 1, key.row, key.row_len, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_bind_int( stmtDeleteCell, 2, key.column_family_code );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_bind_text( stmtDeleteCell, 3, Util::CQ(key.column_qualifier), key.column_qualifier_len, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_bind_int64( stmtDeleteCell, 4, ~key.timestamp );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_step( stmtDeleteCell );
				HT4C_SQLITE_VERIFY( st, db, 0 );
				break;
			}

			case Hypertable::FLAG_DELETE_CELL_VERSION: {
				Util::StmtReset stmt( stmtDeleteCellVersion );

				st = sqlite3_bind_text( stmtDeleteCellVersion, 1, key.row, key.row_len, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_bind_int( stmtDeleteCellVersion, 2, key.column_family_code );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_bind_text( stmtDeleteCellVersion, 3, Util::CQ(key.column_qualifier), key.column_qualifier_len, 0 );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_bind_int64( stmtDeleteCellVersion, 4, ~key.timestamp );
				HT4C_SQLITE_VERIFY( st, db, 0 );

				st = sqlite3_step( stmtDeleteCellVersion );
				HT4C_SQLITE_VERIFY( st, db, 0 );
				break;
			}
		}
	}

	MutatorAsync::MutatorAsync( Db::TablePtr _table )
	: table( _table )
	, db( _table->getEnv()->getDb() )
	, schema( _table->getSchema().get() ) {
	}

	MutatorAsync::~MutatorAsync( ) {
		schema = 0;
		db = 0;
		table = 0;
	}

	Scanner::Scanner( Db::TablePtr _table, const Hypertable::ScanSpec& _scanSpec, uint32_t _flags )
	: table( _table )
	, flags( _flags )
	, db( _table->getEnv()->getDb() )
	, scanSpec( _scanSpec )
	{
		if( scanSpec.get().row_intervals.empty() ) {
			if( scanSpec.get().cell_intervals.empty() ) {
				reader = new Reader( db, _table->getId(), _table->getSchema(), scanSpec.get() );
			}
			else {
				Hypertable::CellIntervals& cellIntervals = scanSpec.get().cell_intervals;
				for( Hypertable::CellIntervals::iterator ci = cellIntervals.begin(); ci != cellIntervals.end(); ++ci ) {
					if( !ci->start_row ) {
						ci->start_row = "";
					}
					if( !ci->end_row || !*ci->end_row ) {
						ci->end_row = Hypertable::Key::END_ROW_MARKER;
					}
				}
				reader = new ReaderCellIntervals( db, _table->getId(), _table->getSchema(), scanSpec.get() );
			}
		}
		else if (scanSpec.get().scan_and_filter_rows) {
			reader = new ReaderScanAndFilter( db, _table->getId(), _table->getSchema(), scanSpec.get() );
		}
		else {
			Hypertable::RowIntervals& rowIntervals = scanSpec.get().row_intervals;
			for( Hypertable::RowIntervals::iterator ri = rowIntervals.begin(); ri != rowIntervals.end(); ++ri ) {
				if( !ri->start ) {
					ri->start = "";
				}
				if( !ri->end || !*ri->end ) {
					ri->end = Hypertable::Key::END_ROW_MARKER;
				}
			}

			reader = new ReaderRowIntervals( db, _table->getId(), _table->getSchema(), scanSpec.get() );
		}

		reader->stmtPrepare( );
	}

	Scanner::~Scanner( ) {
		if( reader ) {
			delete reader;
			reader = 0;
		}
		db = 0;
	}

	bool Scanner::nextCell( Hypertable::Cell& cell ) {
		return reader->nextCell( cell );
	}

	Scanner::CellFilterInfo::CellFilterInfo( )
	: cutoffTime(0)
	, maxVersions(0)
	, filterByExactQualifier(false)
	, filterByRegexpQualifier(false)
	{
	}

	Scanner::CellFilterInfo::CellFilterInfo( const CellFilterInfo& other ) {
		cutoffTime = other.cutoffTime;
		maxVersions = other.maxVersions;
		for each( re2::RE2* re in other.regexpQualifiers ) {
			regexpQualifiers.push_back( new re2::RE2(re->pattern()) );
		}
		exactQualifiers = other.exactQualifiers;
		for each( const std::string& q in exactQualifiers ) {
			exactQualifiersSet.insert( q.c_str() );
		}
		filterByExactQualifier = other.filterByExactQualifier;
		filterByRegexpQualifier = other.filterByRegexpQualifier;
	}

	Scanner::CellFilterInfo& Scanner::CellFilterInfo::operator = ( const CellFilterInfo& other ) {
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
		filterByExactQualifier = other.filterByExactQualifier;
		filterByRegexpQualifier = other.filterByRegexpQualifier;

		return *this;
	}

	Scanner::CellFilterInfo::~CellFilterInfo() {
		for each( re2::RE2* re in regexpQualifiers ) {
			delete re;
		}
		regexpQualifiers.clear();
	}

	bool Scanner::CellFilterInfo::qualifierMatches( const char *qualifier ) {
		if( !filterByExactQualifier && !filterByRegexpQualifier ) {
			return true;
		}

		// check exact match first
		if( exactQualifiersSet.find(qualifier) != exactQualifiersSet.end() ) {
			return true;
		}
		// check for regexp match
		for each( re2::RE2* re in regexpQualifiers ) {
			if( RE2::PartialMatch(qualifier, *re) ) {
				return true;
			}
		}
		return false;
	}

	void Scanner::CellFilterInfo::addQualifier(const char *qualifier, bool is_regexp) {
		if( is_regexp ) {
			re2::RE2* regexp = new re2::RE2( qualifier );
			if( !regexp->ok() ) {
				HT4C_SQLITE_THROW( Hypertable::Error::BAD_SCAN_SPEC, Hypertable::format("Can't convert qualifier '%s' to regexp (%s)", qualifier, regexp->error_arg()).c_str() );
			}
			regexpQualifiers.push_back( regexp );
			filterByRegexpQualifier = true;
		}
		else {
			exactQualifiers.push_back( qualifier );
			exactQualifiersSet.insert( exactQualifiers.back().c_str() );
			filterByExactQualifier = true;
		}
	}

	Scanner::ScanContext::ScanContext( const Hypertable::ScanSpec& _scanSpec, Hypertable::SchemaPtr _schema )
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
		boost::xtime_get( &xtnow, boost::TIME_UTC );
		int64_t now = ((int64_t)xtnow.sec * 1000000000LL) + (int64_t)xtnow.nsec;
		uint32_t maxVersions = scanSpec.max_versions;

		if( scanSpec.columns.size() > 0 ) {
			std::string family, qualifier;
			bool hasQualifier, isRegexp;

			for each( const char *cfstr in scanSpec.columns ) {
				Hypertable::ScanSpec::parse_column( cfstr, family, qualifier, &hasQualifier, &isRegexp );
				Hypertable::Schema::ColumnFamily* cf = schema->get_column_family( family.c_str() );

				if( cf == 0 ) {
					HT4C_SQLITE_THROW( Hypertable::Error::RANGESERVER_INVALID_COLUMNFAMILY, Hypertable::format("Invalid column family '%s'", cfstr).c_str() );
				}
				if( cf->id == 0 ) {
					HT4C_SQLITE_THROW( Hypertable::Error::RANGESERVER_SCHEMA_INVALID_CFID, Hypertable::format("Bad id for column family '%s'", cf->name.c_str()).c_str() );
				}
				if( cf->counter ) {
					HT4C_SQLITE_THROW( Hypertable::Error::BAD_SCAN_SPEC, "Counters are not yet supported" );
				}

				columnFamilies[cf->id] = cf;
				familyMask[cf->id] = true;
				if( hasQualifier ) {
					familyInfo[cf->id].addQualifier( qualifier.c_str(), isRegexp );
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

				if( !hasQualifier || isRegexp ) {
					predicate += Hypertable::format("%scf=%d", predicate.empty() ? "" : " OR ", cf->id ); //TODO use cf IN ('A', 'B')
				}
				else {
					predicate += Hypertable::format("%s(cf=%d AND cq=\"%s\")", predicate.empty() ? "" : " OR ", cf->id, qualifier.c_str() ); //TODO use cf IN ('A', 'B')
				}
			}
		}
		else {
			Hypertable::Schema::AccessGroups& aglist = schema->get_access_groups();

			// ROW_DELETE records have 0 column family, so this allows them to pass through
			familyMask[0] = true;
			for( Hypertable::Schema::AccessGroups::iterator ag = aglist.begin(); ag != aglist.end(); ++ag ) {
				for( Hypertable::Schema::ColumnFamilies::iterator cf = (*ag)->columns.begin(); cf != (*ag)->columns.end(); ++cf ) {
					if( (*cf)->id == 0 ) {
						HT4C_SQLITE_THROW( Hypertable::Error::RANGESERVER_SCHEMA_INVALID_CFID, Hypertable::format("Bad id for column family '%s'", (*cf)->name.c_str()).c_str() );
					}
					if( (*cf)->deleted ) {
						familyMask[(*cf)->id] = false;
						continue;
					}
					if( (*cf)->counter ) {
						HT4C_SQLITE_THROW( Hypertable::Error::BAD_SCAN_SPEC, "Counters are not yet supported" );
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

		if( scanSpec.row_regexp && *scanSpec.row_regexp ) {
			rowRegexp = new re2::RE2( scanSpec.row_regexp );
			if( !rowRegexp->ok() ) {
				HT4C_SQLITE_THROW( Hypertable::Error::BAD_SCAN_SPEC, Hypertable::format("Can't convert row regexp %s to regexp (%s)", scanSpec.row_regexp, rowRegexp->error_arg()).c_str() );
			}
		}
		if( scanSpec.value_regexp && *scanSpec.value_regexp ) {
			valueRegexp = new re2::RE2( scanSpec.value_regexp );
			if( !valueRegexp->ok() ) {
				HT4C_SQLITE_THROW( Hypertable::Error::BAD_SCAN_SPEC, Hypertable::format("Can't convert value regexp %s to regexp (%s)", scanSpec.value_regexp, valueRegexp->error_arg()).c_str() );
			}
		}

		std::string predicateTimestamp;
		if( timeInterval.first > Hypertable::TIMESTAMP_MIN ) {
			predicateTimestamp = Hypertable::format("ts<=%lld", ~timeInterval.first);
		}

		if( timeInterval.second < Hypertable::TIMESTAMP_MAX ) {
			predicateTimestamp += Hypertable::format("%sts>%lld", predicateTimestamp.empty() ? "" : " AND ", ~timeInterval.second);
		}

		if( !predicateTimestamp.empty() ) {
			predicate = predicate.empty() ? predicateTimestamp : Hypertable::format("%s AND (%s)", predicateTimestamp.c_str(), predicate.c_str() );
		}

		columns = "r, cf, cq, ts";
		if( !keysOnly ) {
			columns += ", v";
		}
	}

	Scanner::ScanContext::~ScanContext() {
		if( rowRegexp ) {
			delete rowRegexp;
			rowRegexp = 0;
		}
		if( valueRegexp ) {
			delete valueRegexp;
			valueRegexp = 0;
		}
	}

	Scanner::Reader::Reader( sqlite3* _db, int64_t _tableId, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec )
	: db( _db )
	, tableId( _tableId )
	, stmtQuery( 0 )
	, scanContext( new ScanContext(scanSpec, schema) )
	, currkey( 64 )
	, prevKey( 64 )
	, prevColumnFamilyCode( -1 )
	, revsLimit( 0 )
	, revsCount( 0 )
	, rowCount( 0 )
	, cellCount( 0 )
	, cellPerFamilyCount( 0 )
	, eos( false )
	{
		int st = sqlite3_prepare_v2( db, Hypertable::format("DELETE FROM t%lld WHERE r=? AND cf=? AND ts>?;", tableId).c_str(), -1, &stmtDeleteCf, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );
	}

	Scanner::Reader::~Reader() {
		delete scanContext;
		scanContext = 0;

		Util::stmt_finalize( db, &stmtQuery );
		Util::stmt_finalize( db, &stmtDeleteCf );
		db = 0;
	}

	bool Scanner::Reader::nextCell( Hypertable::Cell& cell ) {
		Hypertable::Key key;
		for( bool moved = moveNext(); moved && !eos; moved = moveNext() ) {
			key.row = reinterpret_cast<const char*>( sqlite3_column_text(stmtQuery, 0) );
			key.row_len = sqlite3_column_bytes( stmtQuery, 0 );
			if( filterRow(key.row) ) {
				key.column_family_code = sqlite3_column_int( stmtQuery, 1 );
				key.column_qualifier = reinterpret_cast<const char*>( sqlite3_column_text(stmtQuery, 2) );
				key.column_qualifier_len = sqlite3_column_bytes( stmtQuery, 2 );
				key.timestamp = ~sqlite3_column_int64( stmtQuery, 3 ); // ascending
				const Hypertable::Schema::ColumnFamily* cf = filterCell( key );
				if( cf ) {
					if( getCell(key, *cf, cell) ) {
						return true;
					}
				}
			}
		}

		return false;
	}

	void Scanner::Reader::stmtPrepare( ) {
		Util::stmt_finalize( db, &stmtQuery );

		std::string predicate;
		if( !scanContext->predicate.empty() ) {
			predicate = " WHERE " + scanContext->predicate;
		}

		std::string select = Hypertable::format( "SELECT %s FROM t%lld%s ORDER BY r, cf, cq, ts;", scanContext->columns.c_str(), tableId, predicate.c_str() );
		int st = sqlite3_prepare_v2( db, select.c_str(), -1, &stmtQuery, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 );
	}

	bool Scanner::Reader::moveNext( ) {
		int st = sqlite3_step( stmtQuery );
		HT4C_SQLITE_VERIFY( st, db, 0 );
		return st == SQLITE_ROW;
	}

	bool Scanner::Reader::filterRow( const char* row ) {
		// row set
		if( !scanContext->rowset.empty() ) {
			int cmp = 1;
			while( !scanContext->rowset.empty() && (cmp = strcmp(*scanContext->rowset.begin(), row)) < 0 ) {
				scanContext->rowset.erase( scanContext->rowset.begin() );
			}
			if( scanContext->rowset.empty() || cmp > 0 ) {
				return false;
			}
		}

		// row regexp
		if( scanContext->rowRegexp ) {
			bool cached, match;
			regexpCache.checkRow( row, &cached, &match );
			if( !cached ) {
				match = RE2::PartialMatch( row, *(scanContext->rowRegexp) );
				regexpCache.setRow( row, match );
			}
			if( !match ) {
				return false;
			}
		}

		return true;
	}

	const Hypertable::Schema::ColumnFamily* Scanner::Reader::filterCell( const Hypertable::Key& key ) {
		if( !scanContext->familyMask[key.column_family_code] ) {
			return 0;
		}

		CellFilterInfo& cfi = scanContext->familyInfo[key.column_family_code];

		// cutoff time
		if( key.timestamp < cfi.cutoffTime ) {
			Util::StmtReset stmt( stmtDeleteCf );

			int st = sqlite3_bind_text( stmtDeleteCf, 1, key.row, key.row_len, 0 );
			HT4C_SQLITE_VERIFY( st, db, 0 );

			st = sqlite3_bind_int( stmtDeleteCf, 2, key.column_family_code );
			HT4C_SQLITE_VERIFY( st, db, 0 );

			st = sqlite3_bind_int64( stmtDeleteCf, 3, ~key.timestamp );
			HT4C_SQLITE_VERIFY( st, db, 0 );

			st = sqlite3_step( stmtDeleteCf );
			HT4C_SQLITE_VERIFY( st, db, 0 );

			return 0;
		}

		// timestamp
		if(  key.timestamp < scanContext->timeInterval.first
			|| key.timestamp >= scanContext->timeInterval.second ) {

			return 0;
		}

		// keep track of revisions
		if( scanContext->rowOffset || scanContext->rowLimit || cfi.maxVersions ) {
			currkey.clear();
			currkey.add( key.row, key.row_len + 1 );
			currkey.add( &key.column_family_code, sizeof(key.column_family_code) );
			currkey.add( key.column_qualifier, key.column_qualifier_len + 1 );

			// row changes
			if( !prevKey.fill() || strcmp(reinterpret_cast<const char*>(prevKey.base), key.row) ) {
				prevKey.set( currkey.base, currkey.fill() );
				revsCount = 0;
				revsLimit = cfi.maxVersions;

				cellPerFamilyCount = 0;
				++rowCount;
			}
			// cell changes
			else if( prevKey.fill() != currkey.fill() || memcmp(currkey.base, prevKey.base, currkey.fill()) ) {
				prevKey.set( currkey.base, currkey.fill() );
				revsCount = 0;
				revsLimit = cfi.maxVersions;
			}

			// row offset
			if( scanContext->rowOffset && rowCount <= scanContext->rowOffset ) {
				return 0;
			}

			// row limit
			if( scanContext->rowLimit && rowCount > scanContext->rowLimit ) {
				limitReached( );
				return 0;
			}

			// revision limit
			++revsCount;
			if( revsLimit && revsCount > revsLimit ) {
				return 0;
			}
		}

		// column qualifier match
		if( cfi.hasQualifierRegexpFilter() ) {
			bool cached, match;
			regexpCache.checkColumn( key.column_family_code, key.column_qualifier, &cached, &match );
			if( !cached ) {
				match = cfi.qualifierMatches(key.column_qualifier);
				regexpCache.setColumn( key.column_family_code, key.column_qualifier, match );
			}
			if( !match ) {
				return 0;
			}
		}
		else if( !cfi.qualifierMatches(key.column_qualifier) ) {
			return 0;
		}

		return scanContext->columnFamilies[key.column_family_code];
	}

	bool Scanner::Reader::getCell( const Hypertable::Key& key, const Hypertable::Schema::ColumnFamily& cf, Hypertable::Cell& cell ) {
		if( !scanContext->valueRegexp ) {
			if( !checkCellLimits(key) ) {
				return false;
			}
		}

		cell.row_key = key.row;
		cell.column_family = cf.name.c_str();
		cell.column_qualifier = key.column_qualifier;
		cell.timestamp = key.timestamp;
		cell.revision = key.revision;
		cell.flag = key.flag;
		cell.value = 0;
		cell.value_len = 0;

		if( !scanContext->keysOnly || scanContext->valueRegexp ) {
			const void* v = sqlite3_column_blob( stmtQuery, 4 );
			uint32_t len = sqlite3_column_bytes( stmtQuery, 4 );

			// filter by value regexp
			if( scanContext->valueRegexp ) {
				if( !RE2::PartialMatch(re2::StringPiece(reinterpret_cast<const char*>(v), len), *(scanContext->valueRegexp)) ) {
					return false;
				}

				if( !checkCellLimits(key) ) {
					return false;
				}
			}

			if( !scanContext->keysOnly ) {
				cell.value = reinterpret_cast<const uint8_t*>( v );
				cell.value_len = len;
			}
		}

		return true;
	}

	bool Scanner::Reader::checkCellLimits( const Hypertable::Key& key ) {
		++cellCount;

		if( prevColumnFamilyCode != key.column_family_code ) {
			prevColumnFamilyCode = key.column_family_code;
			cellPerFamilyCount = 1;
		}
		else {
			++cellPerFamilyCount;
		}

		// cell offset
		if( scanContext->cellOffset && cellCount <= scanContext->cellOffset ) {
			return false;
		}

		// cell limit
		if( scanContext->cellLimit && cellCount > scanContext->cellLimit ) {
			limitReached( );
			return false;
		}

		// cells per family limit
		if( scanContext->cellLimitPerFamily && cellPerFamilyCount > scanContext->cellLimitPerFamily ) {
			return false;
		}

		return true;
	}

	Scanner::ReaderScanAndFilter::ReaderScanAndFilter( sqlite3* db, int64_t tableId, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec )
	: Reader( db, tableId, schema, scanSpec )
	{
	}

	void Scanner::ReaderScanAndFilter::stmtPrepare( ) {
		Util::stmt_finalize( db, &stmtQuery );
		std::string predicate;
		if( !scanContext->predicate.empty() ) {
			predicate = " AND (" + scanContext->predicate + ")";
		}

		std::string select;
		if( strcmp(*scanContext->rowset.begin(),*scanContext->rowset.rbegin()) ) {
			select = Hypertable::format( "SELECT %s FROM t%lld WHERE (r>=\"%s\" AND r <=\"%s\")%s ORDER BY r, cf, cq, ts;"
																	, scanContext->columns.c_str()
																	, tableId
																	, *scanContext->rowset.begin()
																	, *scanContext->rowset.rbegin()
																	, predicate.c_str()
																	);
		}
		else {
			select = Hypertable::format( "SELECT %s FROM t%lld WHERE r=\"%s\"%s ORDER BY r, cf, cq, ts;"
																	, scanContext->columns.c_str()
																	, tableId
																	, *scanContext->rowset.begin()
																	, predicate.c_str()
																	);
		}
		int st = sqlite3_prepare_v2( db, select.c_str(), -1, &stmtQuery, 0 );
		HT4C_SQLITE_VERIFY( st, db, 0 )
	}

	Scanner::ReaderRowIntervals::ReaderRowIntervals( sqlite3* db, int64_t tableId, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& _scanSpec )
	: Reader( db, tableId, schema, _scanSpec )
	, scanSpec( _scanSpec )
	, it( _scanSpec.row_intervals.begin() )
	, rowIntervalDone( true )
	{
	}

	void Scanner::ReaderRowIntervals::stmtPrepare( ) {
		// do nothing
	}

	bool Scanner::ReaderRowIntervals::moveNext( ) {
		int st;

		if( rowIntervalDone ) {
			rowIntervalDone = false;
			if( it == scanSpec.row_intervals.end() ) {
				return false;
			}

			rowCount = 0;
			cellCount = 0;
			cellPerFamilyCount = 0;

			Util::stmt_finalize( db, &stmtQuery );
			std::string predicate;
			if( !scanContext->predicate.empty() ) {
				predicate = " AND (" + scanContext->predicate + ")";
			}

			std::string select;
			if( strcmp(it->start, it->end) ) {
				select = Hypertable::format( "SELECT %s FROM t%lld WHERE (r>%s\"%s\" AND r <%s\"%s\")%s ORDER BY r, cf, cq, ts;"
																	 , scanContext->columns.c_str()
																	 , tableId
																	 , it->start_inclusive ? "=" : ""
																	 , it->start
																	 , it->end_inclusive ? "=" : ""
																	 , it->end
																	 , predicate.c_str()
																	 );
			}
			else {
				select = Hypertable::format( "SELECT %s FROM t%lld WHERE r=\"%s\"%s ORDER BY r, cf, cq, ts;"
																	 , scanContext->columns.c_str()
																	 , tableId
																	 , (*it).start
																	 , predicate.c_str()
																	 );
			}
			st = sqlite3_prepare_v2( db, select.c_str(), -1, &stmtQuery, 0 );
			HT4C_SQLITE_VERIFY( st, db, 0 )
		}

		st = sqlite3_step( stmtQuery );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		if( st == SQLITE_DONE ) {
			it++;
			rowIntervalDone = true;
			return moveNext();
		}

		return st == SQLITE_ROW;
	}

	Scanner::ReaderCellIntervals::ReaderCellIntervals( sqlite3* db, int64_t tableId, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& _scanSpec )
	: Reader( db, tableId, schema, _scanSpec )
	, scanSpec( _scanSpec )
	, it( _scanSpec.cell_intervals.begin() )
	, startColumnQualifier( 0 )
	, endColumnQualifier( 0 )
	, cmpStart( 0 )
	, cmpEnd( 0 )
	, cmpStartRow( 0 )
	, cmpEndRow( 0 )
	, cellIntervalDone( true )
	{
	}

	void Scanner::ReaderCellIntervals::stmtPrepare( ) {
		// do nothing
	}

	bool Scanner::ReaderCellIntervals::moveNext( ) {
		int st;

		if( cellIntervalDone ) {
			cellIntervalDone = false;
			if( it == scanSpec.cell_intervals.end() ) {
				return false;
			}

			rowCount = 0;
			cellCount = 0;
			cellPerFamilyCount = 0;

			std::string family;
			bool hasQualifier, isRegexp;

			cmpStart = it->start_inclusive ? 0 : 1;
			cmpEnd = it->end_inclusive ? 0 : -1;
			Hypertable::ScanSpec::parse_column( it->start_column, family, startColumnQualifierBuf, &hasQualifier, &isRegexp );
			const Hypertable::Schema::ColumnFamily* cf = scanContext->schema->get_column_family( family.c_str() );
			if( !cf ) {
				HT4C_SQLITE_THROW( Hypertable::Error::BAD_SCAN_SPEC, Hypertable::format("Column family '%s' does not exists", family.c_str()).c_str() );
			}
			startColumnFamilyCode = cf->id;
			startColumnQualifier = hasQualifier && !isRegexp ? startColumnQualifierBuf.c_str() : 0;

			Hypertable::ScanSpec::parse_column( it->end_column, family, endColumnQualifierBuf, &hasQualifier, &isRegexp );
			cf = scanContext->schema->get_column_family( family.c_str() );
			if( !cf ) {
				HT4C_SQLITE_THROW(Hypertable::Error::BAD_SCAN_SPEC, Hypertable::format("Column family '%s' does not exists", family.c_str()).c_str() );
			}
			endColumnFamilyCode = cf->id;
			endColumnQualifier = hasQualifier && !isRegexp ? endColumnQualifierBuf.c_str() : 0;

			Util::stmt_finalize( db, &stmtQuery );
			std::string predicate;
			if( !scanContext->predicate.empty() ) {
				predicate = " AND (" + scanContext->predicate + ")";
			}

			std::string select;
			if( strcmp(it->start_row, it->end_row) ) {
				select = Hypertable::format( "SELECT %s FROM t%lld WHERE (r>=\"%s\" AND r <=\"%s\")%s ORDER BY r, cf, cq, ts;"
																	 , scanContext->columns.c_str()
																	 , tableId
																	 , it->start_row
																	 , it->end_row
																	 , predicate.c_str()
																	 );
			}
			else if( startColumnFamilyCode != endColumnFamilyCode ) {
				select = Hypertable::format( "SELECT %s FROM t%lld WHERE r=\"%s\" AND cf>=%d AND cf<=%d%s ORDER BY r, cf, cq, ts;"
																	 , scanContext->columns.c_str()
																	 , tableId
																	 , (*it).start_row
																	 , startColumnFamilyCode
																	 , endColumnFamilyCode
																	 , predicate.c_str()
																	 );
			}
			else {
				select = Hypertable::format( "SELECT %s FROM t%lld WHERE r=\"%s\" AND cf=%d%s ORDER BY r, cf, cq, ts;"
																	 , scanContext->columns.c_str()
																	 , tableId
																	 , (*it).start_row
																	 , startColumnFamilyCode
																	 , predicate.c_str()
																	 );
			}
			st = sqlite3_prepare_v2( db, select.c_str(), -1, &stmtQuery, 0 );
			HT4C_SQLITE_VERIFY( st, db, 0 )
		}

		st = sqlite3_step( stmtQuery );
		HT4C_SQLITE_VERIFY( st, db, 0 );

		if( st == SQLITE_DONE ) {
			it++;
			cellIntervalDone = true;
			return moveNext();
		}

		return st == SQLITE_ROW;
	}

	bool Scanner::ReaderCellIntervals::filterRow( const char* row ) {
		if( (cmpStartRow = strcmp(row, it->start_row)) >= 0 /* not cmpStart */ ) {
			if( (cmpEndRow = strcmp(row, it->end_row)) <= 0 /* not cmpEnd */ ) {
				return Reader::filterRow( row );
			}

			it++;
			cellIntervalDone = true;
		}

		return false;
	}

	const Hypertable::Schema::ColumnFamily* Scanner::ReaderCellIntervals::filterCell( const Hypertable::Key& key ) {
		int cmpStartColumnFamilyCode;
		int cmpEndColumnFamilyCode;
		if(    (cmpStartRow > 0
				|| (   (cmpStartColumnFamilyCode = key.column_family_code - startColumnFamilyCode) >= (startColumnQualifier ? 0 : cmpStart)
						&& (cmpStartColumnFamilyCode > 0 || !startColumnQualifier || strcmp(key.column_qualifier, startColumnQualifier) >= cmpStart)))
			&&   (cmpEndRow < 0
				|| (   (cmpEndColumnFamilyCode = key.column_family_code - endColumnFamilyCode) <= (endColumnQualifier ? 0 : cmpEnd)
						&& (cmpEndColumnFamilyCode < 0 || !endColumnQualifier || strcmp(key.column_qualifier, endColumnQualifier) <= cmpEnd))) ) {

			return Reader::filterCell( key );
		}

		return 0;
	}

	ScannerAsync::ScannerAsync( Db::TablePtr _table )
	: table( _table )
	, db( _table->getEnv()->getDb() ) {
	}

	ScannerAsync::~ScannerAsync( ) {
		db = 0;
		table = 0;
	}

} } }