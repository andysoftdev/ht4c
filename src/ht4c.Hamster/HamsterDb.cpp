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

#ifdef __cplusplus_cli
#error compile native
#endif

#include "stdafx.h"
#include "HamsterException.h"

namespace ht4c { namespace Hamster { namespace Db {

	namespace Util {

			inline bool KeyHasClassifier( const hamsterdb::key& key, char classifier ) {
				return key.get_data() && *reinterpret_cast<const char*>(key.get_data()) == classifier;
			}

			inline char* KeyToString( hamsterdb::key& key ) {
				char* k = reinterpret_cast<char*>( key.get_data() );
				k += KeyClassifiers::Length;
				return k;
			}

			inline const char* KeyToString( const hamsterdb::key& key ) {
				const char* k = reinterpret_cast<const char*>( key.get_data() );
				k += KeyClassifiers::Length;
				return k;
			}

			inline char* KeyToString( hamsterdb::key& key, char classifier ) {
				return KeyHasClassifier(key, classifier) ? KeyToString( key ) : 0;
			}

			inline const char* KeyToString( const hamsterdb::key& key, char classifier ) {
				return KeyHasClassifier(key, classifier) ? KeyToString( key ) : 0;
			}

			inline bool KeyStartWith( const hamsterdb::key& key, char classifier, const std::string& preffix ) {
				const char* name = KeyToString( key, classifier );
				return name && strstr(name, preffix.c_str()) == name;
			}

	}

	Client::Client( HamsterEnvPtr _env )
		: env( _env )
		, sysdb( _env->getSysDb() )
	{
	}

	Client::~Client( ) {
		sysdb = 0;
		env = 0;
	}

	void Client::createNamespace( const std::string& name, bool createIntermediate ) {
		if( name.empty() ) { // root "/" always exists
			HT4C_HAMSTER_THROW( Hypertable::Error::NAMESPACE_EXISTS, "Root namespace '/' always exists" );
		}

		char* ns_tmp = strdup( name.c_str() );
		try {
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
						HT4C_HAMSTER_THROW( Hypertable::Error::NAMESPACE_DOES_NOT_EXIST, Hypertable::format("Namespace '%s' does not exist", ns_tmp).c_str() );
					}
				}
				createNamespace( name );
			}
		}
		catch( hamsterdb::error& e ) {
			std::string name( ns_tmp );
			free( ns_tmp );

			if( e.get_errno() == HAM_DUPLICATE_KEY ) {
				HT4C_HAMSTER_THROW( Hypertable::Error::NAMESPACE_EXISTS, Hypertable::format("Namespace '%s' already exists", name.c_str()).c_str() );
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

		try {
			hamsterdb::key key;
			ns->toKey( key );
			sysdb->find( &key );
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}

			HT4C_HAMSTER_THROW( Hypertable::Error::NAMESPACE_DOES_NOT_EXIST, Hypertable::format("Namespace '%s' does not exist", name.c_str()).c_str() );
		}
		return ns;
	}

	bool Client::namespaceExists( const char* name ) {
		if( !name || !*name ) { // root "/" always exists
			return true;
		}

		try {
			Db::Namespace ns( this, name );
			hamsterdb::key key;
			ns.toKey( key );
			sysdb->find( &key );
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}
			return false;
		}
		return true;
	}

	void Client::dropNamespace( const std::string& name, bool ifExists ) {
		if( name.empty() ) { // root "/" always exists
			HT4C_HAMSTER_THROW( Hypertable::Error::HYPERSPACE_DIR_NOT_EMPTY, "Cannot drop root namepsace '/'" );
		}

		try {
			Db::Namespace ns( this, name );
			hamsterdb::key key;
			ns.toKey( key );

			// Is namespace empty?
			hamsterdb::cursor cursor;
			cursor.create( sysdb );
			try {
				cursor.find( &key, 0, HAM_FIND_GT_MATCH );
				if( Util::KeyStartWith(key, KeyClassifiers::NamespaceListing, name + "/") ) {
					HT4C_HAMSTER_THROW( Hypertable::Error::HYPERSPACE_DIR_NOT_EMPTY, Hypertable::format("Namespace '%s' is not empty", name.c_str()).c_str() );
				}
			}
			catch( hamsterdb::error& e ) {
				if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
					throw;
				}
			}

			// Drop
			ns.toKey( key );
			sysdb->erase( &key );
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}

			if( !ifExists ) {
				HT4C_HAMSTER_THROW( Hypertable::Error::NAMESPACE_DOES_NOT_EXIST, Hypertable::format("Namespace '%s' does not exist", name.c_str()).c_str() );
			}
		}
	}

	void Client::createNamespace( const std::string& name ) {
		Db::Namespace ns( this, name );
		hamsterdb::key key;
		hamsterdb::record record;
		ns.toKey( key );
		sysdb->insert( &key, &record );
	}

	Namespace::Namespace( ClientPtr _client, const std::string& name )
	: client( _client )
	, env( _client->getEnv() )
	, sysdb( env->getSysDb() )
	, keyName( ) {
		keyName.reserve( name.size() + 1 );
		keyName += KeyClassifiers::NamespaceListing;
		keyName += name;
	}

	Namespace::~Namespace( ) {
		sysdb = 0;
		env = 0;
		client = 0;
	}

	const char* Namespace::getName( ) const {
		const char* name = keyName.c_str();
		name += KeyClassifiers::Length;
		return name;
	}

	void Namespace::drop( const std::string& nsName, const std::vector<Db::NamespaceListing>& listing, bool ifExists, bool dropTables ) {
		for( std::vector<Db::NamespaceListing>::const_iterator it = listing.begin(); it != listing.end(); ++it ) {
			if( (*it).isNamespace ) {
				std::string nsSubName = nsName + "/" + (*it).name;
				Db::NamespacePtr nsSub = client->openNamespace( nsSubName );
				nsSub->drop( nsSubName, (*it).subEntries, ifExists, dropTables );
				client->dropNamespace( nsSubName, ifExists );
			}
			else if( dropTables ) {
				dropTable( (*it).name, true );
			}
		}
	}

	void Namespace::getNamespaceListing( bool deep, std::vector<Db::NamespaceListing>& listing ) {
		listing.clear();

		hamsterdb::key key;
		hamsterdb::record record;
		hamsterdb::cursor cursor;
		cursor.create( sysdb );

		try {
			toKey( key );
			cursor.find( &key, 0, HAM_FIND_GT_MATCH );
			std::string tmp( getName() );
			if( !tmp.empty() ) {
				tmp += "/";
			}
			int len = tmp.size();
			for( char* ptr = Util::KeyToString(key, KeyClassifiers::NamespaceListing); ptr && strstr(ptr, tmp.c_str()) == ptr; ptr = Util::KeyToString(key, KeyClassifiers::NamespaceListing) ) {
				ptr += len;
				if( !strchr(ptr, '/') ) {
					cursor.move( 0, &record );
					Db::NamespaceListing nsl;
					nsl.name = ptr;
					nsl.isNamespace = record.get_size() == 0;
					listing.push_back( nsl );

					if( deep ) {
						Db::Namespace ns( client, tmp + ptr );
						ns.getNamespaceListing( true, listing.back().subEntries );
					}
				}
				cursor.move_next( &key, &record );
			}
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}
		}
	}

	void Namespace::createTable( const std::string& name, const std::string& _schema ) {
		if( name.empty() ) {
			HT4C_HAMSTER_THROW( Hypertable::Error::HYPERSPACE_BAD_PATHNAME, "Empty table name" );
		}

		try {
			// Table already exists?
			Db::Table table( this, name );
			hamsterdb::key key;
			table.toKey( key );
			sysdb->find( &key );

			HT4C_HAMSTER_THROW( Hypertable::Error::NAME_ALREADY_IN_USE, Hypertable::format("Name '%s' already in use", name.c_str()).c_str() );
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}
		}

		try {
			// Namespace exists?
			hamsterdb::key key;
			toKey( key );
			sysdb->find( &key );
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}

			HT4C_HAMSTER_THROW( Hypertable::Error::NAMESPACE_DOES_NOT_EXIST, Hypertable::format("Namespace '%s' does not exist", getName()).c_str() );
		}

		// Validate schema
		Hypertable::SchemaPtr schema = Hypertable::Schema::new_instance(_schema, _schema.length());
		if( !schema->is_valid() ) {
			HT4C_HAMSTER_THROW( Hypertable::Error::MASTER_BAD_SCHEMA, Hypertable::format("Invalid table schema '%s'", _schema.c_str()).c_str() );
		}

		const Hypertable::Schema::ColumnFamilies& families = schema->get_column_families();
		for each( const Hypertable::Schema::ColumnFamily* cf in families ) {
			if( cf->counter ) {
				HT4C_HAMSTER_THROW( Hypertable::Error::MASTER_BAD_SCHEMA, Hypertable::format("Counters are currently not supported '%s'", _schema.c_str()).c_str() );
			}
		}

		if( schema->need_id_assignment() ) {
			schema->assign_ids();
		}

		std::string finalschema;
		schema->render( finalschema, true );

		uint16_t id = env->createTable( );
		Db::Table table( this, name, finalschema, id, 0 );
		hamsterdb::key key;
		table.toKey( key );
		Hypertable::DynamicBuffer buf;
		hamsterdb::record record;
		table.toRecord( buf, record );
		sysdb->insert( &key, &record );
	}

	Db::TablePtr Namespace::openTable( const std::string& name ) {
		Db::TablePtr table = new Db::Table( this, name );
		table->open( );
		return table;
	}

	void Namespace::alterTable( const std::string& name, const std::string& _schema ) {
		if( name.empty() ) {
			HT4C_HAMSTER_THROW( Hypertable::Error::HYPERSPACE_BAD_PATHNAME, "Empty table name" );
		}

		Db::Table table( this, name );
		try {
			// Table exists?
			hamsterdb::key key;
			table.toKey( key );
			table.fromRecord( sysdb->find(&key) );
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}

			HT4C_HAMSTER_THROW( Hypertable::Error::HYPERSPACE_FILE_NOT_FOUND, Hypertable::format("Table '%s' does not exist", name.c_str()).c_str() );
		}

		// Validate schema
		Hypertable::SchemaPtr schema = Hypertable::Schema::new_instance(_schema, _schema.length());
		if( !schema->is_valid() ) {
			HT4C_HAMSTER_THROW( Hypertable::Error::MASTER_BAD_SCHEMA, Hypertable::format("Invalid table schema '%s'", _schema.c_str()).c_str() );
		}

		if( schema->need_id_assignment() ) {
			schema->assign_ids();
		}

		std::string finalschema;
		schema->render( finalschema, true );

		Db::Table alterTable( this, name, finalschema, table.getId(), 0 );
		hamsterdb::key key;
		alterTable.toKey( key );
		Hypertable::DynamicBuffer buf;
		hamsterdb::record record;
		alterTable.toRecord( buf, record );
		sysdb->insert( &key, &record, HAM_OVERWRITE );
	}

	bool Namespace::tableExists( const std::string& name ) {
		if( name.empty() ) {
			return true;
		}

		try {
			Db::Table table( this, name );
			hamsterdb::key key;
			table.toKey( key );
			sysdb->find( &key );
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}
			return false;
		}
		return true;
	}

	void Namespace::getTableId( const std::string& name, std::string& id ) {
		Db::Table table( this, name );
		try {
			// Table exists?
			hamsterdb::key key;
			table.toKey( key );
			table.fromRecord( sysdb->find(&key) );
			id = Hypertable::format( "%d", table.getId() );
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}

			HT4C_HAMSTER_THROW( Hypertable::Error::HYPERSPACE_FILE_NOT_FOUND, Hypertable::format("Table '%s' does not exist", name.c_str()).c_str() );
		}
	}

	void Namespace::getTableSchema( const std::string& name, bool withIds, std::string& _schema ) {
		_schema.clear();

		try {
			Db::Table table( this, name );
			hamsterdb::key key;
			table.toKey( key );
			table.fromRecord( sysdb->find(&key) );

			if( withIds ) {
				_schema = table.getSchemaSpec();
			}
			else {
				Hypertable::SchemaPtr schema = table.getSchema();
				schema->render( _schema, false );
			}
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}

			HT4C_HAMSTER_THROW( Hypertable::Error::HYPERSPACE_FILE_NOT_FOUND, Hypertable::format("Table '%s' does not exist", name.c_str()).c_str() );
		}
	}

	
	void Namespace::renameTable( const std::string& name,const std::string& newName ) {
		if( name.empty() ) {
			HT4C_HAMSTER_THROW( Hypertable::Error::HYPERSPACE_BAD_PATHNAME, "Empty table name" );
		}
		if( newName.empty() ) {
			HT4C_HAMSTER_THROW( Hypertable::Error::HYPERSPACE_BAD_PATHNAME, "Empty new table name" );
		}

		try {
			// New table already exists?
			Db::Table table( this, newName );
			hamsterdb::key key;
			table.toKey( key );
			sysdb->find( &key );

			HT4C_HAMSTER_THROW( Hypertable::Error::NAME_ALREADY_IN_USE, Hypertable::format("Name '%s' already in use", newName.c_str()).c_str() );
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}
		}

		Db::Table table( this, name );
		hamsterdb::key key;
		table.toKey( key );
		hamsterdb::record record;
		try {
			// Table exists?
			record = sysdb->find (&key );
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}

			HT4C_HAMSTER_THROW( Hypertable::Error::HYPERSPACE_FILE_NOT_FOUND, Hypertable::format("Table '%s' does not exist", name.c_str()).c_str() );
		}

		sysdb->erase( &key );
		Db::Table tableNew( this, newName, table );
		tableNew.toKey( key );
		sysdb->insert( &key, &record );
	}

	void Namespace::dropTable( const std::string& name, bool ifExists ) {
		if( name.empty() ) {
			HT4C_HAMSTER_THROW( Hypertable::Error::HYPERSPACE_BAD_PATHNAME, "Empty table name" );
		}

		try {
			// Drop
			Db::Table table( this, name );
			hamsterdb::key key;
			table.toKey( key );
			hamsterdb::record record = sysdb->find( &key );
			table.fromRecord( record );
			env->eraseTable( table.getId() );
			sysdb->erase( &key );
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}

			if( !ifExists ) {
				HT4C_HAMSTER_THROW( Hypertable::Error::HYPERSPACE_FILE_NOT_FOUND, Hypertable::format("Table '%s' does not exist", name.c_str()).c_str() );
			}
		}
	}

	void Namespace::toKey( hamsterdb::key& key ) {
		key.set_size(keyName.size() + 1);
		key.set_data((void*)keyName.c_str()); 
	}

	Table::Table( NamespacePtr _ns, const std::string& _name )
	: ns( _ns )
	, name( _name )
	, id( 0 )
	, db( 0 ) {
		init();
	}

	Table::Table( NamespacePtr _ns, const std::string& _name, const std::string& _schema, uint16_t _id, hamsterdb::db* _db )
	: ns( _ns )
	, name( _name )
	, schemaSpec( _schema )
	, id( _id )
	, db( _db ) {
		init();
	}

	Table::Table( NamespacePtr _ns, const std::string& _name, const Table& other )
	: ns( _ns )
	, name( _name )
	, schemaSpec( other.schemaSpec )
	, schema( other.schema )
	, id( other.id )
	, db( other.db ) {
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
			HT4C_HAMSTER_THROW( Hypertable::Error::TABLE_NOT_FOUND, Hypertable::format("Invalid identifier for table '%s'", getFullName()).c_str() );
		}
		if( !db ) {
			HT4C_HAMSTER_THROW( Hypertable::Error::TABLE_NOT_FOUND, Hypertable::format("Table '%s' already disposed", getFullName()).c_str() );
		}
		return new Db::Mutator( this, flags, flushInterval );
	}

	Db::ScannerPtr Table::createScanner( const Hypertable::ScanSpec& scanSpec, uint32_t flags) {
		if( !id ) {
			HT4C_HAMSTER_THROW( Hypertable::Error::TABLE_NOT_FOUND, Hypertable::format("Invalid identifier for table '%s'", getFullName()).c_str() );
		}
		if( !db ) {
			HT4C_HAMSTER_THROW( Hypertable::Error::TABLE_NOT_FOUND, Hypertable::format("Table '%s' already disposed", getFullName()).c_str() );
		}
		return new Db::Scanner( this, scanSpec, flags );
	}

	Hypertable::SchemaPtr Table::getSchema() {
		if( !schema ) {
			schema = Hypertable::Schema::new_instance( schemaSpec, schemaSpec.length() );
		}
		return schema;
	}

	void Table::toKey( hamsterdb::key& key ) {
		key.set_size( keyName.size() + 1 );
		key.set_data( (void*)keyName.c_str() ); 
	}

	void Table::toRecord( Hypertable::DynamicBuffer& buf, hamsterdb::record& record ) {
		if( schemaSpec.empty() ) {
			HT4C_HAMSTER_THROW( Hypertable::Error::MASTER_BAD_SCHEMA, Hypertable::format("Undefined schema for table '%s'", getFullName()).c_str() );
		}

		size_t schema_len = schemaSpec.size() + 1;
		buf.clear();
		buf.reserve( sizeof(uint16_t) + schema_len );
		buf.add( &id, sizeof(uint16_t) );
		buf.add( schemaSpec.c_str(), schema_len );
		record.set_size( buf.fill() );
		record.set_data( buf.base ); 
	}

	void Table::fromRecord( hamsterdb::record& record ) {
		if( record.get_size() ) {
			id = *((const uint16_t*)record.get_data());
			schemaSpec = ((const char*)record.get_data() + sizeof(uint16_t));
		}
		else {
			schemaSpec.empty();
			dispose();
		}
	}

	void Table::open( ) {
		dispose();

		hamsterdb::key key;
		toKey( key );
		fromRecord( getEnv()->getSysDb()->find(&key) );
		db = getEnv()->openTable( id, this, cs );
	}

	void Table::dispose( ) {
		if( db ) {
			getEnv()->disposeTable( id, this );
			db = 0;
		}
		id = 0;
	}

	void Table::init( ) {
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
	, buf( HamsterEnv::KEYSIZE_DB )
	, db( _table->getDb() )
	, schema( _table->getSchema().get() )
	{
		memset( timeOrderAsc, true, sizeof(timeOrderAsc) );
		const Hypertable::Schema::ColumnFamilies& families = schema->get_column_families();
		for each( const Hypertable::Schema::ColumnFamily* cf in families ) {
			timeOrderAsc[cf->id] = !cf->time_order_desc;
		}
	}

	Mutator::~Mutator( ) {
		schema = 0;
		db = 0;
		table = 0;
	}

	void Mutator::set( Hypertable::KeySpec& keySpec, const void* value, uint32_t valueLength ) {
		keySpec.sanity_check();

		Hypertable::Key key;
		toKey( schema, keySpec, key );
		set( key, value, valueLength );
	}

	void Mutator::set( const Hypertable::Cells& cells ) {
		for each( const Hypertable::Cell& cell in cells ) {
			cell.sanity_check();

			Hypertable::Key key;
			toKey( schema
					 , cell
					 , key );

			set( key, cell.value, cell.value_len );
		}
	}

	void Mutator::del( Hypertable::KeySpec& keySpec ) {
		keySpec.sanity_check();

		Hypertable::Key key;
		toKey( schema, keySpec, key );

		if( key.flag == Hypertable::FLAG_INSERT ) {
			HT4C_HAMSTER_THROW( Hypertable::Error::BAD_KEY, Hypertable::format("Invalid delete flag '%d'", key.flag).c_str() );
		}

		del( key );
	}

	void Mutator::flush( ) {
		if( table ) {
			HamsterEnv* env = table->getEnv();
				if( env ) {
					env->flush();
				}
		}
	}

	void Mutator::insert( Hypertable::Key& key, const void* value, uint32_t valueLength ) {
		hamsterdb::key k;
		toKey( key, k );

		hamsterdb::record r;
		r.set_size( valueLength );
		r.set_data( const_cast<void*>(value) );

		db->insert( &k, &r, HAM_OVERWRITE );
	}

	void Mutator::toKey( const Hypertable::Key& key, hamsterdb::key& k ) {
		buf.clear();
		Hypertable::create_key_and_append( buf
			, Hypertable::FLAG_INSERT // always flag INSERT stored, key.flag
			, key.row
			, key.column_family_code
			, key.column_qualifier
			, key.timestamp
			, key.revision
			, timeOrderAsc[key.column_family_code] );

		k.set_size( buf.fill() );
		k.set_data( reinterpret_cast<void*>(buf.base) );
	}

	void Mutator::toKey( Hypertable::Schema* schema
										 , const char* row
										 , const char* columnFamily
										 , const char* columnQualifier
										 , int64_t timestamp
										 , int64_t revision
										 , uint8_t flag
										 , Hypertable::Key &fullKey )
	{
		if( flag > Hypertable::FLAG_DELETE_ROW ) {
			if( !columnFamily ) {
				HT4C_HAMSTER_THROW( Hypertable::Error::BAD_KEY, "Column family not specified" );
			}

			Hypertable::Schema::ColumnFamily* cf = schema->get_column_family( columnFamily );
			if( !cf ) {
				HT4C_HAMSTER_THROW( Hypertable::Error::BAD_KEY, Hypertable::format("Bad column family '%s'", columnFamily).c_str() );
			}
			fullKey.column_family_code = (uint8_t)cf->id;
		}
		else {
			fullKey.column_family_code = 0;
		}

		if( timestamp == Hypertable::AUTO_ASSIGN ) {
			timestamp = Hypertable::get_ts64();
		}

		fullKey.row = row;
		fullKey.column_qualifier = columnQualifier;
		fullKey.timestamp = timestamp;
		fullKey.revision = revision;
		fullKey.flag = flag;
	}

	void Mutator::set( Hypertable::Key& key, const void* value, uint32_t valueLength ) {
		if( key.flag == Hypertable::FLAG_INSERT ) {
			insert( key, value, valueLength );
		}
		else {
			del( key );
		}
	}

	void Mutator::del( Hypertable::Key& key ) {
		hamsterdb::key k;
		toKey( key, k );

		try {
			if( key.flag >= Hypertable::FLAG_DELETE_CELL_VERSION ) {
				db->erase( &k );
			}
			else {
				hamsterdb::cursor cursor;
				cursor.create( db );
				cursor.find( &k, 0, HAM_FIND_GEQ_MATCH );
				while( true ) {
					Hypertable::SerializedKey sk( reinterpret_cast<const uint8_t*>(k.get_data()) );
					if( strcmp(sk.row(), key.row) != 0 ) {
						break;
					}

					if( key.flag > Hypertable::FLAG_DELETE_ROW || key.timestamp > Hypertable::AUTO_ASSIGN ) {
						Hypertable::Key _key;
						if( !_key.load(sk) ) {
							HT4C_HAMSTER_THROW( Hypertable::Error::BAD_KEY, "Cannot load key" );
						}

						if(    key.flag > Hypertable::FLAG_DELETE_ROW
								&& key.column_family_code != 0
								&& _key.column_family_code != key.column_family_code ) {

							if( _key.column_family_code < key.column_family_code ) {
								cursor.move_next( &k );
								continue;
							}
							else {
								break;
							}
						}

						if(    key.flag > Hypertable::FLAG_DELETE_COLUMN_FAMILY
								&& key.column_qualifier
								&& strcmp(_key.column_qualifier, key.column_qualifier) ) {

							cursor.move_next( &k );
							continue;
						}

						if( _key.timestamp > key.timestamp ) {
							cursor.move_next( &k );
							continue;
						}
					}

					cursor.erase();
					cursor.find( &k, 0, HAM_FIND_GEQ_MATCH );
				}
			}
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}
		}
	}

	MutatorAsync::MutatorAsync( Db::TablePtr _table )
	: table( _table )
	, db( _table->getDb() )
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
	, db( _table->getDb() )
	, buf( 4 * 1024 )
	, reader( 0 )
	, scanSpec( _scanSpec )
	{
		cursor.create( db );

		if( scanSpec.get().row_intervals.empty() ) {
			if( scanSpec.get().cell_intervals.empty() ) {
				reader = new Reader( &cursor, _table->getSchema(), scanSpec.get() );
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
				reader = new ReaderCellIntervals( &cursor, _table->getSchema(), scanSpec.get() );
			}
		}
		else if (scanSpec.get().scan_and_filter_rows) {
			reader = new ReaderScanAndFilter( &cursor, _table->getSchema(), scanSpec.get() );
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
			reader = new ReaderRowIntervals( &cursor, _table->getSchema(), scanSpec.get() );
		}
	}

	Scanner::~Scanner( ) {
		if( reader ) {
			delete reader;
			reader = 0;
		}
		db = 0;
	}

	bool Scanner::nextCell( Hypertable::Cell& cell ) {
		try {
			Hypertable::Key key;
			if( reader->nextCell(key, cell) ) {
				buf.clear();
				buf.ensure( key.row_len + 1 + key.column_qualifier_len + 1 + cell.value_len + 1 );
				if( cell.row_key ) {
					cell.row_key = reinterpret_cast<const char*>( buf.add(key.row, key.row_len + 1) );
				}
				if( cell.column_qualifier ) {
					cell.column_qualifier = reinterpret_cast<const char*>( buf.add(key.column_qualifier, key.column_qualifier_len + 1) );
				}
				if( cell.value ) {
					if( cell.value_len ) {
						cell.value = reinterpret_cast<const uint8_t*>( buf.add(cell.value, cell.value_len) );
					}
					else {
						cell.value = reinterpret_cast<const uint8_t*>( "" );
					}
				}

				return true;
			}
		}
		catch( hamsterdb::error& e ) {
			if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
				throw;
			}
		}

		return false;
	}

	Scanner::Reader::Reader( hamsterdb::cursor* _cursor, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec )
	: cursor( _cursor )
	, scanContext( new ScanContext(scanSpec, schema) )
	, prevKey( HamsterEnv::KEYSIZE_DB )
	, prevColumnFamilyCode( -1 )
	, revsLimit( 0 )
	, revsCount( 0 )
	, rowCount( 0 )
	, cellCount( 0 )
	, cellPerFamilyCount( 0 )
	, eos( false )
	{
	}

	Scanner::Reader::~Reader() {
		delete scanContext;
		scanContext = 0;
		cursor = 0;
	}

	bool Scanner::Reader::nextCell( Hypertable::Key& key, Hypertable::Cell& cell ) {
		hamsterdb::key k;
		for( bool moved = moveNext(k); moved && !eos; moved = moveNext(k) ) {
			Hypertable::SerializedKey sk( reinterpret_cast<const uint8_t*>(k.get_data()) );
			if( filterRow(k, sk.row()) ) {
				if( !key.load(sk) ) {
					HT4C_HAMSTER_THROW( Hypertable::Error::BAD_KEY, "Cannot load key" );
				}

				const Hypertable::Schema::ColumnFamily* cf = filterCell( k, key );
				if( cf ) {
					if( getCell(key, *cf, cell) ) {
						return true;
					}
				}
			}
		}

		return false;
	}

	bool Scanner::Reader::moveNext( hamsterdb::key& k ) {
		cursor->move_next( &k );
		return true;
	}

	bool Scanner::Reader::filterRow( hamsterdb::key& k, const char* row ) {
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

	const Hypertable::Schema::ColumnFamily* Scanner::Reader::filterCell( hamsterdb::key& k, const Hypertable::Key& key ) {
		if( !scanContext->familyMask[key.column_family_code] ) {
			return 0;
		}

		CellFilterInfo& cfi = scanContext->familyInfo[key.column_family_code];

		// cutoff time
		if( key.timestamp < cfi.cutoffTime ) {
			cursor->erase( );

			return 0;
		}

		// timestamp
		if(  key.timestamp < scanContext->timeInterval.first
			|| key.timestamp >= scanContext->timeInterval.second ) {

			return 0;
		}

		// keep track of revisions
		const uint8_t* latestKey = reinterpret_cast<const uint8_t*>( key.row );
		size_t latestKeyLen = key.flag_ptr - reinterpret_cast<const uint8_t*>( key.row ) + 1;

		// row changes
		if( !prevKey.fill() || strcmp(reinterpret_cast<const char*>(prevKey.base), key.row) ) {
			prevKey.set( latestKey, latestKeyLen);
			revsCount = 0;
			revsLimit = cfi.maxVersions;

			cellPerFamilyCount = 0;
			++rowCount;
		}
		// cell changes
		else if( prevKey.fill() != latestKeyLen || memcmp(latestKey, prevKey.base, latestKeyLen) ) {
			prevKey.set( latestKey, latestKeyLen);
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

		// column qualifier match
		if( cfi.hasQualifierRegexpFilter() ) {
			bool cached, match;
			regexpCache.checkColumn( key.column_family_code, key.column_qualifier, &cached, &match );
			if( !cached ) {
				match = cfi.qualifierMatches(key.column_qualifier, key.column_qualifier_len);
				regexpCache.setColumn( key.column_family_code, key.column_qualifier, match );
			}
			if( !match ) {
				return 0;
			}
		}
		else if( !cfi.qualifierMatches(key.column_qualifier, key.column_qualifier_len) ) {
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

		CellFilterInfo& cfi = scanContext->familyInfo[key.column_family_code];

		cell.row_key = key.row;
		cell.column_family = cf.name.c_str();
		cell.column_qualifier = key.column_qualifier;
		cell.timestamp = key.timestamp;
		cell.revision = key.revision;
		cell.flag = key.flag;
		cell.value = 0;
		cell.value_len = 0;

		if( !scanContext->keysOnly || (scanContext->valueRegexp || cfi.hasColumnPredicateFilter()) ) {
			hamsterdb::record record;
			cursor->move( 0, &record );

			// filter by column predicate
			if( cfi.hasColumnPredicateFilter() ) {
				if( !cfi.columnPredicateMatches(record.get_data(), record.get_size()) ) {
					return false;
				}
			}

			// filter by value regexp
			if( scanContext->valueRegexp ) {
				if( !RE2::PartialMatch(re2::StringPiece(reinterpret_cast<const char*>(record.get_data()), record.get_size()), *(scanContext->valueRegexp)) ) {
					return false;
				}

				if( !checkCellLimits(key) ) {
					return false;
				}
			}

			if( !scanContext->keysOnly ) {
				cell.value = reinterpret_cast<const uint8_t*>( record.get_data() );
				cell.value_len = record.get_size();
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

	Scanner::ReaderScanAndFilter::ReaderScanAndFilter( hamsterdb::cursor* cursor, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& scanSpec )
	: Reader(cursor, schema, scanSpec)
	, nextRow( true )
	, buf( HamsterEnv::KEYSIZE_DB )
	{
	}

	bool Scanner::ReaderScanAndFilter::moveNext( hamsterdb::key& k ) {
		if( scanContext->rowset.empty() ) {
			return false;
		}

		if( nextRow ) {
			nextRow = false;

			buf.clear();
			Hypertable::create_key_and_append( buf
				, Hypertable::FLAG_INSERT
				, *scanContext->rowset.begin()
				, 0
				, 0
				, Hypertable::TIMESTAMP_MAX
				, Hypertable::AUTO_ASSIGN );

			k.set_size( buf.fill() );
			k.set_data( (void*)buf.base );
			cursor->find( &k, 0, HAM_FIND_GEQ_MATCH );
		}
		else {
			cursor->move_next( &k );
		}

		return true;
	}

	bool Scanner::ReaderScanAndFilter::filterRow( hamsterdb::key& k, const char* row ) {
		if( Reader::filterRow(k, row) ) {
			return true;
		}

		nextRow = true;
		return false;
	}

	Scanner::ReaderRowIntervals::ReaderRowIntervals( hamsterdb::cursor* cursor, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& _scanSpec )
	: Reader(cursor, schema, _scanSpec)
	, scanSpec( _scanSpec )
	, it( _scanSpec.row_intervals.begin() )
	, cmpStart( 0 )
	, cmpEnd( 0 )
	, rowIntervalDone( true )
	, buf( HamsterEnv::KEYSIZE_DB )
	{
	}

	bool Scanner::ReaderRowIntervals::moveNext( hamsterdb::key& k ) {
		if( rowIntervalDone ) {
			rowIntervalDone = false;
			if( it == scanSpec.row_intervals.end() ) {
				return false;
			}

			rowCount = 0;
			cellCount = 0;
			cellPerFamilyCount = 0;

			cmpStart = it->start_inclusive ? 0 : 1;
			cmpEnd = it->end_inclusive ? 0 : -1;
			buf.clear();
			Hypertable::create_key_and_append( buf
				, Hypertable::FLAG_INSERT
				, it->start
				, 0
				, 0
				, Hypertable::TIMESTAMP_MAX
				, Hypertable::AUTO_ASSIGN );

			k.set_size( buf.fill() );
			k.set_data( (void*)buf.base );
			cursor->find( &k, 0, it->start_inclusive ? HAM_FIND_GEQ_MATCH : HAM_FIND_GT_MATCH );
		}
		else {
			try {
				cursor->move_next( &k );
			}
			catch( hamsterdb::error& e ) {
				if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
					throw;
				}

				it++;
				rowIntervalDone = true;

				return moveNext( k );
			}
		}

		return true;
	}

	bool Scanner::ReaderRowIntervals::filterRow( hamsterdb::key& k, const char* row ) {
		if( strcmp(row, it->start) >= cmpStart ) {
			if( strcmp(row, it->end) <= cmpEnd ) {

				return Reader::filterRow( k, row );
			}

			it++;
			rowIntervalDone = true;
		}

		return false;
	}

	Scanner::ReaderCellIntervals::ReaderCellIntervals( hamsterdb::cursor* cursor, Hypertable::SchemaPtr schema, const Hypertable::ScanSpec& _scanSpec )
	: Reader(cursor, schema, _scanSpec)
	, scanSpec( _scanSpec )
	, it( _scanSpec.cell_intervals.begin() )
	, startColumnQualifier( 0 )
	, endColumnQualifier( 0 )
	, cmpStart( 0 )
	, cmpEnd( 0 )
	, cmpStartRow( 0 )
	, cmpEndRow( 0 )
	, cellIntervalDone( true )
	, buf( HamsterEnv::KEYSIZE_DB )
	{
		memset( timeOrderAsc, true, sizeof(timeOrderAsc) );
		const Hypertable::Schema::ColumnFamilies& families = schema->get_column_families();
		for each( const Hypertable::Schema::ColumnFamily* cf in families ) {
			timeOrderAsc[cf->id] = !cf->time_order_desc;
		}
	}

	bool Scanner::ReaderCellIntervals::moveNext( hamsterdb::key& k ) {
		if( cellIntervalDone ) {
			cellIntervalDone = false;
			if( it == scanSpec.cell_intervals.end() ) {
				return false;
			}

			rowCount = 0;
			cellCount = 0;
			cellPerFamilyCount = 0;

			std::string family;
			bool hasQualifier, isRegexp, isPrefix;

			cmpStart = it->start_inclusive ? 0 : 1;
			cmpEnd = it->end_inclusive ? 0 : -1;
			Hypertable::ScanSpec::parse_column( it->start_column, family, startColumnQualifierBuf, &hasQualifier, &isRegexp, &isPrefix );
			const Hypertable::Schema::ColumnFamily* cf = scanContext->schema->get_column_family( family.c_str() );
			if( !cf ) {
				HT4C_HAMSTER_THROW( Hypertable::Error::BAD_SCAN_SPEC, Hypertable::format("Column family '%s' does not exists", family.c_str()).c_str() );
			}
			startColumnFamilyCode = cf->id;
			startColumnQualifier = hasQualifier && !isRegexp ? startColumnQualifierBuf.c_str() : 0;

			Hypertable::ScanSpec::parse_column( it->end_column, family, endColumnQualifierBuf, &hasQualifier, &isRegexp, &isPrefix );
			cf = scanContext->schema->get_column_family( family.c_str() );
			if( !cf ) {
				HT4C_HAMSTER_THROW(Hypertable::Error::BAD_SCAN_SPEC, Hypertable::format("Column family '%s' does not exists", family.c_str()).c_str() );
			}
			endColumnFamilyCode = cf->id;
			endColumnQualifier = hasQualifier && !isRegexp ? endColumnQualifierBuf.c_str() : 0;

			buf.clear();
			Hypertable::create_key_and_append( buf
				, Hypertable::FLAG_INSERT
				, it->start_row
				, startColumnFamilyCode
				, startColumnQualifier
				, Hypertable::TIMESTAMP_MAX
				, Hypertable::AUTO_ASSIGN
				, timeOrderAsc[startColumnFamilyCode] );

			k.set_size( buf.fill() );
			k.set_data( (void*)buf.base );
			cursor->find( &k, 0, it->start_inclusive ? HAM_FIND_GEQ_MATCH : HAM_FIND_GT_MATCH );
		}
		else {
			try {
				cursor->move_next( &k );
			}
			catch( hamsterdb::error& e ) {
				if( e.get_errno() != HAM_KEY_NOT_FOUND ) {
					throw;
				}

				it++;
				cellIntervalDone = true;

				return moveNext( k );
			}
		}

		return true;
	}

	bool Scanner::ReaderCellIntervals::filterRow( hamsterdb::key& k, const char* row ) {
		if( (cmpStartRow = strcmp(row, it->start_row)) >= 0 /* not cmpStart */ ) {
			if( (cmpEndRow = strcmp(row, it->end_row)) <= 0 /* not cmpEnd */ ) {
				return Reader::filterRow( k, row );
			}

			it++;
			cellIntervalDone = true;
		}

		return false;
	}

	const Hypertable::Schema::ColumnFamily* Scanner::ReaderCellIntervals::filterCell( hamsterdb::key& k, const Hypertable::Key& key ) {
		int cmpStartColumnFamilyCode;
		int cmpEndColumnFamilyCode = 1;
		int cmpEndColumnQualifier = 1;
		if(    (cmpStartRow > 0
				|| (   (cmpStartColumnFamilyCode = key.column_family_code - startColumnFamilyCode) >= (startColumnQualifier ? 0 : cmpStart)
						&& (cmpStartColumnFamilyCode > 0 || !startColumnQualifier || strcmp(key.column_qualifier, startColumnQualifier) >= cmpStart)))
			&&   (cmpEndRow < 0
				|| (   (cmpEndColumnFamilyCode = key.column_family_code - endColumnFamilyCode) <= (endColumnQualifier ? 0 : cmpEnd)
						&& (cmpEndColumnFamilyCode < 0 || !endColumnQualifier || (cmpEndColumnQualifier = strcmp(key.column_qualifier, endColumnQualifier)) <= cmpEnd))) ) {

			if( !cmpEndColumnFamilyCode && !cmpEndColumnQualifier ) {
				it++;
				cellIntervalDone = true;
			}

			return Reader::filterCell( k, key );
		}

		return 0;
	}

	ScannerAsync::ScannerAsync( Db::TablePtr _table )
	: table( _table )
	, db( _table->getDb() ) {
	}

	ScannerAsync::~ScannerAsync( ) {
		db = 0;
		table = 0;
	}

} } }