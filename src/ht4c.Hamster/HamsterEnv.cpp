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
#include "HamsterEnv.h"
#include "HamsterFactory.h"
#include "HamsterClient.h"
#include "HamsterException.h"

namespace ht4c { namespace Hamster {

	namespace {

		static const ham_parameter_t sys_pars[] = {
			  { HAM_PARAM_KEYSIZE , HamsterEnv::KEYSIZE_SYSDB }
			, { 0, 0 }
		};

		static const ham_parameter_t table_pars[] = {
			  { HAM_PARAM_KEYSIZE , HamsterEnv::KEYSIZE_DB }
			, { 0, 0 }
		};

		static const uint32_t dbCreateFlags		= 0;
		static const uint32_t dbOpenFlags			= 0;

		static int HAM_CALLCONV KeyCompare( ham_db_t *db
																			, const ham_u8_t *lhs
																			, ham_size_t lhs_size
																			, const ham_u8_t *rhs
																			, ham_size_t rhs_size ) {

				Hypertable::SerializedKey klhs( lhs );
				Hypertable::SerializedKey krhs( rhs );
				return klhs.compare( krhs );
		}

		static int HAM_CALLCONV KeyPrefixCompare( ham_db_t *db
																						, const ham_u8_t *lhs
																						, ham_size_t lhs_length
																						, ham_size_t lhs_real_length
																						, const ham_u8_t *rhs
																						, ham_size_t rhs_length
																						, ham_size_t rhs_real_length ) {

				if( lhs_length < 4 || rhs_length < 4 ) {
					return HAM_PREFIX_REQUEST_FULLKEY;
				}

				Hypertable::SerializedKey klhs( lhs );
				Hypertable::SerializedKey krhs( rhs );

				lhs_length -= 4;
				rhs_length -= 4;

				const uint8_t *ptr1, *ptr2;
				int len1 = klhs.decode_length(&ptr1);
				int len2 = krhs.decode_length(&ptr2);

				if (*ptr1 != *ptr2) {
					// see Key.h
					if (*ptr1 >= 0x80 && *ptr1 != 0xD0)
						len1 -= 8;
					if (*ptr2 >= 0x80 && *ptr2 != 0xD0)
						len2 -= 8;
				}

				bool partial = false;
				if( len1 > (int)lhs_length ) {
					len1 = lhs_length;
					partial = true;
				}
				if( len2 > (int)rhs_length ) {
					len2 = rhs_length;
					partial = true;
				}

				int len = (len1 < len2) ? len1 : len2;
				int cmp = memcmp(ptr1+1, ptr2+1, len-1);
				return !cmp ? (partial ? HAM_PREFIX_REQUEST_FULLKEY : len1 - len2) : cmp;
		}

		static void HAM_CALLCONV errhandler(int level, const char *message) {
		}

		inline ham::db* From( ham::db& db ) {
			ham::db* p = new ham::db();
			*p = db;
			return p;
		}

	}

	HamsterEnv::HamsterEnv( const std::string &filename, const HamsterEnvConfig& config )
	: env( new ham::env() )
	, sysdb( 0 )
	{
		const uint32_t envFlags =		(config.enableRecovery ? HAM_ENABLE_RECOVERY : 0)
															| (config.enableAutoRecovery ? HAM_ENABLE_RECOVERY|HAM_AUTO_RECOVERY : 0);

		::InitializeCriticalSection( &cs );
		try {
			ham_set_errhandler( errhandler );

			const ham_parameter_t env_pars[] = {
					{ HAM_PARAM_CACHESIZE, std::max(1, config.cacheSizeMB) * 1024 * 1024 }
				, { 0, 0 }
			};

			env->open( filename.c_str(), envFlags );
		}
		catch( ham::error& e ) {
			if( e.get_errno() != HAM_FILE_NOT_FOUND ) {
				throw;
			}

			const ham_parameter_t env_pars[] = {
					{ HAM_PARAM_MAX_ENV_DATABASES, std::min(config.maxTables, HAM_DEFAULT_DATABASE_NAME - 1) }
				, { HAM_PARAM_CACHESIZE, std::max(1, config.cacheSizeMB) * 1024 * 1024 }
				, { HAM_PARAM_PAGESIZE, (std::min(64, config.pageSizeKB) / 64) * 64 * 1024 }
				, { 0, 0 }
			};

			env->create( filename.c_str(), envFlags, 0644, env_pars );
		}

		try {
			sysdb = From( env->open_db(SYS_DB, dbOpenFlags) );
		}
		catch( ham::error& e ) {
			if( e.get_errno() != HAM_DATABASE_NOT_FOUND ) {
				throw;
			}
			sysdb = From( env->create_db(SYS_DB, dbCreateFlags, sys_pars) );
		}
	}

	HamsterEnv::~HamsterEnv( ) {
		::DeleteCriticalSection( &cs );
		HT4C_TRY {
			for( std::map<uint16_t, std::set<Db::Table*>>::iterator it = tables.begin(); it != tables.end(); ++it ) {
				for each( Db::Table* table in (*it).second ) {
					table->dispose();
				}
				(*it).second.clear();
			}
			tables.clear();

			if( sysdb ) {
				delete sysdb;
				sysdb = 0;
			}
			delete env;
			env = 0;
		}
		HT4C_HAMSTER_RETHROW
	}

	Common::Client* HamsterEnv::createClient( ) {
		return HamsterClient::create( new Db::Client(this) );
	}

	ham::db* HamsterEnv::createTable( uint16_t& id ) {
		std::vector<ham_u16_t> names = env->get_database_names();
		std::set<uint16_t> ids( names.begin(), names.end() );
		id = FIRST_TABLE_DB;
		while( ids.find(id) != ids.end() ) {
			++id;
		}
		ham::db* db = From( env->create_db(id, dbCreateFlags, table_pars) );
		db->set_prefix_compare_func( KeyPrefixCompare );
		db->set_compare_func( KeyCompare );
		return db;
	}

	ham::db* HamsterEnv::openTable( uint16_t id, Db::Table* table ) {
		ham::db* db = From( env->open_db(id, dbOpenFlags) );
		db->set_prefix_compare_func( KeyPrefixCompare );
		db->set_compare_func( KeyCompare );
		tables[id].insert( table );
		return db;
	}

	void HamsterEnv::disposeTable( uint16_t id ) {
		tables.erase( id );
	}

	void HamsterEnv::eraseTable( uint16_t id ) {
		std::map<uint16_t, std::set<Db::Table*>>::iterator it = tables.find( id );
		if( it != tables.end() ) {
			std::set<Db::Table*> t( (*it).second ); // shallow copy, table->dispose() might call disposeTable( id )
			for each( Db::Table* table in t ) {
				table->dispose();
			}
			tables.erase( id );
		}

		env->erase_db( id );
	}

} }