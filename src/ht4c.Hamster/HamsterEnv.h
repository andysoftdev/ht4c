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

#pragma once

#ifdef __cplusplus_cli
#error compile native
#endif

namespace hamsterdb {
	class env;
	class db;
}

namespace ht4c { namespace Common {

	class Client;

} }

namespace ht4c { namespace Hamster {

	namespace Db {

		class Table;

	}

	struct HamsterEnvConfig;

	/// <summary>
	/// Represents the Hypertable hamster environment.
	/// </summary>
	class HamsterEnv : public Hypertable::ReferenceCount {

	public:

			enum {
				KEYSIZE_SYSDB = 256
			,	KEYSIZE_DB		=  64
			};

			HamsterEnv( const std::string& filename, const HamsterEnvConfig& config );
			virtual ~HamsterEnv( );

			Common::Client* createClient( );
			inline hamsterdb::env* getEnv( ) const {
				return env;
			}
			inline hamsterdb::db* getSysDb( ) const {
				return sysdb;
			}
			uint16_t createTable( );
			hamsterdb::db* openTable( uint16_t id, Db::Table* table, CRITICAL_SECTION& cs );
			void disposeTable( uint16_t id, Db::Table* table );
			void eraseTable( uint16_t id );

			class Lock {

				public:

					inline Lock( HamsterEnv* _env )
					: env( _env ) {
						env->lock();
					}
					inline ~Lock( ) {
						env->unlock();
					}

				private:

					HamsterEnv* env;
			};
			friend class Lock;

		private:

			enum {
				SYS_DB = 1
			, FIRST_TABLE_DB = 2
			};

			struct db_t {
				hamsterdb::db* db;
				std::set<Db::Table*> ref;
				CRITICAL_SECTION cs;

				db_t()
					: db(0)
				{
				}

				void dispose( );
			};

			inline void lock( ) {
				::EnterCriticalSection( &cs );
			}
			inline void unlock( ) {
				::LeaveCriticalSection( &cs );
			}

			hamsterdb::env* env;
			hamsterdb::db* sysdb;
			typedef std::map<uint16_t, db_t> tables_t;
			tables_t tables;

			CRITICAL_SECTION cs;
	};
	typedef boost::intrusive_ptr<HamsterEnv> HamsterEnvPtr;

	typedef HamsterEnv::Lock HamsterEnvLock;

} }
