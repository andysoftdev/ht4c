/** -*- C++ -*-
 * Copyright (C) 2011 Andy Thalmann
 *
 * This file is part of ht4c.
 *
 * ht4c is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
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

namespace ham {
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
			inline ham::env* getEnv( ) const {
				return env;
			}
			inline ham::db* getSysDb( ) const {
				return sysdb;
			}
			ham::db* createTable( uint16_t& id );
			ham::db* openTable( uint16_t id, Db::Table* table );
			void disposeTable( uint16_t id );
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

			inline void lock( ) {
				::EnterCriticalSection( &cs );
			}
			inline void unlock( ) {
				::LeaveCriticalSection( &cs );
			}

			ham::env* env;
			ham::db* sysdb;
			std::map<uint16_t, std::set<Db::Table*>> tables;

			CRITICAL_SECTION cs;
	};
	typedef boost::intrusive_ptr<HamsterEnv> HamsterEnvPtr;

	typedef HamsterEnv::Lock HamsterEnvLock;

} }
