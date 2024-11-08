/** -*- C++ -*-
 * Copyright (C) 2010-2016 Thalmann Software & Consulting, http://www.softdev.ch
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

#include "Common/Compat.h"

#pragma warning( push, 3 )

#include "Common/ReferenceCount.h"
#include "Common/Properties.h"
#include "AsyncComm/Config.h"

#ifdef SUPPORT_HYPERTABLE

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/ConnectionManager.h"
#include "Hyperspace/Session.h"

#endif

#include "Hypertable/Lib/Client.h"

#ifdef SUPPORT_HYPERTABLE_THRIFT

#include "ThriftBroker/Client.h"

#endif

#pragma warning( pop )

#ifdef SUPPORT_HAMSTERDB

#include "ht4c.Hamster/HamsterFactory.h"

#endif

#ifdef SUPPORT_SQLITEDB

#include "ht4c.SQLite/SQLiteFactory.h"

#endif

#ifdef SUPPORT_ODBC

#include "ht4c.Odbc/OdbcFactory.h"

#endif
