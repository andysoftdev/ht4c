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

#include "Common/Filesystem.h"
#include "Hypertable/Lib/ScanSpec.h"
#include "Hypertable/Lib/KeySpec.h"
#include "Hypertable/Lib/Cell.h"
#include "Hypertable/Lib/Cells.h"
#include "Hypertable/Lib/CellPredicate.h"
#include "Hypertable/Lib/Schema.h"

#pragma warning( pop )

#include "Utils.h"
