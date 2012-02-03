/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief provides forward declarations of internally used types
 *
 * This header file provides these forward declarations to prevent several 
 * cyclic dependencies; in particular, the @ref ham_page_t is a type used 
 * throughout, but a @ref ham_page_t contains several other types, which 
 * again reference @ref ham_page_t pointers either directly or indirectly.
 *
 * To solve this self-referential issue once and for all, all major hamster 
 * internal types are forward declared here; when the code requires the actual 
 * implementation of a type it can include the related header file any time it 
 * wishes.
 *
 * @remark This way of solving the cyclic dependency conundrum has the added
 *     benefit of having a single, well known spot where the actual
 *     'typedef' lines reside, so there's zero risk of 'double defined types'.
 */

#ifndef HAM_FWD_DECL_CMN_TYPES_H__
#define HAM_FWD_DECL_CMN_TYPES_H__

#include <ham/hamsterdb.h>
#include <ham/hamsterdb_int.h>

#include "config.h"


class Cursor;

class Database;

class Environment;

struct ham_page_t;
typedef struct ham_page_t ham_page_t;

struct ham_backend_t;
typedef struct ham_backend_t ham_backend_t;

class Cache;

class Log;

class Journal;

struct extkey_t;
typedef struct extkey_t extkey_t;

class ExtKeyCache;

struct freelist_entry_t;
typedef struct freelist_entry_t freelist_entry_t;

struct freelist_cache_t;
typedef struct freelist_cache_t freelist_cache_t;

struct freelist_hints_t;
typedef struct freelist_hints_t freelist_hints_t;

struct runtime_statistics_pagedata_t;
typedef struct runtime_statistics_pagedata_t runtime_statistics_pagedata_t;

struct freelist_global_hints_t;
typedef struct freelist_global_hints_t freelist_global_hints_t;

struct find_hints_t;
typedef struct find_hints_t find_hints_t;

struct insert_hints_t;
typedef struct insert_hints_t insert_hints_t;

struct erase_hints_t;
typedef struct erase_hints_t erase_hints_t;


#include "packstart.h"

struct freelist_payload_t;
typedef struct freelist_payload_t freelist_payload_t;

#include "packstop.h"


#include "packstart.h"

struct btree_key_t;
typedef struct btree_key_t btree_key_t;

#include "packstop.h"


#endif
