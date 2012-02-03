/**
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>

#include "db.h"
#include "env.h"
#include "btree_stats.h"
#include "device.h"
#include "version.h"
#include "serial.h"
#include "txn.h"
#include "device.h"
#include "btree.h"
#include "mem.h"
#include "freelist.h"
#include "extkeys.h"
#include "backend.h"
#include "cache.h"
#include "log.h"
#include "journal.h"
#include "btree_key.h"
#include "os.h"
#include "blob.h"
#include "txn_cursor.h"
#include "cursor.h"
#include "btree_cursor.h"

typedef struct free_cb_context_t
{
    Database *db;
    ham_bool_t is_leaf;

} free_cb_context_t;

Environment::Environment()
  : _txn_id(0), _file_mode(0), _context(0), _device(0), _cache(0), _alloc(0),
    _hdrpage(0), _flushed_txn(0), _oldest_txn(0), _newest_txn(0), _log(0),
    _journal(0), _rt_flags(0), _next(0), _pagesize(0), _cachesize(0), 
    _max_databases(0), _is_active(0), _is_legacy(0), _file_filters(0)
{
#if HAM_ENABLE_REMOTE
    _curl=0;
#endif

	memset(&_perf_data, 0, sizeof(_perf_data));
	memset(&_changeset, 0, sizeof(_changeset));

    _fun_create=0;
    _fun_open=0;
    _fun_rename_db=0;
    _fun_erase_db=0;
    _fun_get_database_names=0;
    _fun_get_parameters=0;
    _fun_flush=0;
    _fun_create_db=0;
    _fun_open_db=0;
    _fun_txn_begin=0;
    _fun_txn_abort=0;
    _fun_txn_commit=0;
    _fun_close=0;
	destroy=0;
}

bool 
Environment::is_private()
{
    // must have exactly 1 database with the ENV_IS_PRIVATE flag
    if (!env_get_list(this))
        return (false);
    
    Database *db=env_get_list(this);
    if (db->get_next())
        return (false);
	return ((db->get_rt_flags()&DB_ENV_IS_PRIVATE) ? true : false);
}

/* 
 * forward decl - implemented in hamsterdb.cc
 */
extern ham_status_t 
__check_create_parameters(Environment *env, Database *db, const char *filename, 
        ham_u32_t *pflags, const ham_parameter_t *param, 
        ham_size_t *ppagesize, ham_u16_t *pkeysize, 
        ham_u64_t *pcachesize, ham_u16_t *pdbname,
        ham_u16_t *pmaxdbs, ham_u16_t *pdata_access_mode, ham_bool_t create);

/*
 * callback function for freeing blobs of an in-memory-database, implemented 
 * in db.c
 */
extern ham_status_t
__free_inmemory_blobs_cb(int event, void *param1, void *param2, void *context);

ham_u16_t
env_get_max_databases(Environment *env)
{
    env_header_t *hdr=(env_header_t*)
                    (page_get_payload(env_get_header_page(env)));
    return (ham_db2h16(hdr->_max_databases));
}

ham_u8_t
env_get_version(Environment *env, ham_size_t idx)
{
    env_header_t *hdr=(env_header_t*)
                    (page_get_payload(env_get_header_page(env)));
    return (envheader_get_version(hdr, idx));
}

ham_u32_t
env_get_serialno(Environment *env)
{
    env_header_t *hdr=(env_header_t*)
                    (page_get_payload(env_get_header_page(env)));
    return (ham_db2h32(hdr->_serialno));
}

void
env_set_serialno(Environment *env, ham_u32_t n)
{
    env_header_t *hdr=(env_header_t*)
                    (page_get_payload(env_get_header_page(env)));
    hdr->_serialno=ham_h2db32(n);
}

env_header_t *
env_get_header(Environment *env)
{
    return ((env_header_t*)(page_get_payload(env_get_header_page(env))));
}

ham_status_t
env_fetch_page(ham_page_t **page_ref, Environment *env, 
        ham_offset_t address, ham_u32_t flags)
{
    return (db_fetch_page_impl(page_ref, env, 0, address, flags));
}

ham_status_t
env_alloc_page(ham_page_t **page_ref, Environment *env,
                ham_u32_t type, ham_u32_t flags)
{
    return (db_alloc_page_impl(page_ref, env, 0, type, flags));
}

static ham_status_t 
_local_fun_create(Environment *env, const char *filename,
            ham_u32_t flags, ham_u32_t mode, const ham_parameter_t *param)
{
    ham_status_t st=0;
    ham_device_t *device=0;
    ham_size_t pagesize=env_get_pagesize(env);

    /* reset all performance data */
    btree_stats_init_globdata(env, env_get_global_perf_data(env));

    ham_assert(!env_get_header_page(env), (0));

    /* 
     * initialize the device if it does not yet exist
     */
    if (!env_get_device(env)) {
        device=ham_device_new(env_get_allocator(env), env, 
                        ((flags&HAM_IN_MEMORY_DB) 
                            ? HAM_DEVTYPE_MEMORY 
                            : HAM_DEVTYPE_FILE));
        if (!device)
            return (HAM_OUT_OF_MEMORY);

        env_set_device(env, device);

        device->set_flags(device, flags);
        st = device->set_pagesize(device, env_get_pagesize(env));
        if (st)
            return st;

        /* now make sure the pagesize is a multiple of 
         * DB_PAGESIZE_MIN_REQD_ALIGNMENT bytes */
        ham_assert(0 == (env_get_pagesize(env) 
                    % DB_PAGESIZE_MIN_REQD_ALIGNMENT), (0));
    }
    else {
        device=env_get_device(env);
        ham_assert(device->get_pagesize(device), (0));
        ham_assert(env_get_pagesize(env) == device->get_pagesize(device), (0));
    }
    ham_assert(device == env_get_device(env), (0));
    ham_assert(env_get_pagesize(env) == device->get_pagesize(device), (""));

    /* create the file */
    st=device->create(device, filename, flags, mode);
    if (st) {
        (void)ham_env_close((ham_env_t *)env, 0);
        return (st);
    }

    /* 
     * allocate the header page
     */
    {
        ham_page_t *page;

        page=page_new(env);
        if (!page) {
            (void)ham_env_close((ham_env_t *)env, 0);
            return (HAM_OUT_OF_MEMORY);
        }
        /* manually set the device pointer */
        page_set_device(page, device);
        st=page_alloc(page);
        if (st) {
            page_delete(page);
            (void)ham_env_close((ham_env_t *)env, 0);
            return (st);
        }
        memset(page_get_pers(page), 0, pagesize);
        page_set_type(page, PAGE_TYPE_HEADER);
        env_set_header_page(env, page);

        /* initialize the header */
        env_set_magic(env, 'H', 'A', 'M', '\0');
        env_set_version(env, HAM_VERSION_MAJ, HAM_VERSION_MIN, 
                HAM_VERSION_REV, 0);
        env_set_serialno(env, HAM_SERIALNO);
        env_set_persistent_pagesize(env, pagesize);
        env_set_max_databases(env, env_get_max_databases_cached(env));
        ham_assert(env_get_max_databases(env) > 0, (0));

        page_set_dirty(page);
    }

    /*
     * create a logfile and a journal (if requested)
     */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY) {
        Log *log=new Log(env);
        st=log->create();
        if (st) { 
            delete log;
            (void)ham_env_close((ham_env_t *)env, 0);
            return (st);
        }
        env_set_log(env, log);

        Journal *journal=new Journal(env);
        st=journal->create();
        if (st) { 
            (void)ham_env_close((ham_env_t *)env, 0);
            return (st);
        }
        env_set_journal(env, journal);
    }

    /* initialize the cache */
    env_set_cache(env, new Cache(env, env_get_cachesize(env)));

    /* flush the header page - this will write through disk if logging is
     * enabled */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY)
    	return (page_flush(env_get_header_page(env)));

    return (0);
}

static ham_status_t
__recover(Environment *env, ham_u32_t flags)
{
    ham_status_t st;
    Log *log=new Log(env);
    Journal *journal=new Journal(env);

    ham_assert(env_get_rt_flags(env)&HAM_ENABLE_RECOVERY, (""));

    /* open the log and the journal (if transactions are enabled) */
    st=log->open();
    env_set_log(env, log);
    if (st && st!=HAM_FILE_NOT_FOUND)
        goto bail;
    if (env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS) {
        st=journal->open();
        env_set_journal(env, journal);
        if (st && st!=HAM_FILE_NOT_FOUND)
            goto bail;
    }

    /* success - check if we need recovery */
    if (!log->is_empty()) {
        if (flags&HAM_AUTO_RECOVERY) {
            st=log->recover();
            if (st)
                goto bail;
        }
        else {
            st=HAM_NEED_RECOVERY;
            goto bail;
        }
    }

    if (env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS) {
        if (!journal->is_empty()) {
            if (flags&HAM_AUTO_RECOVERY) {
                st=journal->recover();
                if (st)
                    goto bail;
            }
            else {
                st=HAM_NEED_RECOVERY;
                goto bail;
            }
        }
    }

goto success;

bail:
    /* in case of errors: close log and journal, but do not delete the files */
    if (log) {
        log->close(true);
        delete log;
        env_set_log(env, 0);
    }
    if (journal) {
        journal->close(true);
        delete journal;
        env_set_journal(env, 0);
    }
    return (st);

success:
    /* done with recovering - if there's no log and/or no journal then
     * create them and store them in the environment */
    if (!log) {
        log=new Log(env);
        st=log->create();
        if (st)
            return (st);
    }
    env_set_log(env, log);

    if (env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS) {
        if (!journal) {
            journal=new Journal(env);
            st=journal->create();
            if (st)
                return (st);
        }
    	env_set_journal(env, journal);
    }
    else if (journal)
        delete journal;

    return (0);
}

static ham_status_t 
_local_fun_open(Environment *env, const char *filename, ham_u32_t flags, 
        const ham_parameter_t *param)
{
    ham_status_t st;
    ham_device_t *device=0;
    ham_u32_t pagesize=0;

    /* reset all performance data */
    btree_stats_init_globdata(env, env_get_global_perf_data(env));

    /* 
     * initialize the device if it does not yet exist
     */
    if (!env_get_device(env)) {
        device=ham_device_new(env_get_allocator(env), env,
                ((flags&HAM_IN_MEMORY_DB) 
                    ? HAM_DEVTYPE_MEMORY 
                    : HAM_DEVTYPE_FILE));
        if (!device)
            return (HAM_OUT_OF_MEMORY);
        env_set_device(env, device);
    }
    else {
        device=env_get_device(env);
    }

    /* 
     * open the file 
     */
    st=device->open(device, filename, flags);
    if (st) {
        (void)ham_env_close((ham_env_t *)env, HAM_DONT_CLEAR_LOG);
        return (st);
    }

    /*
     * read the database header
     *
     * !!!
     * now this is an ugly problem - the database header is one page, but
     * what's the size of this page? chances are good that it's the default
     * page-size, but we really can't be sure.
     *
     * read 512 byte and extract the "real" page size, then read 
     * the real page. (but i really don't like this)
     */
    {
        ham_page_t *page=0;
        env_header_t *hdr;
        ham_u8_t hdrbuf[512];
        ham_page_t fakepage = {{0}};
        ham_bool_t hdrpage_faked = HAM_FALSE;

        /*
         * in here, we're going to set up a faked headerpage for the 
         * duration of this call; BE VERY CAREFUL: we MUST clean up 
         * at the end of this section or we'll be in BIG trouble!
         */
        hdrpage_faked = HAM_TRUE;
        fakepage._pers = (ham_perm_page_union_t *)hdrbuf;
        env_set_header_page(env, &fakepage);

        /*
         * now fetch the header data we need to get an estimate of what 
         * the database is made of really.
         * 
         * Because we 'faked' a headerpage setup right here, we can now use 
         * the regular hamster macros to obtain data from the file 
         * header -- pre v1.1.0 code used specially modified copies of 
         * those macros here, but with the advent of dual-version database 
         * format support here this was getting hairier and hairier. 
         * So we now fake it all the way instead.
         */
        st=device->read(device, 0, hdrbuf, sizeof(hdrbuf));
        if (st) 
            goto fail_with_fake_cleansing;

        hdr = env_get_header(env);
        ham_assert(hdr == (env_header_t *)(hdrbuf + 
                    page_get_persistent_header_size()), (0));

        pagesize = env_get_persistent_pagesize(env);
        env_set_pagesize(env, pagesize);
        st = device->set_pagesize(device, pagesize);
        if (st) 
            goto fail_with_fake_cleansing;

        /*
         * can we use mmap?
         * TODO really necessary? code is already handled in 
         * __check_parameters() above
         */
#if HAVE_MMAP
        if (!(flags&HAM_DISABLE_MMAP)) {
            if (pagesize % os_get_granularity()==0)
                flags|=DB_USE_MMAP;
            else
                device->set_flags(device, flags|HAM_DISABLE_MMAP);
        }
        else {
            device->set_flags(device, flags|HAM_DISABLE_MMAP);
        }
        flags&=~HAM_DISABLE_MMAP; /* don't store this flag */
#else
        device->set_flags(device, flags|HAM_DISABLE_MMAP);
#endif

        /* 
         * check the file magic
         */
        if (env_get_magic(hdr, 0)!='H' ||
                env_get_magic(hdr, 1)!='A' ||
                env_get_magic(hdr, 2)!='M' ||
                env_get_magic(hdr, 3)!='\0') {
            ham_log(("invalid file type"));
            st = HAM_INV_FILE_HEADER;
            goto fail_with_fake_cleansing;
        }

        /* 
         * check the database version
         *
         * if this Database is from 1.0.x: force the PRE110-DAM
         * TODO this is done again some lines below; remove this and
         * replace it with a function __is_supported_version()
         */
        if (envheader_get_version(hdr, 0)!=HAM_VERSION_MAJ ||
                envheader_get_version(hdr, 1)!=HAM_VERSION_MIN) {
            /* before we yak about a bad DB, see if this feller is 
             * a 'backwards compatible' one (1.0.x - 1.0.9) */
            if (envheader_get_version(hdr, 0) == 1 &&
                envheader_get_version(hdr, 1) == 0 &&
                envheader_get_version(hdr, 2) <= 9) {
                env_set_legacy(env, 1);
            }
            else {
                ham_log(("invalid file version"));
                st = HAM_INV_FILE_VERSION;
                goto fail_with_fake_cleansing;
            }
        }

        st = 0;

fail_with_fake_cleansing:

        /* undo the headerpage fake first! */
        if (hdrpage_faked) {
            env_set_header_page(env, 0);
        }

        /* exit when an error was signaled */
        if (st) {
            (void)ham_env_close((ham_env_t *)env, HAM_DONT_CLEAR_LOG);
            return (st);
        }

        /*
         * now read the "real" header page and store it in the Environment
         */
        page=page_new(env);
        if (!page) {
            (void)ham_env_close((ham_env_t *)env, HAM_DONT_CLEAR_LOG);
            return (HAM_OUT_OF_MEMORY);
        }
        page_set_device(page, device);
        st=page_fetch(page);
        if (st) {
            page_delete(page);
            (void)ham_env_close((ham_env_t *)env, HAM_DONT_CLEAR_LOG);
            return (st);
        }
        env_set_header_page(env, page);
    }

    /* 
     * initialize the cache; the cache is needed during recovery, therefore
     * we have to create the cache BEFORE we attempt to recover
     */
    env_set_cache(env, new Cache(env, env_get_cachesize(env)));

    /*
     * open the logfile and check if we need recovery. first open the 
     * (physical) log and re-apply it. afterwards to the same with the
     * (logical) journal.
     */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY) {
        st=__recover(env, flags);
        if (st) {
            (void)ham_env_close((ham_env_t *)env, HAM_DONT_CLEAR_LOG);
            return (st);
        }
    }

    return (HAM_SUCCESS);
}

static ham_status_t
_local_fun_rename_db(Environment *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags)
{
    ham_u16_t dbi;
    ham_u16_t slot;

    /*
     * make sure that the environment was either created or opened, and 
     * a valid device exists
     */
    if (!env_get_device(env))
        return (HAM_NOT_READY);

    /*
     * check if a database with the new name already exists; also search 
     * for the database with the old name
     */
    slot=env_get_max_databases(env);
    ham_assert(env_get_max_databases(env) > 0, (0));
    for (dbi=0; dbi<env_get_max_databases(env); dbi++) {
        ham_u16_t name=index_get_dbname(env_get_indexdata_ptr(env, dbi));
        if (name==newname)
            return (HAM_DATABASE_ALREADY_EXISTS);
        if (name==oldname)
            slot=dbi;
    }

    if (slot==env_get_max_databases(env))
        return (HAM_DATABASE_NOT_FOUND);

    /*
     * replace the database name with the new name
     */
    index_set_dbname(env_get_indexdata_ptr(env, slot), newname);

    env_set_dirty(env);

    /* flush the header page if logging is enabled */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY)
        return (page_flush(env_get_header_page(env)));
    
    return (0);
}

static ham_status_t
_local_fun_erase_db(Environment *env, ham_u16_t name, ham_u32_t flags)
{
    Database *db;
    ham_status_t st;
    free_cb_context_t context;
    ham_backend_t *be;

    /*
     * check if this database is still open
     */
    db=env_get_list(env);
    while (db) {
        ham_u16_t dbname=index_get_dbname(env_get_indexdata_ptr(env,
                            db->get_indexdata_offset()));
        if (dbname==name)
            return (HAM_DATABASE_ALREADY_OPEN);
        db=db->get_next();
    }

    /*
     * if it's an in-memory environment: no need to go on, if the 
     * database was closed, it does no longer exist
     */
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
        return (HAM_DATABASE_NOT_FOUND);

    /*
     * temporarily load the database
     */
    st=ham_new((ham_db_t **)&db);
    if (st)
        return (st);
    st=ham_env_open_db((ham_env_t *)env, (ham_db_t *)db, name, 0, 0);
    if (st) {
        (void)ham_delete((ham_db_t *)db);
        return (st);
    }

    /* logging enabled? then the changeset and the log HAS to be empty */
#ifdef HAM_DEBUG
        if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY) {
            ham_assert(env_get_changeset(env).is_empty(), (""));
            ham_assert(env_get_log(env)->is_empty(), (""));
        }
#endif

    /*
     * delete all blobs and extended keys, also from the cache and
     * the extkey-cache
     *
     * also delete all pages and move them to the freelist; if they're 
     * cached, delete them from the cache
     */
    context.db=db;
    be=db->get_backend();
    if (!be || !be_is_active(be))
        return (HAM_INTERNAL_ERROR);

    if (!be->_fun_enumerate)
        return (HAM_NOT_IMPLEMENTED);

    st=be->_fun_enumerate(be, __free_inmemory_blobs_cb, &context);
    if (st) {
        (void)ham_close((ham_db_t *)db, 0);
        (void)ham_delete((ham_db_t *)db);
        return (st);
    }

    /* set database name to 0 and set the header page to dirty */
    index_set_dbname(env_get_indexdata_ptr(env, 
                        db->get_indexdata_offset()), 0);
    page_set_dirty(env_get_header_page(env));

    /* if logging is enabled: flush the changeset and the header page */
    if (st==0 && env_get_rt_flags(env)&HAM_ENABLE_RECOVERY) {
        ham_u64_t lsn;
        env_get_changeset(env).add_page(env_get_header_page(env));
        st=env_get_incremented_lsn(env, &lsn);
        if (st==0)
            st=env_get_changeset(env).flush(lsn);
    }

    /* clean up and return */
    (void)ham_close((ham_db_t *)db, 0);
    (void)ham_delete((ham_db_t *)db);

    return (0);
}

static ham_status_t
_local_fun_get_database_names(Environment *env, ham_u16_t *names, 
            ham_size_t *count)
{
    ham_u16_t name;
    ham_size_t i=0;
    ham_size_t max_names=0;
    ham_status_t st=0;

    max_names=*count;
    *count=0;

    /*
     * copy each database name in the array
     */
    ham_assert(env_get_max_databases(env) > 0, (0));
    for (i=0; i<env_get_max_databases(env); i++) {
        name = index_get_dbname(env_get_indexdata_ptr(env, i));
        if (name==0)
            continue;

        if (*count>=max_names) {
            st=HAM_LIMITS_REACHED;
            goto bail;
        }

        names[(*count)++]=name;
    }

bail:

    return st;
}

static ham_status_t
_local_fun_close(Environment *env, ham_u32_t flags)
{
    ham_status_t st;
    ham_status_t st2 = HAM_SUCCESS;
    ham_device_t *dev;
    ham_file_filter_t *file_head;

    /*
     * if we're not in read-only mode, and not an in-memory-database,
     * and the dirty-flag is true: flush the page-header to disk
     */
    if (env_get_header_page(env)
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
            && env_get_device(env)
            && env_get_device(env)->is_open(env_get_device(env))
            && (!(env_get_rt_flags(env)&HAM_READ_ONLY))) {
        st=page_flush(env_get_header_page(env));
        if (!st2) st2 = st;
    }

    /*
     * flush the freelist
     */
    st=freel_shutdown(env);
    if (st) {
        if (st2 == 0) 
            st2 = st;
    }

    dev=env_get_device(env);

    /*
     * close the header page
     *
     * !!
     * the last database, which was closed, has set the owner of the 
     * page to 0, which means that we can't call page_free, page_delete
     * etc. We have to use the device-routines.
     */
    if (env_get_header_page(env)) {
        ham_page_t *page=env_get_header_page(env);
        ham_assert(dev, (0));
        if (page_get_pers(page)) {
            st = dev->free_page(dev, page);
            if (!st2) 
                st2 = st;
        }
        allocator_free(env_get_allocator(env), page);
        env_set_header_page(env, 0);
    }

    /* 
     * flush all pages, get rid of the cache 
     */
    if (env_get_cache(env)) {
        (void)db_flush_all(env_get_cache(env), 0);
        delete env_get_cache(env);
        env_set_cache(env, 0);
    }

    /* 
     * close the device
     */
    if (dev) {
        if (dev->is_open(dev)) {
            if (!(env_get_rt_flags(env)&HAM_READ_ONLY)) {
                st = dev->flush(dev);
                if (!st2) 
                    st2 = st;
            }
            st = dev->close(dev);
            if (!st2) 
                st2 = st;
        }
        dev->destroy(dev);
        env_set_device(env, 0);
    }

    /*
     * close all file-level filters
     */
    file_head=env_get_file_filter(env);
    while (file_head) {
        ham_file_filter_t *next=file_head->_next;
        if (file_head->close_cb)
            file_head->close_cb((ham_env_t *)env, file_head);
        file_head=next;
    }
    env_set_file_filter(env, 0);

    /*
     * close the log and the journal
     */
    if (env_get_log(env)) {
        Log *log=env_get_log(env);
        st=log->close(flags&HAM_DONT_CLEAR_LOG);
        if (!st2) 
            st2 = st;
        delete log;
        env_set_log(env, 0);
    }
    if (env_get_journal(env)) {
        Journal *journal=env_get_journal(env);
        st=journal->close(flags&HAM_DONT_CLEAR_LOG);
        if (!st2) 
            st2 = st;
        delete journal;
        env_set_journal(env, 0);
    }

    return st2;
}

static ham_status_t 
_local_fun_get_parameters(Environment *env, ham_parameter_t *param)
{
    ham_parameter_t *p=param;

    if (p) {
        for (; p->name; p++) {
            switch (p->name) {
            case HAM_PARAM_CACHESIZE:
                p->value=env_get_cache(env)->get_capacity();
                break;
            case HAM_PARAM_PAGESIZE:
                p->value=env_get_pagesize(env);
                break;
            case HAM_PARAM_MAX_ENV_DATABASES:
                p->value=env_get_max_databases(env);
                break;
            case HAM_PARAM_GET_FLAGS:
                p->value=env_get_rt_flags(env);
                break;
            case HAM_PARAM_GET_FILEMODE:
                p->value=env_get_file_mode(env);
                break;
            case HAM_PARAM_GET_FILENAME:
                if (env_get_filename(env).size())
                    p->value=(ham_u64_t)(PTR_TO_U64(env_get_filename(env).c_str()));
                else
                    p->value=0;
                break;
            case HAM_PARAM_GET_STATISTICS:
                if (!p->value) {
                    ham_trace(("the value for parameter "
                               "'HAM_PARAM_GET_STATISTICS' must not be NULL "
                               "and reference a ham_statistics_t data "
                               "structure before invoking "
                               "ham_get_parameters"));
                    return (HAM_INV_PARAMETER);
                }
                else {
                    ham_status_t st = btree_stats_fill_ham_statistics_t(env, 0, 
                            (ham_statistics_t *)U64_TO_PTR(p->value));
                    if (st)
                        return st;
                }
                break;
            default:
                ham_trace(("unknown parameter %d", (int)p->name));
                return (HAM_INV_PARAMETER);
            }
        }
    }

    return (0);
}

static ham_status_t
_local_fun_flush(Environment *env, ham_u32_t flags)
{
    ham_status_t st;
    Database *db;
    ham_device_t *dev;

    (void)flags;

    /*
     * never flush an in-memory-database
     */
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
        return (0);

    dev = env_get_device(env);
    if (!dev)
        return HAM_NOT_INITIALIZED;

    /*
     * flush the open backends
     */
    db = env_get_list(env);
    while (db) 
    {
        ham_backend_t *be=db->get_backend();

        if (!be || !be_is_active(be))
            return HAM_NOT_INITIALIZED;
        if (!be->_fun_flush)
            return (HAM_NOT_IMPLEMENTED);
        st = be->_fun_flush(be);
        if (st)
            return st;
        db = db->get_next();
    }

    /*
     * update the header page, if necessary
     */
    if (env_is_dirty(env)) {
        st=page_flush(env_get_header_page(env));
        if (st)
            return st;
    }

    /*
     * flush all open pages to disk
     */
    st=db_flush_all(env_get_cache(env), DB_FLUSH_NODELETE);
    if (st)
        return st;

    /*
     * flush the device - this usually causes a fsync()
     */
    st=dev->flush(dev);
    if (st)
        return st;

    return (HAM_SUCCESS);
}

static ham_status_t 
_local_fun_create_db(Environment *env, Database *db, 
        ham_u16_t dbname, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    ham_u16_t keysize = 0;
    ham_u64_t cachesize = 0;
    ham_u16_t dam = 0;
    ham_u16_t dbi;
    ham_size_t i;
    ham_backend_t *be;
    ham_u32_t pflags;

    db->set_rt_flags(0);

    /* parse parameters */
    st=__check_create_parameters(env, db, 0, &flags, param, 
            0, &keysize, &cachesize, &dbname, 0, &dam, HAM_TRUE);
    if (st)
        return (st);

    /* store the env pointer in the database */
    db->set_env(env);

    /* reset all DB performance data */
    btree_stats_init_dbdata(db, db->get_perf_data());

    /*
     * set the flags; strip off run-time (per session) flags for the 
     * backend::create() method though.
     */
    db->set_rt_flags(flags);
    pflags=flags;
    pflags&=~(HAM_DISABLE_VAR_KEYLEN
             |HAM_CACHE_STRICT
             |HAM_CACHE_UNLIMITED
             |HAM_DISABLE_MMAP
             |HAM_WRITE_THROUGH
             |HAM_READ_ONLY
             |HAM_DISABLE_FREELIST_FLUSH
             |HAM_ENABLE_RECOVERY
             |HAM_AUTO_RECOVERY
             |HAM_ENABLE_TRANSACTIONS
             |HAM_SORT_DUPLICATES
             |DB_USE_MMAP
             |DB_ENV_IS_PRIVATE);

    /*
     * transfer the ownership of the header page to this Database
     */
    page_set_owner(env_get_header_page(env), db);
    ham_assert(env_get_header_page(env), (0));

    /*
     * check if this database name is unique
     */
    ham_assert(env_get_max_databases(env) > 0, (0));
    for (i=0; i<env_get_max_databases(env); i++) {
        ham_u16_t name = index_get_dbname(env_get_indexdata_ptr(env, i));
        if (!name)
            continue;
        if (name==dbname || dbname==HAM_FIRST_DATABASE_NAME) {
            (void)ham_close((ham_db_t *)db, 0);
            return (HAM_DATABASE_ALREADY_EXISTS);
        }
    }

    /*
     * find a free slot in the indexdata array and store the 
     * database name
     */
    ham_assert(env_get_max_databases(env) > 0, (0));
    for (dbi=0; dbi<env_get_max_databases(env); dbi++) {
        ham_u16_t name = index_get_dbname(env_get_indexdata_ptr(env, dbi));
        if (!name) {
            index_set_dbname(env_get_indexdata_ptr(env, dbi), dbname);
            db->set_indexdata_offset(dbi);
            break;
        }
    }
    if (dbi==env_get_max_databases(env)) {
        (void)ham_close((ham_db_t *)db, 0);
        return (HAM_LIMITS_REACHED);
    }

    /* logging enabled? then the changeset and the log HAS to be empty */
#ifdef HAM_DEBUG
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY) {
        ham_assert(env_get_changeset(env).is_empty(), (""));
        ham_assert(env_get_log(env)->is_empty(), (""));
    }
#endif

    /* create the backend */
    be=db->get_backend();
    if (be==NULL) {
        be=btree_create(db, flags);
        if (!be) {
            st=HAM_OUT_OF_MEMORY;
            (void)ham_close((ham_db_t *)db, 0);
            goto bail;
        }
        /* store the backend in the database */
        db->set_backend(be);
    }

    /* initialize the backend */
    st=be->_fun_create(be, keysize, pflags);
    if (st) {
        (void)ham_close((ham_db_t *)db, 0);
        goto bail;
    }

    ham_assert(be_is_active(be) != 0, (0));

    /*
     * initialize the remaining function pointers in Database
     */
    st=db->initialize_local();
    if (st) {
        (void)ham_close((ham_db_t *)db, 0);
        goto bail;
    }

    /*
     * set the default key compare functions
     */
    if (db->get_rt_flags()&HAM_RECORD_NUMBER) {
        ham_set_compare_func((ham_db_t *)db, db_default_recno_compare);
    }
    else {
        ham_set_compare_func((ham_db_t *)db, db_default_compare);
        ham_set_prefix_compare_func((ham_db_t *)db, db_default_prefix_compare);
    }
    ham_set_duplicate_compare_func((ham_db_t *)db, db_default_compare);
    env_set_dirty(env);

    /* 
     * finally calculate and store the data access mode 
     */
    if (env_get_version(env, 0) == 1 &&
        env_get_version(env, 1) == 0 &&
        env_get_version(env, 2) <= 9) {
        dam |= HAM_DAM_ENFORCE_PRE110_FORMAT;
        env_set_legacy(env, 1);
    }
    if (!dam) {
        dam=(flags&HAM_RECORD_NUMBER)
            ? HAM_DAM_SEQUENTIAL_INSERT 
            : HAM_DAM_RANDOM_WRITE;
    }
    db->set_data_access_mode(dam);

    /* 
     * set the key compare function
     */
    if (db->get_rt_flags()&HAM_RECORD_NUMBER) {
        ham_set_compare_func((ham_db_t *)db, db_default_recno_compare);
    }
    else {
        ham_set_compare_func((ham_db_t *)db, db_default_compare);
        ham_set_prefix_compare_func((ham_db_t *)db, db_default_prefix_compare);
    }
    ham_set_duplicate_compare_func((ham_db_t *)db, db_default_compare);

    /*
     * on success: store the open database in the environment's list of
     * opened databases
     */
    db->set_next(env_get_list(env));
    env_set_list(env, db);

bail:
    /* if logging is enabled: flush the changeset and the header page */
    if (st==0 && env_get_rt_flags(env)&HAM_ENABLE_RECOVERY) {
        ham_u64_t lsn;
        env_get_changeset(env).add_page(env_get_header_page(env));
        st=env_get_incremented_lsn(env, &lsn);
        if (st==0)
            st=env_get_changeset(env).flush(lsn);
    }

    return (st);
}

static ham_status_t 
_local_fun_open_db(Environment *env, Database *db, 
        ham_u16_t name, ham_u32_t flags, const ham_parameter_t *param)
{
    Database *head;
    ham_status_t st;
    ham_u16_t dam = 0;
    ham_u64_t cachesize = 0;
    ham_backend_t *be = 0;
    ham_u16_t dbi;

    /*
     * make sure that this database is not yet open/created
     */
    if (db->is_active()) {
        ham_trace(("parameter 'db' is already initialized"));
        return (HAM_DATABASE_ALREADY_OPEN);
    }

    db->set_rt_flags(0);

    /* parse parameters */
    st=__check_create_parameters(env, db, 0, &flags, param, 
            0, 0, &cachesize, &name, 0, &dam, HAM_FALSE);
    if (st)
        return (st);

    /*
     * make sure that this database is not yet open
     */
    head=env_get_list(env);
    while (head) {
        db_indexdata_t *ptr=env_get_indexdata_ptr(env, 
                                head->get_indexdata_offset());
        if (index_get_dbname(ptr)==name)
            return (HAM_DATABASE_ALREADY_OPEN);
        head=head->get_next();
    }

    ham_assert(env_get_allocator(env), (""));
    ham_assert(env_get_device(env), (""));
    ham_assert(0 != env_get_header_page(env), (0));
    ham_assert(env_get_max_databases(env) > 0, (0));

    /* store the env pointer in the database */
    db->set_env(env);

    /* reset the DB performance data */
    btree_stats_init_dbdata(db, db->get_perf_data());

    /*
     * search for a database with this name
     */
    for (dbi=0; dbi<env_get_max_databases(env); dbi++) {
        db_indexdata_t *idx=env_get_indexdata_ptr(env, dbi);
        ham_u16_t dbname = index_get_dbname(idx);
        if (!dbname)
            continue;
        if (name==HAM_FIRST_DATABASE_NAME || name==dbname) {
            db->set_indexdata_offset(dbi);
            break;
        }
    }

    if (dbi==env_get_max_databases(env)) {
        (void)ham_close((ham_db_t *)db, 0);
        return (HAM_DATABASE_NOT_FOUND);
    }

    /* create the backend */
    be=db->get_backend();
    if (be==NULL) {
        be=btree_create(db, flags);
        if (!be) {
            (void)ham_close((ham_db_t *)db, 0);
            return (HAM_OUT_OF_MEMORY);
        }
        /* store the backend in the database */
        db->set_backend(be);
    }

    /* initialize the backend */
    st=be->_fun_open(be, flags);
    if (st) {
        (void)ham_close((ham_db_t *)db, 0);
        return (st);
    }

    ham_assert(be_is_active(be) != 0, (0));

    /*
     * initialize the remaining function pointers in Database
     */
    st=db->initialize_local();
    if (st) {
        (void)ham_close((ham_db_t *)db, 0);
        return (st);
    }

    /* 
     * set the database flags; strip off the persistent flags that may have been
     * set by the caller, before mixing in the persistent flags as obtained 
     * from the backend.
     */
    flags &= (HAM_DISABLE_VAR_KEYLEN
             |HAM_CACHE_STRICT
             |HAM_CACHE_UNLIMITED
             |HAM_DISABLE_MMAP
             |HAM_WRITE_THROUGH
             |HAM_READ_ONLY
             |HAM_DISABLE_FREELIST_FLUSH
             |HAM_ENABLE_RECOVERY
             |HAM_AUTO_RECOVERY
             |HAM_ENABLE_TRANSACTIONS
             |HAM_SORT_DUPLICATES
             |DB_USE_MMAP
             |DB_ENV_IS_PRIVATE);
    db->set_rt_flags(flags|be_get_flags(be));
    ham_assert(!(be_get_flags(be)&HAM_DISABLE_VAR_KEYLEN), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_CACHE_STRICT), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_CACHE_UNLIMITED), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_DISABLE_MMAP), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_WRITE_THROUGH), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_READ_ONLY), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_DISABLE_FREELIST_FLUSH), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_ENABLE_RECOVERY), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_AUTO_RECOVERY), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_ENABLE_TRANSACTIONS), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&DB_USE_MMAP), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));

    /*
     * SORT_DUPLICATES is only allowed if the Database was created
     * with ENABLE_DUPLICATES!
     */
    if ((flags&HAM_SORT_DUPLICATES) 
            && !(db->get_rt_flags()&HAM_ENABLE_DUPLICATES)) {
        ham_trace(("flag HAM_SORT_DUPLICATES set but duplicates are not "
                   "enabled for this Database"));
        (void)ham_close((ham_db_t *)db, 0);
        return (HAM_INV_PARAMETER);
    }

    /* 
     * finally calculate and store the data access mode 
     */
    if (env_get_version(env, 0) == 1 &&
        env_get_version(env, 1) == 0 &&
        env_get_version(env, 2) <= 9) {
        dam |= HAM_DAM_ENFORCE_PRE110_FORMAT;
        env_set_legacy(env, 1);
    }
    if (!dam) {
        dam=(db->get_rt_flags()&HAM_RECORD_NUMBER)
            ? HAM_DAM_SEQUENTIAL_INSERT 
            : HAM_DAM_RANDOM_WRITE;
    }
    db->set_data_access_mode(dam);

    /* 
     * set the key compare function
     */
    if (db->get_rt_flags()&HAM_RECORD_NUMBER) {
        ham_set_compare_func((ham_db_t *)db, db_default_recno_compare);
    }
    else {
        ham_set_compare_func((ham_db_t *)db, db_default_compare);
        ham_set_prefix_compare_func((ham_db_t *)db, db_default_prefix_compare);
    }
    ham_set_duplicate_compare_func((ham_db_t *)db, db_default_compare);

    /*
     * on success: store the open database in the environment's list of
     * opened databases
     */
    db->set_next(env_get_list(env));
    env_set_list(env, db);

    return (0);
}

static ham_status_t 
_local_fun_txn_begin(Environment *env, ham_txn_t **txn, 
                    const char *name, ham_u32_t flags)
{
    ham_status_t st;

    st=txn_begin(txn, env, name, flags);
    if (st) {
        txn_free(*txn);
        *txn=0;
    }

    /* append journal entry */
    if (st==0
            && env_get_rt_flags(env)&HAM_ENABLE_RECOVERY
            && env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS) {
        ham_u64_t lsn;
        st=env_get_incremented_lsn(env, &lsn);
        if (st==0)
            st=env_get_journal(env)->append_txn_begin(*txn, env, name, lsn);
    }

    return (st);
}

static ham_status_t
_local_fun_txn_commit(Environment *env, ham_txn_t *txn, ham_u32_t flags)
{
    ham_status_t st;

    /* append journal entry */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY
            && env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS) {
        ham_u64_t lsn;
        st=env_get_incremented_lsn(env, &lsn);
        if (st)
            return (st);
        st=env_get_journal(env)->append_txn_commit(txn, lsn);
        if (st)
            return (st);
    }

    st=txn_commit(txn, flags);

    /* on success: flush all open file handles if HAM_WRITE_THROUGH is 
     * enabled; then purge caches */
    if (st==0) {
        if (env_get_rt_flags(env)&HAM_WRITE_THROUGH) {
            ham_device_t *device=env_get_device(env);
            (void)env_get_log(env)->flush();
            (void)device->flush(device);
        }
    }

    return (st);
}

static ham_status_t
_local_fun_txn_abort(Environment *env, ham_txn_t *txn, ham_u32_t flags)
{
    /* an ugly hack - txn_abort() will free the txn structure, but we need
     * it for the journal; therefore create a temp. copy which we can use
     * later */
    ham_txn_t copy=*txn;
    ham_status_t st=txn_abort(txn, flags);

    /* append journal entry */
    if (st==0
            && env_get_rt_flags(env)&HAM_ENABLE_RECOVERY
            && env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS) {
        ham_u64_t lsn;
        st=env_get_incremented_lsn(env, &lsn);
        if (st==0)
            st=env_get_journal(env)->append_txn_abort(&copy, lsn);
    }

    /* on success: flush all open file handles if HAM_WRITE_THROUGH is 
     * enabled; then purge caches */
    if (st==0) {
        if (env_get_rt_flags(env)&HAM_WRITE_THROUGH) {
            ham_device_t *device=env_get_device(env);
            (void)env_get_log(env)->flush();
            (void)device->flush(device);
        }
    }

    return (st);
}

ham_status_t
env_initialize_local(Environment *env)
{
    env->_fun_create             =_local_fun_create;
    env->_fun_open               =_local_fun_open;
    env->_fun_rename_db          =_local_fun_rename_db;
    env->_fun_erase_db           =_local_fun_erase_db;
    env->_fun_get_database_names =_local_fun_get_database_names;
    env->_fun_get_parameters     =_local_fun_get_parameters;
    env->_fun_create_db          =_local_fun_create_db;
    env->_fun_open_db            =_local_fun_open_db;
    env->_fun_flush              =_local_fun_flush;
    env->_fun_close              =_local_fun_close;
    env->_fun_txn_begin          =_local_fun_txn_begin;
    env->_fun_txn_commit         =_local_fun_txn_commit;
    env->_fun_txn_abort          =_local_fun_txn_abort;

    return (0);
}

void
env_append_txn(Environment *env, ham_txn_t *txn)
{
    txn_set_env(txn, env);

    if (!env_get_newest_txn(env)) {
        ham_assert(env_get_oldest_txn(env)==0, (""));
        env_set_oldest_txn(env, txn);
        env_set_newest_txn(env, txn);
    }
    else {
        txn_set_older(txn, env_get_newest_txn(env));
        txn_set_newer(env_get_newest_txn(env), txn);
        env_set_newest_txn(env, txn);
        /* if there's no oldest txn (this means: all txn's but the
         * current one were already flushed) then set this txn as
         * the oldest txn */
        if (!env_get_oldest_txn(env))
            env_set_oldest_txn(env, txn);
    }
}

void
env_remove_txn(Environment *env, ham_txn_t *txn)
{
    if (env_get_newest_txn(env)==txn) {
        env_set_newest_txn(env, txn_get_older(txn));
    }

    if (env_get_oldest_txn(env)==txn) {
        ham_txn_t *n=txn_get_newer(txn);
        env_set_oldest_txn(env, n);
        if (n)
            txn_set_older(n, 0);
    }
    else {
        ham_assert(!"not yet implemented", (""));
    }
}

static ham_status_t
__flush_txn(Environment *env, ham_txn_t *txn)
{
    ham_status_t st=0;
    txn_op_t *op=txn_get_oldest_op(txn);
    txn_cursor_t *cursor=0;

    while (op) {
        txn_opnode_t *node=txn_op_get_node(op);
        ham_backend_t *be=txn_opnode_get_db(node)->get_backend();
        if (!be)
            return (HAM_INTERNAL_ERROR);

        /* make sure that this op was not yet flushed - this would be
         * a serious bug */
        ham_assert(txn_op_get_flags(op)!=TXN_OP_FLUSHED, (""));

        /* logging enabled? then the changeset and the log HAS to be empty */
#ifdef HAM_DEBUG
        if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY) {
            ham_assert(env_get_changeset(env).is_empty(), (""));
            ham_assert(env_get_log(env)->is_empty(), (""));
        }
#endif

        /* currently, some low-level functions (i.e. in log.c) still need
         * to know about the Transaction that we flush, therefore set the
         * env_flushed_txn pointer */
        env_set_flushed_txn(env, txn);

        /* 
         * depending on the type of the operation: actually perform the
         * operation on the btree 
         *
         * if the txn-op has a cursor attached, then all (txn)cursors 
         * which are coupled to this op have to be uncoupled, and their 
         * parent (btree) cursor must be coupled to the btree item instead.
         */
        if ((txn_op_get_flags(op)&TXN_OP_INSERT)
                || (txn_op_get_flags(op)&TXN_OP_INSERT_OW)
                || (txn_op_get_flags(op)&TXN_OP_INSERT_DUP)) {
            ham_u32_t additional_flag=
                (txn_op_get_flags(op)&TXN_OP_INSERT_DUP)
                    ? HAM_DUPLICATE
                    : HAM_OVERWRITE;
            if (!txn_op_get_cursors(op)) {
                st=be->_fun_insert(be, txn_opnode_get_key(node), 
                        txn_op_get_record(op), 
                        txn_op_get_orig_flags(op)|additional_flag);
            }
            else {
                txn_cursor_t *tc2, *tc1=txn_op_get_cursors(op);
                Cursor *c2, *c1=txn_cursor_get_parent(tc1);
                /* pick the first cursor, get the parent/btree cursor and
                 * insert the key/record pair in the btree. The btree cursor
                 * then will be coupled to this item. */
                st=btree_cursor_insert(c1->get_btree_cursor(), 
                        txn_opnode_get_key(node), txn_op_get_record(op), 
                        txn_op_get_orig_flags(op)|additional_flag);
                if (st)
                    goto bail;

                /* uncouple the cursor from the txn-op, and remove it */
                txn_op_remove_cursor(op, tc1);
                c1->couple_to_btree();
                c1->set_to_nil(Cursor::CURSOR_TXN);

                /* all other (btree) cursors need to be coupled to the same 
                 * item as the first one. */
                while ((tc2=txn_op_get_cursors(op))) {
                    txn_op_remove_cursor(op, tc2);
                    c2=txn_cursor_get_parent(tc2);
                    btree_cursor_couple_to_other(c2->get_btree_cursor(), 
                                c1->get_btree_cursor());
                    c2->couple_to_btree();
                    c2->set_to_nil(Cursor::CURSOR_TXN);
                }
            }
        }
        else if (txn_op_get_flags(op)&TXN_OP_ERASE) {
            if (txn_op_get_referenced_dupe(op)) {
                st=btree_erase_duplicate((ham_btree_t *)be, 
                        txn_opnode_get_key(node), 
                        txn_op_get_referenced_dupe(op), txn_op_get_flags(op));
            }
            else {
                st=be->_fun_erase(be, txn_opnode_get_key(node), 
                        txn_op_get_flags(op));
            }
        }

bail:
        /* now flush the changeset to disk */
        if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY) {
            env_get_changeset(env).add_page(env_get_header_page(env));
            st=env_get_changeset(env).flush(txn_op_get_lsn(op));
        }

        env_set_flushed_txn(env, 0);

        if (st) {
            ham_trace(("failed to flush op: %d (%s)", 
                            (int)st, ham_strerror(st)));
            return (st);
        }

        /* 
         * this op is about to be flushed! 
         *
         * as a concequence, all (txn)cursors which are coupled to this op
         * have to be uncoupled, as their parent (btree) cursor was
         * already coupled to the btree item instead
         */
        txn_op_set_flags(op, TXN_OP_FLUSHED);
        while ((cursor=txn_op_get_cursors(op))) {
            Cursor *pc=txn_cursor_get_parent(cursor);
            ham_assert(pc->get_txn_cursor()==cursor, (""));
            pc->couple_to_btree();
            pc->set_to_nil(Cursor::CURSOR_TXN);
        }

        /* continue with the next operation of this txn */
        op=txn_op_get_next_in_txn(op);
    }

    return (0);
}

ham_status_t
env_flush_committed_txns(Environment *env)
{
    ham_txn_t *oldest;

    ham_assert(!(env_get_rt_flags(env)&DB_DISABLE_AUTO_FLUSH), (""));

    /* always get the oldest transaction; if it was committed: flush 
     * it; if it was aborted: discard it; otherwise return */
    while ((oldest=env_get_oldest_txn(env))) {
        if (txn_get_flags(oldest)&TXN_STATE_COMMITTED) {
            ham_status_t st=__flush_txn(env, oldest);
            if (st)
                return (st);
        }
        else if (txn_get_flags(oldest)&TXN_STATE_ABORTED) {
            ; /* nop */
        }
        else
            break;

        /* now remove the txn from the linked list */
        env_remove_txn(env, oldest);

        /* and free the whole memory */
        txn_free(oldest);
    }

    /* clear the changeset; if the loop above was not entered or the 
     * transaction was empty then it may still contain pages */
    env_get_changeset(env).clear();

    return (0);
}

ham_status_t
env_get_incremented_lsn(Environment *env, ham_u64_t *lsn) 
{
    Journal *j=env_get_journal(env);
    if (j) {
        if (j->get_lsn()==0xffffffffffffffffull) {
            ham_log(("journal limits reached (lsn overflow) - please reorg"));
            return (HAM_LIMITS_REACHED);
        }
        *lsn=j->get_incremented_lsn();
        return (0);
    }
    else {
        ham_assert(!"need lsn but have no journal!", (""));
        return (HAM_INTERNAL_ERROR);
    }
}

static ham_status_t
__purge_cache_max20(Environment *env)
{
    ham_status_t st;
    ham_page_t *page;
    Cache *cache=env_get_cache(env);
    unsigned i, max_pages=(unsigned)cache->get_cur_elements();

    /* don't remove pages from the cache if it's an in-memory database */
    if ((env_get_rt_flags(env)&HAM_IN_MEMORY_DB))
        return (0);
    if (!cache->is_too_big())
        return (0);

    /* 
     * max_pages specifies how many pages we try to flush in case the
     * cache is full. some benchmarks showed that 10% is a good value. 
     *
     * if STRICT cache limits are enabled then purge as much as we can
     */
    if (!(env_get_rt_flags(env)&HAM_CACHE_STRICT)) {
        max_pages/=10;
        /* but still we set an upper limit to avoid IO spikes */
        if (max_pages>20)
            max_pages=20;
    }

    /* try to free 10% of the unused pages */
    for (i=0; i<max_pages; i++) {
        page=cache->get_unused_page();
        if (!page) {
            if (i==0 && (env_get_rt_flags(env)&HAM_CACHE_STRICT)) 
                return (HAM_CACHE_FULL);
            else
                break;
        }

        st=db_write_page_and_delete(page, 0);
        if (st)
            return (st);
    }

    if (i==max_pages && max_pages!=0)
        return (HAM_LIMITS_REACHED);
    return (0);
}

ham_status_t
env_purge_cache(Environment *env)
{
    ham_status_t st;
    Cache *cache=env_get_cache(env);

    do {
        st=__purge_cache_max20(env);
        if (st && st!=HAM_LIMITS_REACHED)
            return st;
    } while (st==HAM_LIMITS_REACHED && cache->is_too_big());

    return (0);
}
