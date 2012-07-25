/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
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
#include "txn.h"
#include "env.h"
#include "mem.h"
#include "cursor.h"

#if HAM_ENABLE_REMOTE

#define CURL_STATICLIB /* otherwise libcurl uses wrong __declspec */
#include <curl/curl.h>
#include <curl/easy.h>

#include "protocol/protocol.h"

typedef struct curl_buffer_t
{
    ham_size_t packed_size;
    ham_u8_t *packed_data;
    ham_size_t offset;
    proto_wrapper_t *wrapper;
    Allocator *alloc;
} curl_buffer_t;

static size_t
__writefunc(void *buffer, size_t size, size_t nmemb, void *ptr)
{
    curl_buffer_t *buf=(curl_buffer_t *)ptr;
    char *cbuf=(char *)buffer;
    ham_size_t payload_size=0;

    if (buf->offset==0) {
        if (*(ham_u32_t *)&cbuf[0]!=ham_db2h32(HAM_TRANSFER_MAGIC_V1)) {
            ham_trace(("invalid protocol version"));
            return (0);
        }
        payload_size=ham_h2db32(*(ham_u32_t *)&cbuf[4]);

        /* did we receive the whole data in this packet? */
        if (payload_size+8==size*nmemb) {
            buf->wrapper=proto_unpack((ham_size_t)(size*nmemb), 
                        (ham_u8_t *)&cbuf[0]);
            if (!buf->wrapper)
                return (0);
            return (size*nmemb);
        }

        /* otherwise we have to buffer the received data */
        buf->packed_size=payload_size+8;
        buf->packed_data=(ham_u8_t *)buf->alloc->alloc(buf->packed_size);
        if (!buf->packed_data)
            return (0);
        memcpy(buf->packed_data, &cbuf[0], size*nmemb);
        buf->offset+=(ham_size_t)(size*nmemb);
    }
    /* append to an existing buffer? */
    else {
        memcpy(buf->packed_data+buf->offset, &cbuf[0], size*nmemb);
        buf->offset+=(ham_size_t)(size*nmemb);
    }

    /* check if we've received the whole data */
    if (buf->offset==buf->packed_size) {
        buf->wrapper=proto_unpack(buf->packed_size, buf->packed_data);
        if (!buf->wrapper)
            return (0);
        buf->alloc->free(buf->packed_data);
        if (!buf->wrapper)
            return 0;
    }

    return (size*nmemb);
}

static size_t
__readfunc(char *buffer, size_t size, size_t nmemb, void *ptr)
{
    curl_buffer_t *buf=(curl_buffer_t *)ptr;
    size_t remaining=buf->packed_size-buf->offset;

    if (remaining==0)
        return (0);

    if (nmemb>remaining)
        nmemb=remaining;

    memcpy(buffer, buf->packed_data+buf->offset, nmemb);
    buf->offset+=(ham_size_t)nmemb;
    return (nmemb);
}

#define SETOPT(curl, opt, val)                                                \
                    if ((cc=curl_easy_setopt(curl, opt, val))) {              \
                        ham_log(("curl_easy_setopt failed: %d/%s", cc,        \
                                    curl_easy_strerror(cc)));                 \
                        return (HAM_INTERNAL_ERROR);                          \
                    }

static ham_status_t
_perform_request(Environment *env, CURL *handle, proto_wrapper_t *request,
                proto_wrapper_t **reply)
{
    CURLcode cc;
    long response=0;
    char header[128];
    curl_buffer_t rbuf={0};
    curl_buffer_t wbuf={0};
    struct curl_slist *slist=0;

    wbuf.alloc=env->get_allocator();

    *reply=0;

    if (!proto_pack(request, wbuf.alloc, &rbuf.packed_data,
                &rbuf.packed_size)) {
        ham_log(("protoype proto_pack failed"));
        return (HAM_INTERNAL_ERROR);
    }

    sprintf(header, "Content-Length: %u", rbuf.packed_size);
    slist=curl_slist_append(slist, header);
    slist=curl_slist_append(slist, "Transfer-Encoding:");
    slist=curl_slist_append(slist, "Expect:");

#ifdef HAM_DEBUG
    SETOPT(handle, CURLOPT_VERBOSE, 1);
#endif
    SETOPT(handle, CURLOPT_URL, env->get_filename().c_str());
    SETOPT(handle, CURLOPT_READFUNCTION, __readfunc);
    SETOPT(handle, CURLOPT_READDATA, &rbuf);
    SETOPT(handle, CURLOPT_UPLOAD, 1);
    SETOPT(handle, CURLOPT_PUT, 1);
    SETOPT(handle, CURLOPT_WRITEFUNCTION, __writefunc);
    SETOPT(handle, CURLOPT_WRITEDATA, &wbuf);
    SETOPT(handle, CURLOPT_HTTPHEADER, slist);

    cc=curl_easy_perform(handle);

    if (rbuf.packed_data)
        env->get_allocator()->free(rbuf.packed_data);
    curl_slist_free_all(slist);

    if (cc) {
        ham_trace(("network transmission failed: %s", curl_easy_strerror(cc)));
        return (HAM_NETWORK_ERROR);
    }

    cc=curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &response);
    if (cc) {
        ham_trace(("network transmission failed: %s", curl_easy_strerror(cc)));
        return (HAM_NETWORK_ERROR);
    }

    if (response!=200) {
        ham_trace(("server returned error %u", response));
        return (HAM_NETWORK_ERROR);
    }

    *reply=wbuf.wrapper;

    return (0);
}

static ham_status_t 
_remote_fun_create(Environment *env, const char *filename,
            ham_u32_t flags, ham_u32_t mode, const ham_parameter_t *param)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    CURL *handle=curl_easy_init();

    request=proto_init_connect_request(filename);

    st=_perform_request(env, handle, request, &reply);
    proto_delete(request);
    if (st) {
        curl_easy_cleanup(handle);
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_connect_reply(reply), (""));

    st=proto_connect_reply_get_status(reply);
    if (st==0) {
        env->set_curl(handle);
        env->set_flags(env->get_flags()
                    |proto_connect_reply_get_env_flags(reply));
    }

    proto_delete(reply);

    return (st);
}

static ham_status_t 
_remote_fun_open(Environment *env, const char *filename, ham_u32_t flags, 
        const ham_parameter_t *param)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    CURL *handle=curl_easy_init();

    request=proto_init_connect_request(filename);

    st=_perform_request(env, handle, request, &reply);
    proto_delete(request);
    if (st) {
        curl_easy_cleanup(handle);
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_connect_reply(reply), (""));

    st=proto_connect_reply_get_status(reply);
    if (st==0) {
        env->set_curl(handle);
        env->set_flags(env->get_flags()
                    |proto_connect_reply_get_env_flags(reply));
    }

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_rename_db(Environment *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;

    request=proto_init_env_rename_request(oldname, newname, flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_rename_reply(reply), (""));

    st=proto_env_rename_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_erase_db(Environment *env, ham_u16_t name, ham_u32_t flags)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    
    request=proto_init_env_erase_db_request(name, flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_erase_db_reply(reply), (""));

    st=proto_env_erase_db_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_get_database_names(Environment *env, ham_u16_t *names, 
            ham_size_t *count)
{
    ham_status_t st;
    ham_size_t i;
    proto_wrapper_t *request, *reply;

    request=proto_init_env_get_database_names_request();

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_get_database_names_reply(reply), (""));

    st=proto_env_get_database_names_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    /* copy the retrieved names */
    for (i=0; i<proto_env_get_database_names_reply_get_names_size(reply)
            && i<*count; i++) {
        names[i]=proto_env_get_database_names_reply_get_names(reply)[i];
    }

    *count=i;

    proto_delete(reply);

    return (0);
}

static ham_status_t 
_remote_fun_env_get_parameters(Environment *env, ham_parameter_t *param)
{
    static char filename[1024];
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    ham_size_t i=0, num_names=0;
    ham_u32_t *names;
    ham_parameter_t *p;
    
    /* count number of parameters */
    p=param;
    if (p) {
        for (; p->name; p++) {
            num_names++;
        }
    }

    /* allocate a memory and copy the parameter names */
    names=(ham_u32_t *)env->get_allocator()->alloc(num_names*sizeof(ham_u32_t));
    if (!names)
        return (HAM_OUT_OF_MEMORY);
    p=param;
    if (p) {
        for (i=0; p->name; p++) {
            names[i]=p->name;
            i++;
        }
    }

    request=proto_init_env_get_parameters_request(names, num_names);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);

    env->get_allocator()->free(names);

    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_get_parameters_reply(reply), (""));

    st=proto_env_get_parameters_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    p=param;
    while (p && p->name) {
        switch (p->name) {
        case HAM_PARAM_CACHESIZE:
            ham_assert(proto_env_get_parameters_reply_has_cachesize(reply), (""));
            p->value=proto_env_get_parameters_reply_get_cachesize(reply);
            break;
        case HAM_PARAM_PAGESIZE:
            ham_assert(proto_env_get_parameters_reply_has_pagesize(reply), (""));
            p->value=proto_env_get_parameters_reply_get_pagesize(reply);
            break;
        case HAM_PARAM_MAX_ENV_DATABASES:
            ham_assert(proto_env_get_parameters_reply_has_max_env_databases(reply), (""));
            p->value=proto_env_get_parameters_reply_get_max_env_databases(reply);
            break;
        case HAM_PARAM_GET_FLAGS:
            ham_assert(proto_env_get_parameters_reply_has_flags(reply), (""));
            p->value=proto_env_get_parameters_reply_get_flags(reply);
            break;
        case HAM_PARAM_GET_FILEMODE:
            ham_assert(proto_env_get_parameters_reply_has_filemode(reply), (""));
            p->value=proto_env_get_parameters_reply_get_filemode(reply);
            break;
        case HAM_PARAM_GET_FILENAME:
            if (proto_env_get_parameters_reply_has_filename(reply)) {
                strncpy(filename, 
                        proto_env_get_parameters_reply_get_filename(reply),
                            sizeof(filename));
                p->value=(ham_u64_t)(&filename[0]);
            }
            break;
        default:
            ham_trace(("unknown parameter %d", (int)p->name));
            break;
        }
        p++;
    }

    proto_delete(reply);

    return (0);
}

static ham_status_t
_remote_fun_env_flush(Environment *env, ham_u32_t flags)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;

    request=proto_init_env_flush_request(flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_flush_reply(reply), (""));

    st=proto_env_flush_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

static ham_status_t 
_remote_fun_create_db(Environment *env, Database *db, 
        ham_u16_t dbname, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    ham_size_t i=0, num_params=0;
    ham_u32_t *names;
    ham_u64_t *values;
    const ham_parameter_t *p;
    
    /* count number of parameters */
    p=param;
    if (p) {
        for (; p->name; p++) {
            num_params++;
        }
    }

    /* allocate a memory and copy the parameter names */
    names=(ham_u32_t *)env->get_allocator()->alloc(num_params*sizeof(ham_u32_t));
    values=(ham_u64_t *)env->get_allocator()->alloc(num_params*sizeof(ham_u64_t));
    if (!names || !values)
        return (HAM_OUT_OF_MEMORY);
    p=param;
    if (p) {
        for (; p->name; p++) {
            names[i]=p->name;
            values[i]=p->value;
            i++;
        }
    }

    request=proto_init_env_create_db_request(dbname, flags, 
                names, values, num_params);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);

    env->get_allocator()->free(names);
    env->get_allocator()->free(values);

    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_create_db_reply(reply), (""));

    st=proto_env_create_db_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    db->set_remote_handle(proto_env_create_db_reply_get_db_handle(reply));
    db->set_rt_flags(proto_env_create_db_reply_get_flags(reply));

    /* store the env pointer in the database */
    db->set_env(env);

    proto_delete(reply);

    /*
     * on success: store the open database in the environment's list of
     * opened databases
     */
    db->set_next(env->get_databases());
    env->set_databases(db);

    /*
     * initialize the remaining function pointers in Database
     */
    return (db->initialize_remote());
}

static ham_status_t 
_remote_fun_open_db(Environment *env, Database *db, 
        ham_u16_t dbname, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    ham_size_t i=0, num_params=0;
    ham_u32_t *names;
    ham_u64_t *values;
    const ham_parameter_t *p;
    
    /* count number of parameters */
    p=param;
    if (p) {
        for (; p->name; p++) {
            num_params++;
        }
    }

    /* allocate a memory and copy the parameter names */
    names=(ham_u32_t *)env->get_allocator()->alloc(num_params*sizeof(ham_u32_t));
    values=(ham_u64_t *)env->get_allocator()->alloc(num_params*sizeof(ham_u64_t));
    if (!names || !values)
        return (HAM_OUT_OF_MEMORY);
    p=param;
    if (p) {
        for (; p->name; p++) {
            names[i]=p->name;
            values[i]=p->value;
            i++;
        }
    }

    request=proto_init_env_open_db_request(dbname, flags, 
                names, values, num_params);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);

    env->get_allocator()->free(names);
    env->get_allocator()->free(values);

    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_env_open_db_reply(reply), (""));

    st=proto_env_open_db_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    /* store the env pointer in the database */
    db->set_env(env);
    db->set_remote_handle(proto_env_open_db_reply_get_db_handle(reply));
    db->set_rt_flags(proto_env_open_db_reply_get_flags(reply));

    proto_delete(reply);

    /*
     * on success: store the open database in the environment's list of
     * opened databases
     */
    db->set_next(env->get_databases());
    env->set_databases(db);

    /*
     * initialize the remaining function pointers in Database
     */
    return (db->initialize_remote());
}

static ham_status_t
_remote_fun_env_close(Environment *env, ham_u32_t flags)
{
    (void)flags;

    if (env->get_curl()) {
        curl_easy_cleanup(env->get_curl());
        env->set_curl(0);
    }
    
    return (0);
}

static ham_status_t
_remote_fun_txn_begin(Environment *env, Transaction **txn, 
                const char *name, ham_u32_t flags)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    
    request=proto_init_txn_begin_request(name, flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_txn_begin_reply(reply), (""));

    st=proto_txn_begin_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    st=txn_begin(txn, env, name, flags);
    if (st)
        *txn=0;
    else
        txn_set_remote_handle(*txn, 
                    proto_txn_begin_reply_get_txn_handle(reply));

    proto_delete(reply);

    return (st);
}

static ham_status_t
_remote_fun_txn_commit(Environment *env, Transaction *txn, ham_u32_t flags)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    
    request=proto_init_txn_commit_request(txn_get_remote_handle(txn), flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_txn_commit_reply(reply), (""));

    st=proto_txn_commit_reply_get_status(reply);

    proto_delete(reply);

    if (st==0) {
        env_remove_txn(env, txn);
        txn_free(txn);
    }

    return (st);
}

static ham_status_t
_remote_fun_txn_abort(Environment *env, Transaction *txn, ham_u32_t flags)
{
    ham_status_t st;
    proto_wrapper_t *request, *reply;

    request=proto_init_txn_abort_request(txn_get_remote_handle(txn), flags);
    
    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_txn_abort_reply(reply), (""));

    st=proto_txn_abort_reply_get_status(reply);

    proto_delete(reply);

    if (st==0) {
        env_remove_txn(env, txn);
        txn_free(txn);
    }

    return (st);
}


#endif /* HAM_ENABLE_REMOTE */

ham_status_t
env_initialize_remote(Environment *env)
{
#if HAM_ENABLE_REMOTE
    env->_fun_create             =_remote_fun_create;
    env->_fun_open               =_remote_fun_open;
    env->_fun_rename_db          =_remote_fun_rename_db;
    env->_fun_erase_db           =_remote_fun_erase_db;
    env->_fun_get_database_names =_remote_fun_get_database_names;
    env->_fun_get_parameters     =_remote_fun_env_get_parameters;
    env->_fun_flush              =_remote_fun_env_flush;
    env->_fun_create_db          =_remote_fun_create_db;
    env->_fun_open_db            =_remote_fun_open_db;
    env->_fun_close              =_remote_fun_env_close;
    env->_fun_txn_begin          =_remote_fun_txn_begin;
    env->_fun_txn_commit         =_remote_fun_txn_commit;
    env->_fun_txn_abort          =_remote_fun_txn_abort;

    env->set_flags(env->get_flags()|DB_IS_REMOTE);
#else
    return (HAM_NOT_IMPLEMENTED);
#endif

    return (0);
}

#if HAM_ENABLE_REMOTE

ham_status_t 
DatabaseImplementationRemote::get_parameters(ham_parameter_t *param)
{
    static char filename[1024];
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;
    ham_size_t i, num_names=0;
    ham_u32_t *names;
    ham_parameter_t *p;
    
    /* count number of parameters */
    p=param;
    if (p) {
        for (; p->name; p++) {
            num_names++;
        }
    }

    /* allocate a memory and copy the parameter names */
    names=(ham_u32_t *)env->get_allocator()->alloc(num_names*sizeof(ham_u32_t));
    if (!names)
        return (HAM_OUT_OF_MEMORY);
    p=param;
    if (p) {
        for (i=0; p->name; p++) {
            names[i]=p->name;
            i++;
        }
    }

    request=proto_init_db_get_parameters_request(m_db->get_remote_handle(),
                        names, num_names);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);

    env->get_allocator()->free(names);

    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_db_get_parameters_reply(reply), (""));

    st=proto_db_get_parameters_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (st);
    }

    p=param;
    while (p && p->name) {
        switch (p->name) {
        case HAM_PARAM_CACHESIZE:
            ham_assert(proto_db_get_parameters_reply_has_cachesize(reply), (""));
            p->value=proto_db_get_parameters_reply_get_cachesize(reply);
            break;
        case HAM_PARAM_PAGESIZE:
            ham_assert(proto_db_get_parameters_reply_has_pagesize(reply), (""));
            p->value=proto_db_get_parameters_reply_get_pagesize(reply);
            break;
        case HAM_PARAM_MAX_ENV_DATABASES:
            ham_assert(proto_db_get_parameters_reply_has_max_env_databases(reply), (""));
            p->value=proto_db_get_parameters_reply_get_max_env_databases(reply);
            break;
        case HAM_PARAM_GET_FLAGS:
            ham_assert(proto_db_get_parameters_reply_has_flags(reply), (""));
            p->value=proto_db_get_parameters_reply_get_flags(reply);
            break;
        case HAM_PARAM_GET_FILEMODE:
            ham_assert(proto_db_get_parameters_reply_has_filemode(reply), (""));
            p->value=proto_db_get_parameters_reply_get_filemode(reply);
            break;
        case HAM_PARAM_GET_FILENAME:
            ham_assert(proto_db_get_parameters_reply_has_filename(reply), (""));
            strncpy(filename, proto_db_get_parameters_reply_get_filename(reply),
                        sizeof(filename));
            p->value=(ham_u64_t)(&filename[0]);
            break;
        case HAM_PARAM_KEYSIZE:
            ham_assert(proto_db_get_parameters_reply_has_keysize(reply), (""));
            p->value=proto_db_get_parameters_reply_get_keysize(reply);
            break;
        case HAM_PARAM_GET_DATABASE_NAME:
            ham_assert(proto_db_get_parameters_reply_has_dbname(reply), (""));
            p->value=proto_db_get_parameters_reply_get_dbname(reply);
            break;
        case HAM_PARAM_GET_KEYS_PER_PAGE:
            ham_assert(proto_db_get_parameters_reply_has_keys_per_page(reply), (""));
            p->value=proto_db_get_parameters_reply_get_keys_per_page(reply);
            break;
        case HAM_PARAM_GET_DATA_ACCESS_MODE:
            ham_assert(proto_db_get_parameters_reply_has_dam(reply), (""));
            p->value=proto_db_get_parameters_reply_get_dam(reply);
            break;
        default:
            ham_trace(("unknown parameter %d", (int)p->name));
            break;
        }
        p++;
    }

    proto_delete(reply);

    return (st);
}

ham_status_t 
DatabaseImplementationRemote::check_integrity(Transaction *txn)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;
    
    request=proto_init_check_integrity_request(m_db->get_remote_handle(), 
                        txn ? txn_get_remote_handle(txn) : 0);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_check_integrity_reply(reply), (""));
    st=proto_check_integrity_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}


ham_status_t 
DatabaseImplementationRemote::get_key_count(Transaction *txn, ham_u32_t flags, 
                    ham_offset_t *keycount)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;
    
    request=proto_init_db_get_key_count_request(m_db->get_remote_handle(), 
                        txn ? txn_get_remote_handle(txn) : 0, flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_db_get_key_count_reply(reply), (""));

    st=proto_db_get_key_count_reply_get_status(reply);
    if (!st)
        *keycount=proto_db_get_key_count_reply_get_key_count(reply);

    proto_delete(reply);

    return (st);
}

ham_status_t 
DatabaseImplementationRemote::insert(Transaction *txn, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;
    ham_bool_t send_key=HAM_TRUE;

    ByteArray *arena=(txn==0 || (txn_get_flags(txn)&HAM_TXN_TEMPORARY))
                        ? &m_db->get_key_arena()
                        : &txn->get_key_arena();

    /* recno: do not send the key */
    if (m_db->get_rt_flags()&HAM_RECORD_NUMBER) {
        send_key=HAM_FALSE;

        /* allocate memory for the key */
        if (!key->data) {
            arena->resize(sizeof(ham_u64_t));
            key->data=arena->get_ptr();
            key->size=sizeof(ham_u64_t);
        }
    }
    
    request=proto_init_db_insert_request(m_db->get_remote_handle(), 
                        txn ? txn_get_remote_handle(txn) : 0, 
                        send_key ? key : 0, record, flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_db_insert_reply(reply)!=0, (""));
    st=proto_db_insert_reply_get_status(reply);

    /* recno: the key was modified! */
    if (st==0 && proto_db_insert_reply_has_key(reply)) {
        if (proto_db_insert_reply_get_key_size(reply)==sizeof(ham_offset_t)) {
            ham_assert(key->data!=0, (""));
            ham_assert(key->size==sizeof(ham_offset_t), (""));
            memcpy(key->data, proto_db_insert_reply_get_key_data(reply),
                    sizeof(ham_offset_t));
        }
    }

    proto_delete(reply);

    return (st);
}

ham_status_t 
DatabaseImplementationRemote::erase(Transaction *txn, ham_key_t *key, 
                ham_u32_t flags)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;
    
    request=proto_init_db_erase_request(m_db->get_remote_handle(), 
                        txn ? txn_get_remote_handle(txn) : 0, 
                        key, flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_db_erase_reply(reply)!=0, (""));
    st=proto_db_erase_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}


ham_status_t 
DatabaseImplementationRemote::find(Transaction *txn, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;

    request=proto_init_db_find_request(m_db->get_remote_handle(), 
                        txn ? txn_get_remote_handle(txn) : 0, 
                        key, record, flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ByteArray *key_arena=(txn==0 || (txn_get_flags(txn)&HAM_TXN_TEMPORARY))
                        ? &m_db->get_key_arena()
                        : &txn->get_key_arena();
    ByteArray *rec_arena=(txn==0 || (txn_get_flags(txn)&HAM_TXN_TEMPORARY))
                        ? &m_db->get_record_arena()
                        : &txn->get_record_arena();

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_db_find_reply(reply)!=0, (""));

    st=proto_db_find_reply_get_status(reply);
    if (st==0) {
        /* approx. matching: need to copy the _flags and the key data! */
        if (proto_db_find_reply_has_key(reply)) {
            ham_assert(key, (""));
            key->_flags=proto_db_find_reply_get_key_intflags(reply);
            key->size=proto_db_find_reply_get_key_size(reply);
            if (!(key->flags&HAM_KEY_USER_ALLOC)) {
                key_arena->resize(key->size);
                key->data=key_arena->get_ptr();
            }
            memcpy(key->data, proto_db_find_reply_get_key_data(reply),
                    key->size);
        }
        if (proto_db_find_reply_has_record(reply)) {
            record->size=proto_db_find_reply_get_record_size(reply);
            if (!(record->flags&HAM_RECORD_USER_ALLOC)) {
                rec_arena->resize(record->size);
                record->data=rec_arena->get_ptr();
            }
            memcpy(record->data, proto_db_find_reply_get_record_data(reply),
                    record->size);
        }
    }

    proto_delete(reply);

    return (st);
}

Cursor *
DatabaseImplementationRemote::cursor_create(Transaction *txn, ham_u32_t flags)
{
    Environment *env=m_db->get_env();
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_create_request(m_db->get_remote_handle(), 
                        txn ? txn_get_remote_handle(txn) : 0, flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (0);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_create_reply(reply)!=0, (""));

    st=proto_cursor_create_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (0);
    }

    Cursor *c=new Cursor(m_db);

    c->set_remote_handle(proto_cursor_create_reply_get_cursor_handle(reply));

    proto_delete(reply);

    return (c);
}

Cursor *
DatabaseImplementationRemote::cursor_clone(Cursor *src)
{
    Environment *env=src->get_db()->get_env();
    ham_status_t st;
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_clone_request(src->get_remote_handle());

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (0);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_clone_reply(reply)!=0, (""));

    st=proto_cursor_clone_reply_get_status(reply);
    if (st) {
        proto_delete(reply);
        return (0);
    }

    Cursor *c=new Cursor(src->get_db());

    c->set_remote_handle(proto_cursor_clone_reply_get_cursor_handle(reply));

    proto_delete(reply);

    return (c);
}

ham_status_t 
DatabaseImplementationRemote::cursor_insert(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;
    ham_bool_t send_key=HAM_TRUE;
    Transaction *txn=cursor->get_txn();

    ByteArray *arena=(txn==0 || (txn_get_flags(txn)&HAM_TXN_TEMPORARY))
                        ? &m_db->get_key_arena()
                        : &txn->get_key_arena();

    /* recno: do not send the key */
    if (m_db->get_rt_flags()&HAM_RECORD_NUMBER) {
        send_key=HAM_FALSE;

        /* allocate memory for the key */
        if (!key->data) {
            arena->resize(sizeof(ham_u64_t));
            key->data=arena->get_ptr();
            key->size=sizeof(ham_u64_t);
        }
    }
    
    request=proto_init_cursor_insert_request(cursor->get_remote_handle(), 
                        send_key ? key : 0, record, flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_insert_reply(reply)!=0, (""));

    st=proto_cursor_insert_reply_get_status(reply);

    /* recno: the key was modified! */
    if (st==0 && proto_cursor_insert_reply_has_key(reply)) {
        if (proto_cursor_insert_reply_get_key_size(reply)
                ==sizeof(ham_offset_t)) {
            ham_assert(key->data!=0, (""));
            ham_assert(key->size==sizeof(ham_offset_t), (""));
            memcpy(key->data, proto_cursor_insert_reply_get_key_data(reply),
                    sizeof(ham_offset_t));
        }
    }

    proto_delete(reply);

    return (st);
}


ham_status_t 
DatabaseImplementationRemote::cursor_erase(Cursor *cursor, ham_u32_t flags)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_erase_request(cursor->get_remote_handle(), 
                                    flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_erase_reply(reply)!=0, (""));

    st=proto_cursor_erase_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

ham_status_t 
DatabaseImplementationRemote::cursor_find(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;

    request=proto_init_cursor_find_request(cursor->get_remote_handle(), 
                        key, record, flags);
    
    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    Transaction *txn=cursor->get_txn();

    ByteArray *arena=(txn==0 || (txn_get_flags(txn)&HAM_TXN_TEMPORARY))
                        ? &m_db->get_record_arena()
                        : &txn->get_record_arena();

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_find_reply(reply)!=0, (""));

    st=proto_cursor_find_reply_get_status(reply);
    if (st) 
        goto bail;

    /* approx. matching: need to copy the _flags! */
    if (proto_cursor_find_reply_has_key(reply)) {
        key->_flags=proto_cursor_find_reply_get_key_intflags(reply);
    }
    if (proto_cursor_find_reply_has_record(reply)) {
        ham_assert(record, (""));
        record->size=proto_cursor_find_reply_get_record_size(reply);
        if (!(record->flags&HAM_RECORD_USER_ALLOC)) {
            arena->resize(record->size);
            record->data=arena->get_ptr();
        }
        memcpy(record->data, proto_cursor_find_reply_get_record_data(reply),
                record->size);
    }

bail:
    proto_delete(reply);
    return (st);
}

ham_status_t 
DatabaseImplementationRemote::cursor_get_duplicate_count(Cursor *cursor, 
                    ham_size_t *count, ham_u32_t flags)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_get_duplicate_count_request(
                        cursor->get_remote_handle(), flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_get_duplicate_count_reply(reply)!=0, (""));

    st=proto_cursor_get_duplicate_count_reply_get_status(reply);
    if (st) 
        goto bail;

    *count=proto_cursor_get_duplicate_count_reply_get_count(reply);

bail:
    proto_delete(reply);
    return (st);
}


ham_status_t 
DatabaseImplementationRemote::cursor_get_record_size(Cursor *cursor, 
                    ham_offset_t *size)
{
    (void)cursor;
    (void)size;
    /* need this? send me a mail and i will implement it */
    return (HAM_NOT_IMPLEMENTED);
}

ham_status_t 
DatabaseImplementationRemote::cursor_overwrite(Cursor *cursor, 
                    ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_overwrite_request(
                        cursor->get_remote_handle(), record, flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_overwrite_reply(reply)!=0, (""));

    st=proto_cursor_overwrite_reply_get_status(reply);

    proto_delete(reply);

    return (st);
}

ham_status_t 
DatabaseImplementationRemote::cursor_move(Cursor *cursor, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;
    
    Transaction *txn=cursor->get_txn();
    ByteArray *key_arena=(txn==0 || (txn_get_flags(txn)&HAM_TXN_TEMPORARY))
                        ? &m_db->get_key_arena()
                        : &txn->get_key_arena();
    ByteArray *rec_arena=(txn==0 || (txn_get_flags(txn)&HAM_TXN_TEMPORARY))
                        ? &m_db->get_record_arena()
                        : &txn->get_record_arena();

    request=proto_init_cursor_move_request(cursor->get_remote_handle(), 
                        key, record, flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_move_reply(reply)!=0, (""));

    st=proto_cursor_move_reply_get_status(reply);
    if (st) 
        goto bail;

    /* modify key/record, but make sure that USER_ALLOC is respected! */
    if (proto_cursor_move_reply_has_key(reply)) {
        ham_assert(key, (""));
        key->_flags=proto_cursor_move_reply_get_key_intflags(reply);
        key->size=proto_cursor_move_reply_get_key_size(reply);
        if (!(key->flags&HAM_KEY_USER_ALLOC)) {
            key_arena->resize(key->size);
            key->data=key_arena->get_ptr();
        }
        memcpy(key->data, proto_cursor_move_reply_get_key_data(reply),
                key->size);
    }

    /* same for the record */
    if (proto_cursor_move_reply_has_record(reply)) {
        ham_assert(record, (""));
        record->size=proto_cursor_move_reply_get_record_size(reply);
        if (!(record->flags&HAM_RECORD_USER_ALLOC)) {
            rec_arena->resize(record->size);
            record->data=rec_arena->get_ptr();
        }
        memcpy(record->data, proto_cursor_move_reply_get_record_data(reply),
                record->size);
    }

bail:
    proto_delete(reply);
    return (st);
}

void 
DatabaseImplementationRemote::cursor_close(Cursor *cursor)
{
    ham_status_t st;
    Environment *env=cursor->get_db()->get_env();
    proto_wrapper_t *request, *reply;
    
    request=proto_init_cursor_close_request(cursor->get_remote_handle());

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return;
    }

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_cursor_close_reply(reply)!=0, (""));

    proto_cursor_close_reply_get_status(reply);
    proto_delete(reply);
}

ham_status_t 
DatabaseImplementationRemote::close(ham_u32_t flags)
{
    ham_status_t st;
    Environment *env=m_db->get_env();
    proto_wrapper_t *request, *reply;
    
    /*
     * auto-cleanup cursors?
     */
    if (flags&HAM_AUTO_CLEANUP) {
        Cursor *cursor=m_db->get_cursors();
        while ((cursor=m_db->get_cursors())) {
            m_db->close_cursor(cursor);
        }
    }
    else if (m_db->get_cursors()) {
        return (HAM_CURSOR_STILL_OPEN);
    }

    request=proto_init_db_close_request(m_db->get_remote_handle(), flags);

    st=_perform_request(env, env->get_curl(), request, &reply);
    proto_delete(request);
    if (st) {
        if (reply)
            proto_delete(reply);
        return (st);
    }

    /* free cached memory */
    m_db->get_key_arena().clear();
    m_db->get_record_arena().clear();

    ham_assert(reply!=0, (""));
    ham_assert(proto_has_db_close_reply(reply), (""));

    st=proto_db_close_reply_get_status(reply);

    proto_delete(reply);

    if (st==0)
        m_db->set_remote_handle(0);

    return (st);
}

#endif // HAM_ENABLE_REMOTE

