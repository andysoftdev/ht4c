/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

#include "blob.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "freelist.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "txn.h"
#include "btree.h"
#include "btree_key.h"


#define SMALLEST_CHUNK_SIZE  (sizeof(ham_offset_t)+sizeof(blob_t)+1)

/* 
 * if the blob is small enough (or if logging is enabled) then go through
 * the cache. otherwise use direct I/O
 */
static ham_bool_t
__blob_from_cache(Environment *env, ham_size_t size)
{
    if (env->get_log())
        return (size<(env->get_usable_pagesize()));  
    return (size<(ham_size_t)(env->get_pagesize()>>3));
}

/**
 * write a series of data chunks to storage at file offset 'addr'.
 * 
 * The chunks are assumed to be stored in sequential order, adjacent
 * to each other, i.e. as one long data strip.
 * 
 * Writing is performed on a per-page basis, where special conditions
 * will decide whether or not the write operation is performed
 * through the page cache or directly to device; such is determined 
 * on a per-page basis.
 */
static ham_status_t
__write_chunks(Environment *env, Page *page, ham_offset_t addr, 
        ham_bool_t allocated, ham_bool_t freshly_created, 
        ham_u8_t **chunk_data, ham_size_t *chunk_size, 
        ham_size_t chunks)
{
    ham_size_t i;
    ham_status_t st;
    ham_offset_t pageid;
    Device *device=env->get_device();
	ham_size_t pagesize = env->get_pagesize();

    ham_assert(freshly_created ? allocated : 1, (0));

    /*
     * for each chunk...
     */
    for (i=0; i<chunks; i++) {
        while (chunk_size[i]) {
            /*
             * get the page-ID from this chunk
             */
            pageid = addr - (addr % pagesize);

            /*
             * is this the current page?
             */
            if (page && page->get_self()!=pageid)
                page=0;

            /*
             * fetch the page from the cache, if it's in the cache
             * (unless we're logging - in this case always go through
             * the buffered routines)
             */
            if (!page) {
                /*
                 * keep pages in cache when they are located at the 'edges' of 
                 * the blob, as they MAY be accessed for different data.
                 * Of course, when a blob is small, there's only one (partial) 
                 * page accessed anyhow, so that one should end up in cache 
                 * then.
                 *
                 * When transaction logging is turned on, it's the same story, 
                 * really. We _could_ keep all those pages in cache now,
                 * but this would be thrashing the cache with blob data that's 
                 * accessed once only and for transaction abort (or commit)
                 * the amount of effort does not change.
                 *
                 * THOUGHT:
                 *
                 * Do we actually care what was in that page, which is going 
                 * to be overwritten in its entirety, BEFORE we do this, i.e. 
                 * before the transaction? 
                 *
                 * Answer: NO (and YES in special circumstances).
                 *
                 * Elaboration: As this would have been free space before, the 
                 * actual content does not matter, so it's not required to add
                 * the FULL pages written by the blob write action here to the 
                 * transaction log: even on transaction abort, that lingering 
                 * data is marked as 'bogus'/free as it was before anyhow.
                 *
                 * And then, assuming a longer running transaction, where this 
                 * page was freed during a previous action WITHIN
                 * the transaction, well, than the transaction log should 
                 * already carry this page's previous content as instructed 
                 * by the erase operation. HOWEVER, the erase operation would 
                 * not have a particular NEED to edit this page, as an erase op 
                 * is complete by just marking this space as free in the 
                 * freelist, resulting in the freelist pages (and the btree 
                 * pages) being the only ones being edited and ending up in 
                 * the transaction log then.
                 *
                 * Which means we'll have to log the previous content of these 
                 * pages to the transaction log anyhow. UNLESS, that is, when
                 * WE allocated these pages in the first place: then there 
                 * cannot be any 'pre-transaction' state of these pages 
                 * except that of 'not existing', i.e. 'free'. In which case, 
                 * their actual content doesn't matter! (freshly_created)
                 *
                 * And what if we have recovery logging turned on, but it's 
                 * not about an active transaction here?
                 * In that case, the recovery log would only log the OLD page 
                 * content, which we've concluded is insignificant, ever. Of 
                 * course, that's assuming (again!) that we're writing to 
                 * freshly created pages, which no-one has seen before. 
                 *
                 * Just as long as we can prevent this section from thrashing 
                 * the page cache, thank you very much...
                 */
                ham_bool_t at_blob_edge = (__blob_from_cache(env, chunk_size[i])
                        || (addr % pagesize) != 0 
                        || chunk_size[i] < pagesize);
                ham_bool_t cacheonly = (!at_blob_edge 
                                    && (!env->get_log()
                                        || freshly_created));

                st=env_fetch_page(&page, env, pageid, 
                        cacheonly ? DB_ONLY_FROM_CACHE : 
                        at_blob_edge ? 0 : 0/*DB_NEW_PAGE_DOES_THRASH_CACHE*/);
				ham_assert(st ? !page : 1, (0));
                /* blob pages don't have a page header */
                if (page) {
                    page_set_npers_flags(page, 
                        page_get_npers_flags(page)|PAGE_NPERS_NO_HEADER);
                }
                else if (st) {
                    return st;
                }
            }

            /*
             * if we have a page pointer: use it; otherwise write directly
             * to the device
             */
            if (page) {
                ham_size_t writestart=
                        (ham_size_t)(addr-page->get_self());
                ham_size_t writesize =
                        (ham_size_t)(pagesize - writestart);
                if (writesize>chunk_size[i])
                    writesize=chunk_size[i];
                memcpy(&page_get_raw_payload(page)[writestart], chunk_data[i],
                            writesize);
                page_set_dirty(page);
                addr+=writesize;
                chunk_data[i]+=writesize;
                chunk_size[i]-=writesize;
            }
            else {
                ham_size_t s = chunk_size[i];
                /* limit to the next page boundary */
                if (s > pageid+pagesize-addr)
                    s = (ham_size_t)(pageid+pagesize-addr);

                st=device->write(addr, chunk_data[i], s);
                if (st)
                    return st;
                addr+=s;
                chunk_data[i]+=s;
                chunk_size[i]-=s;
            }
        }
    }

    return (0);
}

static ham_status_t
__read_chunk(Environment *env, Page *page, Page **fpage, 
        ham_offset_t addr, Database *db, ham_u8_t *data, ham_size_t size)
{
    ham_status_t st;
    Device *device=env->get_device();

    while (size) {
        /*
         * get the page-ID from this chunk
         */
        ham_offset_t pageid;
		pageid = addr - (addr % env->get_pagesize());

        if (page) {
            if (page->get_self()!=pageid)
                page=0;
        }

        /*
         * is it the current page? if not, try to fetch the page from
         * the cache - but only read the page from disk, if the 
         * chunk is small
         */
        if (!page) {
            if (db)
                st=db_fetch_page(&page, db, pageid, 
                    __blob_from_cache(env, size) ? 0 : DB_ONLY_FROM_CACHE);
            else
                st=env_fetch_page(&page, env, pageid, 
                    __blob_from_cache(env, size) ? 0 : DB_ONLY_FROM_CACHE);
			ham_assert(st ? !page : 1, (0));
            /* blob pages don't have a page header */
            if (page)
                page_set_npers_flags(page, 
                    page_get_npers_flags(page)|PAGE_NPERS_NO_HEADER);
			else if (st)
				return st;
        }

        /*
         * if we have a page pointer: use it; otherwise read directly
         * from the device
         */
        if (page) {
            ham_size_t readstart=
                    (ham_size_t)(addr-page->get_self());
            ham_size_t readsize =
                    (ham_size_t)(env->get_pagesize()-readstart);
            if (readsize>size)
                readsize=size;
            memcpy(data, &page_get_raw_payload(page)[readstart], readsize);
            addr+=readsize;
            data+=readsize;
            size-=readsize;
        }
        else {
            ham_size_t s=(size<env->get_pagesize() 
                    ? size : env->get_pagesize());
            /* limit to the next page boundary */
            if (s>pageid+env->get_pagesize()-addr)
                s=(ham_size_t)(pageid+env->get_pagesize()-addr);

            st=device->read(addr, data, s);
            if (st) 
                return st;
            addr+=s;
            data+=s;
            size-=s;
        }
    }

    if (fpage)
        *fpage=page;

    return (0);
}

static ham_status_t
__get_duplicate_table(dupe_table_t **table_ref, Page **page, 
                    Environment *env, ham_u64_t table_id)
{
    ham_status_t st;
    blob_t hdr;
    Page *hdrpage=0;
    dupe_table_t *table;

	*page = 0;
    if (env->get_flags()&HAM_IN_MEMORY_DB) {
        ham_u8_t *p=(ham_u8_t *)U64_TO_PTR(table_id); 
        *table_ref = (dupe_table_t *)(p+sizeof(hdr));
		return HAM_SUCCESS;
    }

	*table_ref = 0;

    /*
     * load the blob header
     */
    st=__read_chunk(env, 0, &hdrpage, table_id, 0, 
                    (ham_u8_t *)&hdr, sizeof(hdr));
    if (st)
        return st;

    /*
     * if the whole table is in a page (and not split between several
     * pages), just return a pointer directly in the page
     */
    if (hdrpage->get_self()+env->get_usable_pagesize() >=
            table_id+blob_get_size(&hdr)) {
        ham_u8_t *p=page_get_raw_payload(hdrpage);
        /* yes, table is in the page */
        *page=hdrpage;
        *table_ref = (dupe_table_t *)
                &p[table_id-hdrpage->get_self()+sizeof(hdr)];
		return HAM_SUCCESS;
    }

    /*
     * otherwise allocate memory for the table
     */
    table=(dupe_table_t *)env->get_allocator()->alloc( 
                (ham_size_t)blob_get_size(&hdr));
    if (!table)
        return (HAM_OUT_OF_MEMORY);

    /*
     * then read the rest of the blob
     */
    st=__read_chunk(env, hdrpage, 0, table_id+sizeof(hdr), 0,
            (ham_u8_t *)table, (ham_size_t)blob_get_size(&hdr));
    if (st)
        return st;

    *table_ref = table;
	return HAM_SUCCESS;
}

/**
 * Allocate space in storage for and write the content references by 'data'
 * (and length 'size') to storage.
 * 
 * Conditions will apply whether the data is written through cache or direct
 * to device.
 * 
 * The content is, of course, prefixed by a BLOB header.
 * 
 * Partial writes are handled in this function.
 */
ham_status_t
blob_allocate(Environment *env, Database *db, ham_record_t *record,
        ham_u32_t flags, ham_offset_t *blobid)
{
    ham_status_t st;
    Page *page=0;
    ham_offset_t addr;
    blob_t hdr;
    ham_u8_t *chunk_data[2];
    ham_size_t alloc_size;
    ham_size_t chunk_size[2];
    Device *device=env->get_device();
    ham_bool_t freshly_created = HAM_FALSE;
   
    *blobid=0;

    /*
     * PARTIAL WRITE
     * 
     * if offset+partial_size equals the full record size, then we won't 
     * have any gaps. In this case we just write the full record and ignore
     * the partial parameters.
     */
    if (flags&HAM_PARTIAL) {
        if (record->partial_offset==0 
                && record->partial_offset+record->partial_size==record->size)
            flags&=~HAM_PARTIAL;
    }

    /*
     * in-memory-database: the blobid is actually a pointer to the memory
     * buffer, in which the blob (with the blob-header) is stored
     */
    if (env->get_flags()&HAM_IN_MEMORY_DB) {
        blob_t *hdr;
        ham_u8_t *p=(ham_u8_t *)env->get_allocator()->alloc( 
                                    record->size+sizeof(blob_t));
        if (!p) {
            return HAM_OUT_OF_MEMORY;
        }

        /* initialize the header */
        hdr=(blob_t *)p;
        memset(hdr, 0, sizeof(*hdr));
        blob_set_self(hdr, (ham_offset_t)PTR_TO_U64(p));
        blob_set_alloc_size(hdr, record->size+sizeof(blob_t));
        blob_set_size(hdr, record->size);

        /* do we have gaps? if yes, fill them with zeroes */
        if (flags&HAM_PARTIAL) {
            ham_u8_t *s=p+sizeof(blob_t);
            if (record->partial_offset)
                memset(s, 0, record->partial_offset);
            memcpy(s+record->partial_offset,
                    record->data, record->partial_size);
            if (record->partial_offset+record->partial_size<record->size)
                memset(s+record->partial_offset+record->partial_size, 0, 
                    record->size-(record->partial_offset+record->partial_size));
        }
        else {
            memcpy(p+sizeof(blob_t), record->data, record->size);
        }

        *blobid=(ham_offset_t)PTR_TO_U64(p);
        return (0);
    }

    memset(&hdr, 0, sizeof(hdr));

    /*
     * blobs are CHUNKSIZE-allocated 
     */
    alloc_size=sizeof(blob_t)+record->size;
    alloc_size += DB_CHUNKSIZE - 1;
    alloc_size -= alloc_size % DB_CHUNKSIZE;

    /* 
     * check if we have space in the freelist 
     */
    st = freel_alloc_area(&addr, env, db, alloc_size);
    if (!addr) 
    {
        if (st)
            return st;

        /*
         * if the blob is small AND if logging is disabled: load the page 
         * through the cache
         */
        if (__blob_from_cache(env, alloc_size)) {
            st = db_alloc_page(&page, db, PAGE_TYPE_BLOB, 
                        PAGE_IGNORE_FREELIST);
			ham_assert(st ? page == NULL : 1, (0));
			ham_assert(!st ? page  != NULL : 1, (0));
            if (st)
                return st;
            /* blob pages don't have a page header */
            page_set_npers_flags(page, 
                    page_get_npers_flags(page)|PAGE_NPERS_NO_HEADER);
            addr=page->get_self();
            /* move the remaining space to the freelist */
            (void)freel_mark_free(env, db, addr+alloc_size,
                    env->get_pagesize()-alloc_size, HAM_FALSE);
            blob_set_alloc_size(&hdr, alloc_size);
        }
        else {
            /*
             * otherwise use direct IO to allocate the space
             */
            ham_size_t aligned=alloc_size;
            aligned += env->get_pagesize() - 1;
            aligned -= aligned % env->get_pagesize();

            st=device->alloc(aligned, &addr);
            if (st) 
                return (st);

            /* if aligned!=size, and the remaining chunk is large enough:
             * move it to the freelist */
            {
                ham_size_t diff=aligned-alloc_size;
                if (diff > SMALLEST_CHUNK_SIZE) {
                    (void)freel_mark_free(env, db, addr+alloc_size, 
                            diff, HAM_FALSE);
                    blob_set_alloc_size(&hdr, aligned-diff);
                }
                else {
                    blob_set_alloc_size(&hdr, aligned);
                }
            }
            freshly_created = HAM_TRUE;
        }

        ham_assert(HAM_SUCCESS == freel_check_area_is_allocated(env, db,
                    addr, alloc_size), (0));
    }
    else {
		ham_assert(!st, (0));
        blob_set_alloc_size(&hdr, alloc_size);
    }

    blob_set_size(&hdr, record->size);
    blob_set_self(&hdr, addr);

    /*
     * PARTIAL WRITE
     *
     * are there gaps at the beginning? If yes, then we'll fill with zeros
     */
    if ((flags&HAM_PARTIAL) && (record->partial_offset)) {
        ham_u8_t *ptr;
        ham_size_t gapsize=record->partial_offset;

        ptr=(ham_u8_t *)env->get_allocator()->calloc( 
                                    gapsize > env->get_pagesize()
                                        ? env->get_pagesize()
                                        : gapsize);
        if (!ptr)
            return (HAM_OUT_OF_MEMORY);

        /* 
         * first: write the header
         */
        chunk_data[0]=(ham_u8_t *)&hdr;
        chunk_size[0]=sizeof(hdr);
        st=__write_chunks(env, page, addr, HAM_TRUE, freshly_created, 
                        chunk_data, chunk_size, 1);
        if (st)
            return (st);

        addr+=sizeof(hdr);

        /* now fill the gap; if the gap is bigger than a pagesize we'll
         * split the gap into smaller chunks 
         */
        while (gapsize>=env->get_pagesize()) {
            chunk_data[0]=ptr;
            chunk_size[0]=env->get_pagesize();
            st=__write_chunks(env, page, addr, HAM_TRUE, 
                    freshly_created, chunk_data, chunk_size, 1);
            if (st)
                break;
            gapsize-=env->get_pagesize();
            addr+=env->get_pagesize();
        }

        /* fill the remaining gap */
        if (gapsize) {
            chunk_data[0]=ptr;
            chunk_size[0]=gapsize;

            st=__write_chunks(env, page, addr, HAM_TRUE, freshly_created, 
                            chunk_data, chunk_size, 1);
            if (st)
                return (st);
            addr+=gapsize;
        }

        env->get_allocator()->free(ptr);

        /* now write the "real" data */
        chunk_data[0]=(ham_u8_t *)record->data;
        chunk_size[0]=record->partial_size;

        st=__write_chunks(env, page, addr, HAM_TRUE, freshly_created, 
                        chunk_data, chunk_size, 1);
        if (st)
            return (st);
        addr+=record->partial_size;
    }
    else {
        /* 
         * not writing partially: write header and data, then we're done
         */
        chunk_data[0]=(ham_u8_t *)&hdr;
        chunk_size[0]=sizeof(hdr);
        chunk_data[1]=(ham_u8_t *)record->data;
        chunk_size[1]=(flags&HAM_PARTIAL) 
                        ? record->partial_size 
                        : record->size;

        st=__write_chunks(env, page, addr, HAM_TRUE, freshly_created, 
                        chunk_data, chunk_size, 2);
        if (st)
            return (st);
        addr+=sizeof(hdr)+
            ((flags&HAM_PARTIAL) ? record->partial_size : record->size);
    }

    /*
     * store the blobid; it will be returned to the caller
     */
    *blobid=blob_get_self(&hdr);

    /*
     * PARTIAL WRITES:
     *
     * if we have gaps at the end of the blob: just append more chunks to
     * fill these gaps. Since they can be pretty large we split them into
     * smaller chunks if necessary.
     */
    if (flags&HAM_PARTIAL) {
        if (record->partial_offset+record->partial_size < record->size) {
            ham_u8_t *ptr;
            ham_size_t gapsize=record->size
                            - (record->partial_offset+record->partial_size);

            /* now fill the gap; if the gap is bigger than a pagesize we'll
             * split the gap into smaller chunks 
             *
             * we split this loop in two - the outer loop will allocate the
             * memory buffer, thus saving some allocations
             */
            while (gapsize>env->get_pagesize()) {
                ham_u8_t *ptr=(ham_u8_t *)env->get_allocator()->calloc( 
                                            env->get_pagesize());
                if (!ptr)
                    return (HAM_OUT_OF_MEMORY);
                while (gapsize>env->get_pagesize()) {
                    chunk_data[0]=ptr;
                    chunk_size[0]=env->get_pagesize();
                    st=__write_chunks(env, page, addr, HAM_TRUE, 
                            freshly_created, chunk_data, chunk_size, 1);
                    if (st)
                        break;
                    gapsize-=env->get_pagesize();
                    addr+=env->get_pagesize();
                }
                env->get_allocator()->free(ptr);
                if (st)
                    return (st);
            }
            
            /* now write the remainder, which is less than a pagesize */
            ham_assert(gapsize<env->get_pagesize(), (""));

            chunk_size[0]=gapsize;
            ptr=chunk_data[0]=(ham_u8_t *)env->get_allocator()->calloc(gapsize);
            if (!ptr)
                return (HAM_OUT_OF_MEMORY);

            st=__write_chunks(env, page, addr, HAM_TRUE, freshly_created, 
                        chunk_data, chunk_size, 1);
            env->get_allocator()->free(ptr);
            if (st)
                return (st);
        }
    }

    return (0);
}

ham_status_t
blob_read(Database *db, ham_offset_t blobid, 
        ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    Page *page;
    blob_t hdr;
    ham_size_t blobsize=0;

    /*
     * in-memory-database: the blobid is actually a pointer to the memory
     * buffer, in which the blob is stored
     */
    if (db->get_env()->get_flags()&HAM_IN_MEMORY_DB) {
        blob_t *hdr=(blob_t *)U64_TO_PTR(blobid);
        ham_u8_t *data=(ham_u8_t *)(U64_TO_PTR(blobid))+sizeof(blob_t);

        /* when the database is closing, the header is already deleted */
        if (!hdr) {
            record->size = 0;
            return (0);
        }

        blobsize = (ham_size_t)blob_get_size(hdr);

        if (flags&HAM_PARTIAL) {
            if (record->partial_offset>blobsize) {
                ham_trace(("partial offset is greater than the total "
                            "record size"));
                return (HAM_INV_PARAMETER);
            }
            if (record->partial_offset+record->partial_size>blobsize)
                blobsize=blobsize-record->partial_offset;
            else
                blobsize=record->partial_size;
        }

        if (!blobsize) {
            /* empty blob? */
            record->data = 0;
            record->size = 0;
        }
        else {
            ham_u8_t *d=data;
            if (flags&HAM_PARTIAL)
                d+=record->partial_offset;

            if ((flags&HAM_DIRECT_ACCESS) 
                    && !(record->flags&HAM_RECORD_USER_ALLOC)) {
                record->size=blobsize;
                record->data=d;
            }
            else {
                /* resize buffer, if necessary */
                if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
                    st=db->resize_record_allocdata(blobsize);
                    if (st)
                        return (st);
                    record->data = db->get_record_allocdata();
                }
                /* and copy the data */
                memcpy(record->data, d, blobsize);
                record->size = blobsize;
            }
        }

        return (0);
    }

    ham_assert(blobid%DB_CHUNKSIZE==0, ("blobid is %llu", blobid));

    /*
     * first step: read the blob header 
     */
    st=__read_chunk(db->get_env(), 0, &page, blobid, db,
                    (ham_u8_t *)&hdr, sizeof(hdr));
    if (st)
        return (st);

    ham_assert(blob_get_alloc_size(&hdr)%DB_CHUNKSIZE==0, (0));

    /*
     * sanity check
     */
    if (blob_get_self(&hdr)!=blobid)
        return (HAM_BLOB_NOT_FOUND);

    blobsize = (ham_size_t)blob_get_size(&hdr);

    if (flags&HAM_PARTIAL) {
        if (record->partial_offset>blobsize) {
            ham_trace(("partial offset+size is greater than the total "
                        "record size"));
            return (HAM_INV_PARAMETER);
        }
        if (record->partial_offset+record->partial_size>blobsize)
            blobsize=blobsize-record->partial_offset;
        else
            blobsize=record->partial_size;
    }

    /* 
     * empty blob? 
     */
    if (!blobsize) {
        record->data = 0;
        record->size = 0;
        return (0);
    }

    /*
     * second step: resize the blob buffer
     */
    if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        st=db->resize_record_allocdata(blobsize);
        if (st)
            return (st);
        record->data = db->get_record_allocdata();
    }

    /*
     * third step: read the blob data
     */
    st=__read_chunk(db->get_env(), page, 0, 
                    blobid+sizeof(blob_t)+(flags&HAM_PARTIAL 
                            ? record->partial_offset 
                            : 0),
                    db, (ham_u8_t *)record->data, blobsize);
    if (st)
        return (st);

    record->size = blobsize;

    return (0);
}

ham_status_t
blob_get_datasize(Database *db, ham_offset_t blobid, ham_offset_t *size)
{
    ham_status_t st;
    Page *page;
    blob_t hdr;

    /*
     * in-memory-database: the blobid is actually a pointer to the memory
     * buffer, in which the blob is stored
     */
    if (db->get_env()->get_flags()&HAM_IN_MEMORY_DB) {
        blob_t *hdr=(blob_t *)U64_TO_PTR(blobid);
        *size=(ham_size_t)blob_get_size(hdr);
        return (0);
    }

    ham_assert(blobid%DB_CHUNKSIZE==0, ("blobid is %llu", blobid));

    /* read the blob header */
    st=__read_chunk(db->get_env(), 0, &page, blobid, db,
                (ham_u8_t *)&hdr, sizeof(hdr));
    if (st)
        return (st);
    
    if (blob_get_self(&hdr)!=blobid)
        return (HAM_BLOB_NOT_FOUND);

    *size=blob_get_size(&hdr);
    return (0);
}

ham_status_t
blob_overwrite(Environment *env, Database *db, ham_offset_t old_blobid, 
        ham_record_t *record, ham_u32_t flags, ham_offset_t *new_blobid)
{
    ham_status_t st;
    ham_size_t alloc_size;
    blob_t old_hdr;
    blob_t new_hdr;
    Page *page;

    /*
     * PARTIAL WRITE
     * 
     * if offset+partial_size equals the full record size, then we won't 
     * have any gaps. In this case we just write the full record and ignore
     * the partial parameters.
     */
    if (flags&HAM_PARTIAL) {
        if (record->partial_offset==0 
                && record->partial_offset+record->partial_size==record->size)
            flags&=~HAM_PARTIAL;
    }

    /*
     * inmemory-databases: free the old blob, 
     * allocate a new blob (but if both sizes are equal, just overwrite
     * the data)
     */
    if (env->get_flags()&HAM_IN_MEMORY_DB) 
    {
        blob_t *nhdr, *phdr=(blob_t *)U64_TO_PTR(old_blobid);

        if (blob_get_size(phdr)==record->size) {
            ham_u8_t *p=(ham_u8_t *)phdr;
            if (flags&HAM_PARTIAL) {
                memmove(p+sizeof(blob_t)+record->partial_offset, 
                        record->data, record->partial_size);
            }
            else {
                memmove(p+sizeof(blob_t), record->data, record->size);
            }
            *new_blobid=(ham_offset_t)PTR_TO_U64(phdr);
        }
        else {
            st=blob_allocate(env, db, record, flags, new_blobid);
            if (st)
                return (st);
            nhdr=(blob_t *)U64_TO_PTR(*new_blobid);
            blob_set_flags(nhdr, blob_get_flags(phdr));

            env->get_allocator()->free(phdr);
        }

        return (HAM_SUCCESS);
    }

    ham_assert(old_blobid%DB_CHUNKSIZE==0, (0));

    /*
     * blobs are CHUNKSIZE-allocated 
     */
    alloc_size=sizeof(blob_t)+record->size;
    alloc_size += DB_CHUNKSIZE - 1;
    alloc_size -= alloc_size % DB_CHUNKSIZE;

    /*
     * first, read the blob header; if the new blob fits into the 
     * old blob, we overwrite the old blob (and add the remaining
     * space to the freelist, if there is any)
     */
    st=__read_chunk(env, 0, &page, old_blobid, db,
                    (ham_u8_t *)&old_hdr, sizeof(old_hdr));
    if (st)
        return (st);

    ham_assert(blob_get_alloc_size(&old_hdr)%DB_CHUNKSIZE==0, (0));

    /*
     * sanity check
     */
    ham_assert(blob_get_self(&old_hdr)==old_blobid, 
            ("invalid blobid %llu != %llu", blob_get_self(&old_hdr), 
            old_blobid));
    if (blob_get_self(&old_hdr)!=old_blobid)
        return (HAM_BLOB_NOT_FOUND);

    /*
     * now compare the sizes; does the new data fit in the old allocated
     * space?
     */
    if (alloc_size<=blob_get_alloc_size(&old_hdr)) 
    {
        ham_u8_t *chunk_data[2];
        ham_size_t chunk_size[2];

        /* 
         * setup the new blob header
         */
        blob_set_self(&new_hdr, blob_get_self(&old_hdr));
        blob_set_size(&new_hdr, record->size);
        blob_set_flags(&new_hdr, blob_get_flags(&old_hdr));
        if (blob_get_alloc_size(&old_hdr)-alloc_size>SMALLEST_CHUNK_SIZE)
            blob_set_alloc_size(&new_hdr, alloc_size);
        else
            blob_set_alloc_size(&new_hdr, blob_get_alloc_size(&old_hdr));

        /*
         * PARTIAL WRITE
         *
         * if we have a gap at the beginning, then we have to write the
         * blob header and the blob data in two steps; otherwise we can
         * write both immediately
         */
        if ((flags&HAM_PARTIAL) && (record->partial_offset)) {
            chunk_data[0]=(ham_u8_t *)&new_hdr;
            chunk_size[0]=sizeof(new_hdr);
            st=__write_chunks(env, page, blob_get_self(&new_hdr), HAM_FALSE,
                    HAM_FALSE, chunk_data, chunk_size, 1);
            if (st)
                return (st);

            chunk_data[0]=(ham_u8_t *)record->data;
            chunk_size[0]=record->partial_size;
            st=__write_chunks(env, page, 
                    blob_get_self(&new_hdr)+sizeof(new_hdr)
                            +record->partial_offset, 
                    HAM_FALSE, HAM_FALSE, chunk_data, chunk_size, 1);
            if (st)
                return (st);
        }
        else {
            chunk_data[0]=(ham_u8_t *)&new_hdr;
            chunk_size[0]=sizeof(new_hdr);
            chunk_data[1]=(ham_u8_t *)record->data;
            chunk_size[1]=(flags&HAM_PARTIAL) 
                                ? record->partial_size 
                                : record->size;

            st=__write_chunks(env, page, blob_get_self(&new_hdr), HAM_FALSE,
                    HAM_FALSE, chunk_data, chunk_size, 2);
            if (st)
                return (st);
        }

        /*
         * move remaining data to the freelist
         */
        if (blob_get_alloc_size(&old_hdr)!=blob_get_alloc_size(&new_hdr)) {
            (void)freel_mark_free(env, db,
                  blob_get_self(&new_hdr)+blob_get_alloc_size(&new_hdr), 
                  (ham_size_t)(blob_get_alloc_size(&old_hdr)-
                  blob_get_alloc_size(&new_hdr)), HAM_FALSE);
        }

        /*
         * the old rid is the new rid
         */
        *new_blobid=blob_get_self(&new_hdr);

        return (HAM_SUCCESS);
    }
    else {
        /* 
         * when the new data is larger, allocate a fresh space for it 
         * and discard the old;
         'overwrite' has become (delete + insert) now.
         */
        st=blob_allocate(env, db, record, flags, new_blobid);
        if (st)
            return (st);

        (void)freel_mark_free(env, db, old_blobid, 
                (ham_size_t)blob_get_alloc_size(&old_hdr), HAM_FALSE);
    }

    return (HAM_SUCCESS);
}

ham_status_t
blob_free(Environment *env, Database *db, ham_offset_t blobid, ham_u32_t flags)
{
    ham_status_t st;
    blob_t hdr;

    /*
     * in-memory-database: the blobid is actually a pointer to the memory
     * buffer, in which the blob is stored
     */
    if (env->get_flags()&HAM_IN_MEMORY_DB) {
        env->get_allocator()->free((void *)U64_TO_PTR(blobid));
        return (0);
    }

    ham_assert(blobid%DB_CHUNKSIZE==0, (0));

    /*
     * fetch the blob header 
     */
    st=__read_chunk(env, 0, 0, blobid, db, (ham_u8_t *)&hdr, sizeof(hdr));
    if (st)
        return (st);

    ham_assert(blob_get_alloc_size(&hdr)%DB_CHUNKSIZE==0, (0));

    /*
     * sanity check
     */
    ham_verify(blob_get_self(&hdr)==blobid, 
            ("invalid blobid %llu != %llu", blob_get_self(&hdr), blobid));
    if (blob_get_self(&hdr)!=blobid)
        return (HAM_BLOB_NOT_FOUND);

    /*
     * move the blob to the freelist
     */
    st = freel_mark_free(env, db, blobid, 
            (ham_size_t)blob_get_alloc_size(&hdr), HAM_FALSE);
	ham_assert(!st, ("unexpected error, at least not covered in the old code"));

    return st;
}

static ham_size_t
__get_sorted_position(Database *db, dupe_table_t *table, ham_record_t *record,
                ham_u32_t flags)
{
    ham_duplicate_compare_func_t foo = db->get_duplicate_compare_func();
    ham_size_t l, r, m;
    int cmp;
    dupe_entry_t *e;
    ham_record_t item_record;
    ham_u16_t dam;
    ham_status_t st=0;

    /*
     * Use a slightly adapted form of binary search: as we already have our 
     * initial position (as was stored in the cursor), we take that as our
     * first 'median' value and go from there.
     */
    l = 0;
    r = dupe_table_get_count(table) - 1; /* get_count() is 1 too many! */

    /*
     * Maybe Wrong Idea: sequential access/insert doesn't mean the RECORD 
     * values are sequential too! They MAY be, but don't have to!
     *
     * For now, we assume they are also sequential when you're storing records
     * in duplicate-key tables (probably a secondary index table for another
     * table, this one).
     */
    dam = db->get_data_access_mode();
    if (dam & HAM_DAM_SEQUENTIAL_INSERT) {
        /* assume the insertion point sits at the end of the dupe table */
        m = r;
    }
    else {
        m = (l + r) / 2;
    }
    ham_assert(m <= r, (0));
        
    while (l <= r) {
        ham_assert(m<dupe_table_get_count(table), (""));

        e = dupe_table_get_entry(table, m);

        memset(&item_record, 0, sizeof(item_record));
        item_record._rid=dupe_entry_get_rid(e);
        item_record._intflags = dupe_entry_get_flags(e)&(KEY_BLOB_SIZE_SMALL
                                                         |KEY_BLOB_SIZE_TINY
                                                         |KEY_BLOB_SIZE_EMPTY);
        st=btree_read_record(db, &item_record, 
                    (ham_u64_t *)&dupe_entry_get_ridptr(e), flags);
        if (st)
            return (st);

        cmp = foo((ham_db_t *)db, (ham_u8_t *)record->data, record->size, 
                        (ham_u8_t *)item_record.data, item_record.size);
        /* item is lower than the left-most item of our range */
        if (m == l) {
            if (cmp < 0)
                break;
        }
        if (l == r) {
            if (cmp >= 0) {
                /* write GEQ record value in NEXT slot */
                m++;
            }
            else /* if (cmp < 0) */ {
                ham_assert(m == r, (0));
            }
            break;
        }
        else if (cmp == 0) {
            /* write equal record value in NEXT slot */
            m++;
            break;
        }
        else if (cmp < 0) {
            if (m == 0) /* new item will be smallest item in the list */
                break;
            r = m - 1;
        }
        else {
            /* write GE record value in NEXT slot, when we have nothing 
             * left to search */
            m++;
            l = m;
        }
        m = (l + r) / 2;
    }

    /* now 'm' points at the insertion point in the table */
    return (m);
}

ham_status_t
blob_duplicate_insert(Database *db, ham_offset_t table_id, 
        ham_record_t *record, ham_size_t position, ham_u32_t flags, 
        dupe_entry_t *entries, ham_size_t num_entries, 
        ham_offset_t *rid, ham_size_t *new_position)
{
    ham_status_t st=0;
    dupe_table_t *table=0;
    ham_bool_t alloc_table=0;
	ham_bool_t resize=0;
    Page *page=0;
    Environment *env=db->get_env();

    /*
     * create a new duplicate table if none existed, and insert
     * the first entry
     */
    if (!table_id) {
        ham_assert(num_entries==2, (""));
        /* allocates space for 8 (!) entries */
        table=(dupe_table_t *)env->get_allocator()->calloc( 
                        sizeof(dupe_table_t)+7*sizeof(dupe_entry_t));
        if (!table)
            return HAM_OUT_OF_MEMORY;
        dupe_table_set_capacity(table, 8);
        dupe_table_set_count(table, 1);
        memcpy(dupe_table_get_entry(table, 0), &entries[0], 
                        sizeof(entries[0]));

        /* skip the first entry */
        entries++;
        num_entries--;
        alloc_table=1;
    }
    else {
        /*
         * otherwise load the existing table 
         */
        st=__get_duplicate_table(&table, &page, env, table_id);
		ham_assert(st ? table == NULL : 1, (0));
		ham_assert(st ? page == NULL : 1, (0));
        if (!table)
			return st ? st : HAM_INTERNAL_ERROR;
        if (!page && !(env->get_flags()&HAM_IN_MEMORY_DB))
            alloc_table=1;
    }

    ham_assert(num_entries==1, (""));

    /*
     * resize the table, if necessary
     */ 
    if (!(flags & HAM_OVERWRITE)
            && dupe_table_get_count(table)+1>=dupe_table_get_capacity(table)) {
        dupe_table_t *old=table;
        ham_size_t new_cap=dupe_table_get_capacity(table);

        if (new_cap < 3*8)
            new_cap += 8;
        else
            new_cap += new_cap/3;

        table=(dupe_table_t *)env->get_allocator()->calloc( 
                    sizeof(dupe_table_t)+(new_cap-1)*sizeof(dupe_entry_t));
        if (!table)
            return (HAM_OUT_OF_MEMORY);
        dupe_table_set_capacity(table, new_cap);
        dupe_table_set_count(table, dupe_table_get_count(old));
        memcpy(dupe_table_get_entry(table, 0), dupe_table_get_entry(old, 0),
                       dupe_table_get_count(old)*sizeof(dupe_entry_t));
        if (alloc_table)
            env->get_allocator()->free(old);

        alloc_table=1;
        resize=1;
    }

    /*
     * insert sorted, unsorted or overwrite the entry at the requested position
     */
    if (flags&HAM_OVERWRITE) {
        dupe_entry_t *e=dupe_table_get_entry(table, position);

        if (!(dupe_entry_get_flags(e)&(KEY_BLOB_SIZE_SMALL
                                    |KEY_BLOB_SIZE_TINY
                                    |KEY_BLOB_SIZE_EMPTY))) {
            (void)blob_free(env, db, dupe_entry_get_rid(e), 0);
        }

        memcpy(dupe_table_get_entry(table, position), 
                        &entries[0], sizeof(entries[0]));
    }
    else {
        if (db->get_rt_flags()&HAM_SORT_DUPLICATES) {
            position=__get_sorted_position(db, table, record, flags);
        }
        else if (flags&HAM_DUPLICATE_INSERT_BEFORE) {
            /* do nothing, insert at the current position */
        }
        else if (flags&HAM_DUPLICATE_INSERT_AFTER) {
            position++;
            if (position > dupe_table_get_count(table))
                position=dupe_table_get_count(table);
        }
        else if (flags&HAM_DUPLICATE_INSERT_FIRST) {
            position=0;
        }
        else if (flags&HAM_DUPLICATE_INSERT_LAST) {
            position=dupe_table_get_count(table);
        }
        else {
            position=dupe_table_get_count(table);
        }

        if (position != dupe_table_get_count(table)) {
            memmove(dupe_table_get_entry(table, position+1), 
                dupe_table_get_entry(table, position), 
                sizeof(entries[0])*(dupe_table_get_count(table)-position));
        }

        memcpy(dupe_table_get_entry(table, position), 
                &entries[0], sizeof(entries[0]));

        dupe_table_set_count(table, dupe_table_get_count(table)+1);
    }

    /*
     * write the table back to disk and return the blobid of the table
     */
    if ((table_id && !page) || resize) {
        ham_record_t rec={0};
        rec.data=(ham_u8_t *)table;
        rec.size=sizeof(dupe_table_t)
                    +(dupe_table_get_capacity(table)-1)*sizeof(dupe_entry_t);
        st=blob_overwrite(env, db, table_id, &rec, 0, rid);
    }
    else if (!table_id) {
        ham_record_t rec={0};
        rec.data=(ham_u8_t *)table;
        rec.size=sizeof(dupe_table_t)
                    +(dupe_table_get_capacity(table)-1)*sizeof(dupe_entry_t);
        st=blob_allocate(env, db, &rec, 0, rid);
    }
    else if (table_id && page) {
        page_set_dirty(page);
    }
    else {
        ham_assert(!"shouldn't be here", (0));
	}

    if (alloc_table)
        env->get_allocator()->free(table);

    if (new_position)
        *new_position=position;

    return (st);
}

ham_status_t
blob_duplicate_erase(Database *db, ham_offset_t table_id,
        ham_size_t position, ham_u32_t flags, ham_offset_t *new_table_id)
{
    ham_status_t st;
    ham_record_t rec;
    ham_size_t i;
    dupe_table_t *table;
    ham_offset_t rid;
	Environment *env = db->get_env();

    /* store the public record pointer, otherwise it's destroyed */
    ham_size_t rs=db->get_record_allocsize();
    void      *rp=db->get_record_allocdata();
    db->set_record_allocdata(0);
    db->set_record_allocsize(0);

    memset(&rec, 0, sizeof(rec));

    if (new_table_id)
        *new_table_id=table_id;

    st=blob_read(db, table_id, &rec, 0);
    if (st)
        return (st);

    /* restore the public record pointer */
    db->set_record_allocsize(rs);
    db->set_record_allocdata(rp);

    table=(dupe_table_t *)rec.data;

    /*
     * if HAM_ERASE_ALL_DUPLICATES is set *OR* if the last duplicate is deleted:
     * free the whole duplicate table
     */
    if (flags&HAM_ERASE_ALL_DUPLICATES
            || (position==0 && dupe_table_get_count(table)==1)) {
        for (i=0; i<dupe_table_get_count(table); i++) {
            dupe_entry_t *e=dupe_table_get_entry(table, i);
            if (!(dupe_entry_get_flags(e)&(KEY_BLOB_SIZE_SMALL
                                        |KEY_BLOB_SIZE_TINY
                                        |KEY_BLOB_SIZE_EMPTY))) {
                st=blob_free(env, db, dupe_entry_get_rid(e), 0);
                if (st) {
                    env->get_allocator()->free(table);
                    return (st);
                }
            }
        }
        st=blob_free(env, db, table_id, 0); /* [i_a] isn't this superfluous (& 
                                        * dangerous), thanks to the 
                                        * free_all_dupes loop above??? */
        env->get_allocator()->free(table);
        if (st)
            return (st);

        if (new_table_id)
            *new_table_id=0;

        return (0);
    }
    else {
        ham_record_t rec={0};
        dupe_entry_t *e=dupe_table_get_entry(table, position);
        if (!(dupe_entry_get_flags(e)&(KEY_BLOB_SIZE_SMALL
                                    |KEY_BLOB_SIZE_TINY
                                    |KEY_BLOB_SIZE_EMPTY))) {
            st=blob_free(env, db, dupe_entry_get_rid(e), 0);
            if (st) {
                env->get_allocator()->free(table);
                return (st);
            }
        }
        memmove(e, e+1,
            ((dupe_table_get_count(table)-position)-1)*sizeof(dupe_entry_t));
        dupe_table_set_count(table, dupe_table_get_count(table)-1);

        rec.data=(ham_u8_t *)table;
        rec.size=sizeof(dupe_table_t)
                    +(dupe_table_get_capacity(table)-1)*sizeof(dupe_entry_t);
        st=blob_overwrite(env, db, table_id, &rec, 0, &rid);
        if (st) {
            env->get_allocator()->free(table);
            return (st);
        }
        if (new_table_id)
            *new_table_id=rid;
    }

    /*
     * return 0 as a rid if the table is empty
     */
    if (dupe_table_get_count(table)==0)
        if (new_table_id)
            *new_table_id=0;

    env->get_allocator()->free(table);
    return (0);
}

ham_status_t
blob_duplicate_get_count(Environment *env, ham_offset_t table_id,
        ham_size_t *count, dupe_entry_t *entry)
{
	ham_status_t st;
    dupe_table_t *table;
    Page *page=0;

    st=__get_duplicate_table(&table, &page, env, table_id);
	ham_assert(st ? table == NULL : 1, (0));
	ham_assert(st ? page == NULL : 1, (0));
    if (!table)
		return st ? st : HAM_INTERNAL_ERROR;

    *count=dupe_table_get_count(table);
    if (entry)
        memcpy(entry, dupe_table_get_entry(table, (*count)-1), sizeof(*entry));

    if (!(env->get_flags()&HAM_IN_MEMORY_DB)) {
        if (!page)
            env->get_allocator()->free(table);
	}
    return (0);
}

ham_status_t 
blob_duplicate_get(Environment *env, ham_offset_t table_id,
        ham_size_t position, dupe_entry_t *entry)
{
	ham_status_t st;
    dupe_table_t *table;
    Page *page=0;

    st = __get_duplicate_table(&table, &page, env, table_id);
	ham_assert(st ? table == NULL : 1, (0));
	ham_assert(st ? page == NULL : 1, (0));
    if (!table)
		return st ? st : HAM_INTERNAL_ERROR;

    if (position>=dupe_table_get_count(table)) 
	{
        if (!(env->get_flags()&HAM_IN_MEMORY_DB))
            if (!page)
                env->get_allocator()->free(table);
        return HAM_KEY_NOT_FOUND;
    }
    memcpy(entry, dupe_table_get_entry(table, position), sizeof(*entry));

    if (!(env->get_flags()&HAM_IN_MEMORY_DB)) {
        if (!page)
            env->get_allocator()->free(table);
	}
    return (0);
}

ham_status_t 
blob_duplicate_get_table(Environment *env, ham_offset_t table_id, 
                    dupe_table_t **ptable, ham_bool_t *needs_free)
{
    ham_status_t st;
    Page *page=0;

    st=__get_duplicate_table(ptable, &page, env, table_id);
    if (st)
        return (st);

    if (!(env->get_flags()&HAM_IN_MEMORY_DB))
        if (!page)
            *needs_free=1;

    return (0);
}
