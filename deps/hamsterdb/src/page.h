/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief an object which handles a database page 
 *
 */

#ifndef HAM_PAGE_H__
#define HAM_PAGE_H__

#include <string.h>

#include "internal_fwd_decl.h"

#include "endianswap.h"
#include "error.h"


#include "packstart.h"

/*
 * This header is only available if the (non-persistent) flag
 * NPERS_NO_HEADER is not set! 
 *
 * all blob-areas in the file do not have such a header, if they
 * span page-boundaries
 *
 * !!
 * if this structure is changed, db_get_usable_pagesize has 
 * to be changed as well!
 */
HAM_PACK_0 struct HAM_PACK_1 page_header_t {
    /**
     * flags of this page - currently only used for the page type
     * @sa page_type_codes
     */
    ham_u32_t _flags;

    /** some reserved bytes */
    ham_u32_t _reserved1;
    ham_u32_t _reserved2;

    /**
     * this is just a blob - the backend (hashdb, btree etc) 
     * will use it appropriately
     */
    ham_u8_t _payload[1];

} HAM_PACK_2;

/**
 * The page header which is persisted on disc
 *
 * This structure definition is present outside of @ref Page scope 
 * to allow compile-time OFFSETOF macros to correctly judge the size, depending 
 * on platform and compiler settings.
 */
typedef HAM_PACK_0 union HAM_PACK_1 page_data_t
{
    /** the persistent header */
    struct page_header_t _s;

    /** a char pointer to the allocated storage on disk */
    ham_u8_t _p[1];

} HAM_PACK_2 page_data_t;

#include "packstop.h"

/**
 * get the size of the persistent header of a page
 *
 * equals the size of struct page_data_t, without the payload byte
 *
 * @note
 * this is not equal to sizeof(struct page_data_t)-1, because of
 * padding (i.e. on gcc 4.1/64bit the size would be 15 bytes)
 */
#define page_get_persistent_header_size()   (OFFSETOF(page_data_t, _s._payload))


/**
 * The Page class
 *
 * Each Page instance is a node in several linked lists.
 * In order to avoid multiple memory allocations, the previous/next pointers 
 * are part of the Page class (m_prev and m_next).
 * Both fields are arrays of pointers and can be used i.e.
 * with m_prev[Page::LIST_BUCKET] etc. (or with the methods 
 * defined below).
 */
class Page {
  public:   
    enum {
        /** a bucket in the hash table of the cache manager */
        LIST_BUCKET     = 0,
        /** list of all cached pages */
        LIST_CACHED     = 1,
        /** list of all pages in a changeset */
        LIST_CHANGESET  = 2,
        /** array limit */
        MAX_LISTS       = 3
    };

    Page() 
    : m_alloc(0), m_owner(0), m_device(0), m_flags(0), 
      m_dirty(false), m_cursors(0), m_pers(0), m_self(0) {
#if defined(HAM_OS_WIN32) || defined(HAM_OS_WIN64)
        m_win32mmap=0;
#endif
        memset(&m_prev[0], 0, sizeof(m_prev));
        memset(&m_next[0], 0, sizeof(m_next));
    }

    /** is this the header page? */
    bool is_header() {
        return (m_self==0);
    }

    /** get the address of this page */
    ham_offset_t get_self() {
        return (m_self);
    }

    /** set the address of this page */
    void set_self(ham_offset_t address) {
        m_self=address;
    }

    /** the memory allocator */
    Allocator *m_alloc;

    /** reference to the database object; can be NULL */
    Database *m_owner;

    /** the device of this page */
    Device *m_device;

    /** non-persistent flags */
    ham_u32_t m_flags;

    /** is this page dirty and needs to be flushed to disk? */
    bool m_dirty;

#if defined(HAM_OS_WIN32) || defined(HAM_OS_WIN64)
    /** handle for win32 mmap */
    HANDLE m_win32mmap;
#endif

    /** linked list of all cursors which point to that page */
    Cursor *m_cursors;

    /** linked lists of pages - see comments above */
    Page *m_prev[Page::MAX_LISTS]; 
    Page *m_next[Page::MAX_LISTS];

    /** from here on everything will be written to disk */
    page_data_t *m_pers;

  private:
    /** address of this page */
    ham_offset_t m_self;
};

/** * the database object which 0wnz this page */
#define page_get_owner(page)         ((page)->m_owner)

/** set the database object which 0wnz this page */
#define page_set_owner(page, db)     (page)->m_owner=(db)

/** get the previous page of a linked list */
#ifdef HAM_DEBUG
extern Page *
page_get_previous(Page *page, int which);
#else
#   define page_get_previous(page, which)    ((page)->m_prev[(which)])
#endif /* HAM_DEBUG */

/** set the previous page of a linked list */
#ifdef HAM_DEBUG
extern void
page_set_previous(Page *page, int which, Page *other);
#else
#   define page_set_previous(page, which, p) (page)->m_prev[(which)]=(p)
#endif /* HAM_DEBUG */

/** get the next page of a linked list */
#ifdef HAM_DEBUG
extern Page *
page_get_next(Page *page, int which);
#else
#   define page_get_next(page, which)        ((page)->m_next[(which)])
#endif /* HAM_DEBUG */

/** set the next page of a linked list */
#ifdef HAM_DEBUG
extern void
page_set_next(Page *page, int which, Page *other);
#else
#   define page_set_next(page, which, p)     (page)->m_next[(which)]=(p)
#endif /* HAM_DEBUG */

/** get memory allocator */
#define page_get_allocator(page)             (page)->m_alloc

/** set memory allocator */
#define page_set_allocator(page, a)          (page)->m_alloc=a

/** get the device of this page */
#define page_get_device(page)                (page)->m_device

/** set the device of this page */
#define page_set_device(page, d)             (page)->m_device=d

/** get linked list of cursors */
#define page_get_cursors(page)           (page)->m_cursors

/** set linked list of cursors */
#define page_set_cursors(page, c)        (page)->m_cursors=(c)

/** get persistent page flags */
#define page_get_pers_flags(page)        (ham_db2h32((page)->m_pers->_s._flags))

/** set persistent page flags */
#define page_set_pers_flags(page, f)     (page)->m_pers->_s._flags=ham_h2db32(f)

/** get non-persistent page flags */
#define page_get_npers_flags(page)       (page)->m_flags

/** set non-persistent page flags */
#define page_set_npers_flags(page, f)    (page)->m_flags=(f)

/** page->m_pers was allocated with malloc, not mmap */
#define PAGE_NPERS_MALLOC               1

/** page will be deleted when committed */
#define PAGE_NPERS_DELETE_PENDING       2

/** page has no header */
#define PAGE_NPERS_NO_HEADER            4

/** is this page dirty? */
#define page_is_dirty(page)             ((page)->m_dirty)

/** mark this page dirty */
#define page_set_dirty(page)            (page)->m_dirty=true

/** page is no longer dirty */
#define page_set_undirty(page)          (page)->m_dirty=false

#if defined(HAM_OS_WIN32) || defined(HAM_OS_WIN64)
/** win32: get a pointer to the mmap handle */
#   define page_get_mmap_handle_ptr(p)  &((p)->m_win32mmap)
#else
#   define page_get_mmap_handle_ptr(p)  0
#endif

/** set the page-type */
#define page_set_type(page, t)          page_set_pers_flags(page, t)

/** get the page-type */
#define page_get_type(page)             (page_get_pers_flags(page))

/**
 * @defgroup page_type_codes valid page types
 * @{
 *
 * Each database page is tagged with a type code; these are all 
 * known/supported page type codes.
 * 
 * @note When ELBLOBs (Extremely Large BLOBs) are stored in the database, 
 * that is BLOBs which span multiple pages apiece, only their initial page 
 * will have a valid type code; subsequent pages of the ELBLOB will store 
 * the data as-is, so as to provide one continuous storage space per ELBLOB.
 * 
 * @sa page_data_t::page_header_t::_flags
 */

/** unidentified db page type */
#define PAGE_TYPE_UNKNOWN               0x00000000     

/** the db header page: this is the first page in the database/environment */
#define PAGE_TYPE_HEADER                0x10000000     

/** the db B+tree root page */
#define PAGE_TYPE_B_ROOT                0x20000000     

/** a B+tree node page, i.e. a page which is part of the database index */
#define PAGE_TYPE_B_INDEX               0x30000000     

/** a freelist management page */
#define PAGE_TYPE_FREELIST              0x40000000     

/** a page which stores (the front part of) a BLOB. */
#define PAGE_TYPE_BLOB                  0x50000000     

/**
 * @}
 */

/** get pointer to persistent payload (after the header!) */
#define page_get_payload(page)          (page)->m_pers->_s._payload

/** get pointer to persistent payload (including the header!) */
#define page_get_raw_payload(page)      (page)->m_pers->_p

/** set pointer to persistent data */
#define page_set_pers(page, p)          (page)->m_pers=(p)

/** get pointer to persistent data */
#define page_get_pers(page)             (page)->m_pers

#ifdef HAM_DEBUG
/**
 * check if a page is in a linked list
 */
extern ham_bool_t 
page_is_in_list(Page *head, Page *page, int which);
#else
#define page_is_in_list(head, page, which)                                     \
     (page_get_next(page, which)                                               \
         ? HAM_TRUE                                                            \
         : (page_get_previous(page, which))                                    \
             ? HAM_TRUE                                                        \
             : (head==page)                                                    \
                 ? HAM_TRUE                                                    \
                 : HAM_FALSE)
#endif

/**
 * linked list functions: insert the page at the beginning of a list
 *
 * @remark returns the new head of the list
 */
inline Page *
page_list_insert(Page *head, int which, Page *page)
{
    page_set_next(page, which, 0);
    page_set_previous(page, which, 0);

    if (!head)
        return (page);

    page_set_next(page, which, head);
    page_set_previous(head, which, page);
    return (page);
}

/**
 * linked list functions: remove the page from a list
 *
 * @remark returns the new head of the list
 */
inline Page *
page_list_remove(Page *head, int which, Page *page)
{
    Page *n, *p;

    if (page==head) {
        n=page_get_next(page, which);
        if (n)
            page_set_previous(n, which, 0);
        page_set_next(page, which, 0);
        page_set_previous(page, which, 0);
        return (n);
    }

    n=page_get_next(page, which);
    p=page_get_previous(page, which);
    if (p)
        page_set_next(p, which, n);
    if (n)
        page_set_previous(n, which, p);
    page_set_next(page, which, 0);
    page_set_previous(page, which, 0);
    return (head);
}

/**
 * add a cursor to this page
 */
extern void
page_add_cursor(Page *page, Cursor *cursor);

/**
 * remove a cursor from this page
 */
extern void
page_remove_cursor(Page *page, Cursor *cursor);

/**
 * create a new page structure
 *
 * @return a pointer to a new @ref Page instance.
 *
 * @return NULL if out of memory
 */
extern Page *
page_new(Environment *env);

/**
 * delete a page structure
 */
extern void
page_delete(Page *page);

/**
 * allocate a new page from the device
 */
extern ham_status_t
page_alloc(Page *page);

/**
 * fetch a page from the device
 */
extern ham_status_t
page_fetch(Page *page);

/**
 * write a page to the device
 */
extern ham_status_t
page_flush(Page *page);

/**
 * free a page
 */
extern ham_status_t
page_free(Page *page);

/**
 * uncouple all cursors from a page
 *
 * @remark this is called whenever the page is deleted or becoming invalid
 */
extern ham_status_t
page_uncouple_all_cursors(Page *page, ham_size_t start);


#endif /* HAM_PAGE_H__ */