/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief btree find/insert/erase statistical structures, functions and macros
 */

#ifndef HAM_BTREE_STATS_H__
#define HAM_BTREE_STATS_H__

#include "internal_fwd_decl.h"

namespace hamsterdb {

class BtreeStatistics {
  public:
    // Indices into find/insert/erase specific statistics
    enum {
      kOperationFind      = 0,
      kOperationInsert    = 1,
      kOperationErase     = 2,
      kOperationMax       = 3
    };

    struct FindHints {
      /* the original flags of ham_find */
      ham_u32_t original_flags;

      /* the modified flags */
      ham_u32_t flags;

      /* page/btree leaf to check first */
      ham_u64_t leaf_page_addr;

      /* check specified btree leaf node page first */
      bool try_fast_track;
    };

    struct InsertHints {
      /* the original flags of ham_insert */
      ham_u32_t original_flags;

      /* the modified flags */
      ham_u32_t flags;

      /* page/btree leaf to check first */
      ham_u64_t leaf_page_addr;

      /* the processed leaf page */
      Page *processed_leaf_page;

      /* the slot in that page */
      ham_u16_t processed_slot;

      /* count the number of appends */
      ham_size_t append_count;

      /* count the number of prepends */
      ham_size_t prepend_count;
    };

    /** constructor */
    BtreeStatistics(Database *db);

    /** retrieve database hints for ham_find */
    FindHints get_find_hints(ham_u32_t flags);

    /** retrieve database hints for insert */
    InsertHints get_insert_hints(ham_u32_t flags);

    /** A ham_find/ham_cusor_find succeeded */
    void find_succeeded(Page *page);

    /** A ham_find/ham_cursor_find failed */
    void find_failed();

    /** A ham_insert/ham_cursor_insert succeeded */
    void insert_succeeded(Page *page, ham_u16_t slot);

    /** A ham_insert/ham_cursor_insert failed */
    void insert_failed();

    /** A ham_erase/ham_cusor_erase succeeded */
    void erase_succeeded(Page *page);

    /** A ham_erase/ham_cursor_erase failed */
    void erase_failed();

    /** reset page statistics */
    void reset_page(Page *page);

  private:
    /** the Database */
    Database *m_db;

    /* last leaf page for find/insert/erase */
    ham_u64_t m_last_leaf_pages[kOperationMax];

    /* count of how often this leaf page was used */
    ham_size_t m_last_leaf_count[kOperationMax];

    /* count the number of appends */
    ham_size_t m_append_count;

    /* count the number of prepends */
    ham_size_t m_prepend_count;
};


} // namespace hamsterdb

#endif /* HAM_BTREE_STATS_H__ */
