/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/*
 * The Cache Manager
 *
 * Stores pages in a non-intrusive hash table (each Page instance keeps
 * next/previous pointers for the overflow bucket). Can efficiently purge
 * unused pages, because all pages are also stored in a (non-intrusive)
 * linked list, and whenever a page is accessed it is removed and re-inserted
 * at the head. The tail therefore points to the page which was not used
 * in a long time, and is the primary candidate for purging.
 */

#ifndef HAM_CACHE_H__
#define HAM_CACHE_H__

#include <vector>

#include "config.h"
#include "env_local.h"

namespace hamsterdb {

class LocalEnvironment;
class PageManager;

/*
 * The Cache Manager
 */
class Cache
{
    enum {
      // The number of buckets should be a prime number or similar, as it
      // is used in a MODULO hash scheme
      kBucketSize = 10317
    };

  public:
    // the default constructor
    // |capacity_size| is in bytes!
    Cache(LocalEnvironment *env,
            ham_u64_t capacity_bytes = HAM_DEFAULT_CACHESIZE)
      : m_env(env), m_capacity(capacity_bytes), m_cur_elements(0),
        m_alloc_elements(0), m_totallist(0), m_totallist_tail(0),
        m_cache_hits(0), m_cache_misses(0) {
      if (m_capacity == 0)
        m_capacity = HAM_DEFAULT_CACHESIZE;

      for (ham_u32_t i = 0; i < kBucketSize; i++)
        m_buckets.push_back(0);
    }

    // Retrieves a page from the cache, also removes the page from the cache
    // and re-inserts it at the front. Returns null if the page was not cached.
    Page *get_page(ham_u64_t address, ham_u32_t flags = 0) {
      ham_u64_t hash = calc_hash(address);
      Page *page = m_buckets[hash];
      while (page) {
        if (page->get_address() == address)
          break;
        page = page->get_next(Page::kListBucket);
      }

      /* not found? then return */
      if (!page) {
        m_cache_misses++;
        return (0);
      }

      // Now re-insert the page at the head of the "totallist", and
      // thus move far away from the tail. The pages at the tail are highest
      // candidates to be deleted when the cache is purged.
      remove_page(page);
      put_page(page);

      m_cache_hits++;

      return (page);
    }

    // Stores a page in the cache
    void put_page(Page *page) {
      ham_u64_t hash = calc_hash(page->get_address());

      ham_assert(page->get_data());

      /* first remove the page from the cache, if it's already cached
       *
       * we re-insert the page because we want to make sure that the
       * cache->_totallist_tail pointer is updated and that the page
       * is inserted at the HEAD of the list
       */
      if (page->is_in_list(m_totallist, Page::kListCache))
        remove_page(page);

      /* now (re-)insert into the list of all cached pages, and increment
       * the counter */
      ham_assert(!page->is_in_list(m_totallist, Page::kListCache));
      m_totallist = page->list_insert(m_totallist, Page::kListCache);

      m_cur_elements++;
      if (page->get_flags() & Page::kNpersMalloc)
        m_alloc_elements++;

      /*
       * insert it in the cache buckets
       * !!!
       * to avoid inserting the page twice, we first remove it from the
       * bucket
       */
      if (page->is_in_list(m_buckets[hash], Page::kListBucket))
        m_buckets[hash] = page->list_remove(m_buckets[hash], Page::kListBucket);
      ham_assert(!page->is_in_list(m_buckets[hash], Page::kListBucket));
      m_buckets[hash] = page->list_insert(m_buckets[hash], Page::kListBucket);

      /* is this the chronologically oldest page? then set the pointer */
      if (!m_totallist_tail)
        m_totallist_tail = page;
    }

    // Removes a page from the cache
    void remove_page(Page *page) {
      bool removed = false;

      /* are we removing the chronologically oldest page? then
       * update the pointer with the next oldest page */
      if (m_totallist_tail == page)
        m_totallist_tail = page->get_previous(Page::kListCache);

      /* remove the page from the cache buckets */
      if (page->get_address()) {
        ham_u64_t hash = calc_hash(page->get_address());
        if (page->is_in_list(m_buckets[hash], Page::kListBucket)) {
          m_buckets[hash] = page->list_remove(m_buckets[hash],
                        Page::kListBucket);
        }
      }

      /* remove it from the list of all cached pages */
      if (page->is_in_list(m_totallist, Page::kListCache)) {
        m_totallist = page->list_remove(m_totallist, Page::kListCache);
        removed = true;
      }
      /* decrease the number of cached elements */
      if (removed) {
        m_cur_elements--;
        if (page->get_flags() & Page::kNpersMalloc)
          m_alloc_elements--;
      }
    }

    typedef void (*PurgeCallback)(Page *page, PageManager *pm);

    // Purges the cache; the callback is called for every page that needs
    // to be purged
    void purge(PurgeCallback cb, PageManager *pm, unsigned limit) {
      ham_assert(is_full() && limit > 0);

      unsigned i = 0;

      /* get the chronologically oldest page */
      Page *oldest = m_totallist_tail;
      if (!oldest)
        return;

      /* now iterate through all pages, starting from the oldest
       * (which is the tail of the "totallist", the list of ALL cached
       * pages) */
      Page *page = oldest;
      do {
        /* pick the first unused page (not in a changeset) that is NOT mapped */
        if (page->get_flags() & Page::kNpersMalloc
            && !m_env->get_changeset().contains(page)) {
          Page *prev = page->get_previous(Page::kListCache);
          remove_page(page);
          cb(page, pm);
          i++;
          page = prev;
        }
        else
          page = page->get_previous(Page::kListCache);
        ham_assert(page != oldest);
      } while (i < limit && page && page != oldest);
    }

    // the visitor callback returns true if the page should be removed from
    // the cache and deleted
    typedef bool (*VisitCallback)(Page *page, Database *db, ham_u32_t flags);

    // Visits all pages in the "totallist"; this is used by the Environment
    // to flush (and delete) pages
    void visit(VisitCallback cb, Database *db, ham_u32_t flags) {
      Page *head = m_totallist;
      while (head) {
        Page *next = head->get_next(Page::kListCache);

        if (cb(head, db, flags)) {
          remove_page(head);
          delete head;
        }
        head = next;
      }
    }

    // Returns true if the caller should purge the cache
    bool is_full() const {
      return (m_alloc_elements * m_env->get_page_size() > m_capacity);
    }

    // Returns the capacity (in bytes)
    ham_u64_t get_capacity() const {
      return (m_capacity);
    }

    // Returns the number of currently cached elements
    ham_u64_t get_current_elements() const {
      return (m_cur_elements);
    }

    // Checks the cache integrity; throws an exception if the integrity
    // was violated
    void check_integrity();

    // Fills in the current metrics
    void get_metrics(ham_env_metrics_t *metrics) const {
      metrics->cache_hits = m_cache_hits;
      metrics->cache_misses = m_cache_misses;
    }

  private:
    // Calculates the hash of a page address
    ham_u64_t calc_hash(ham_u64_t o) const {
      return (o % kBucketSize);
    }

    // Sets the HEAD of the global page list
    void set_totallist(Page *l) {
      m_totallist = l;
    }

    // the current Environment
    LocalEnvironment *m_env;

    // the capacity (in bytes)
    ham_u64_t m_capacity;

    // the current number of cached elements
    ham_u64_t m_cur_elements;

    // the current number of cached elements that were allocated (and not
    // mapped)
    ham_u64_t m_alloc_elements;

    // linked list of ALL cached pages
    Page *m_totallist;

    // the tail of the linked "totallist" - this is the oldest element,
    // and therefore the highest candidate for a flush
    Page *m_totallist_tail;

    // the buckets - a linked list of Page pointers
    std::vector<Page *> m_buckets;

    // counts the cache hits
    ham_u64_t m_cache_hits;

    // counts the cache misses
    ham_u64_t m_cache_misses;
};

} // namespace hamsterdb

#endif /* HAM_CACHE_H__ */
