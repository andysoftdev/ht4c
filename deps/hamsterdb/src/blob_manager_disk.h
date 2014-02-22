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

#ifndef HAM_BLOB_MANAGER_DISK_H__
#define HAM_BLOB_MANAGER_DISK_H__

#include "blob_manager.h"
#include "env_local.h"

namespace hamsterdb {

#include "packstart.h"

/*
 * The header of a blob page
 *
 * Contains a fixed length freelist and a couter for the number of free
 * bytes
 */
HAM_PACK_0 class HAM_PACK_1 PBlobPageHeader
{
  public:
    void initialize() {
      memset(this, 0, sizeof(PBlobPageHeader));
    }

    // Returns a PBlobPageHeader from a page
    static PBlobPageHeader *from_page(Page *page) {
      return (PBlobPageHeader *)&page->get_payload()[0];
    }

    // Returns the number of pages which are all managed by this header
    ham_u32_t get_num_pages() const {
      return (ham_db2h32(m_num_pages));
    }

    // Sets the number of pages which are all managed by this header
    void set_num_pages(ham_u32_t num_pages) {
      m_num_pages = ham_h2db32(num_pages);
    }

    // Returns the "free bytes" counter
    ham_u32_t get_free_bytes() const {
      return (ham_db2h32(m_free_bytes));
    }

    // Sets the "free bytes" counter
    void set_free_bytes(ham_u32_t free_bytes) {
      m_free_bytes = ham_h2db32(free_bytes);
    }

    // Returns the total number of freelist entries
    ham_u8_t get_freelist_entries() const {
      return (32);
    }

    // Returns the offset of freelist entry |i|
    ham_u32_t get_freelist_offset(ham_u32_t i) const {
      return (ham_h2db32(m_freelist[i].offset));
    }

    // Sets the offset of freelist entry |i|
    void set_freelist_offset(ham_u32_t i, ham_u32_t offset) {
      m_freelist[i].offset = ham_db2h32(offset);
    }

    // Returns the size of freelist entry |i|
    ham_u32_t get_freelist_size(ham_u32_t i) const {
      return (ham_h2db32(m_freelist[i].size));
    }

    // Sets the size of freelist entry |i|
    void set_freelist_size(ham_u32_t i, ham_u32_t size) {
      m_freelist[i].size = ham_db2h32(size);
    }

  private:
    // Number of "regular" pages for this blob; used for blobs exceeding
    // a page size
    ham_u32_t m_num_pages;

    // Number of free bytes in this page
    ham_u32_t m_free_bytes;

    // Number of freelist entries; currently unused
    ham_u8_t m_reserved;

    struct FreelistEntry {
      ham_u32_t offset;
      ham_u32_t size;
    };

    // The freelist - offset/size pairs in this page
    FreelistEntry m_freelist[32];
} HAM_PACK_2;

#include "packstop.h"


/*
 * A BlobManager for disk-based databases
 */
class DiskBlobManager : public BlobManager
{
  enum {
    // Overhead per page
    kPageOverhead = Page::kSizeofPersistentHeader + sizeof(PBlobPageHeader)
  };

  public:
    DiskBlobManager(LocalEnvironment *env)
      : BlobManager(env) {
    }

    // allocate/create a blob
    // returns the blob-id (the start address of the blob header)
    ham_u64_t allocate(LocalDatabase *db, ham_record_t *record,
                    ham_u32_t flags);

    // reads a blob and stores the data in |record|. The pointer |record.data|
    // is backed by the |arena|, unless |HAM_RECORD_USER_ALLOC| is set.
    // flags: either 0 or HAM_DIRECT_ACCESS
    void read(LocalDatabase *db, ham_u64_t blobid,
                    ham_record_t *record, ham_u32_t flags,
                    ByteArray *arena);

    // retrieves the size of a blob
    ham_u64_t get_blob_size(LocalDatabase *db, ham_u64_t blobid);

    // overwrite an existing blob
    //
    // will return an error if the blob does not exist
    // returns the blob-id (the start address of the blob header) in |blobid|
    ham_u64_t overwrite(LocalDatabase *db, ham_u64_t old_blobid,
                    ham_record_t *record, ham_u32_t flags);

    // delete an existing blob
    void erase(LocalDatabase *db, ham_u64_t blobid,
                    Page *page = 0, ham_u32_t flags = 0);

  private:
    friend class DuplicateManager;
    friend class BlobManagerFixture;

    // write a series of data chunks to storage at file offset 'addr'.
    //
    // The chunks are assumed to be stored in sequential order, adjacent
    // to each other, i.e. as one long data strip.
    void write_chunks(LocalDatabase *db, Page *page, ham_u64_t addr,
                    ham_u8_t **chunk_data, ham_u32_t *chunk_size,
                    ham_u32_t chunks);

    // same as above, but for reading chunks from the file
    void read_chunk(Page *page, Page **fpage, ham_u64_t addr,
                    LocalDatabase *db, ham_u8_t *data, ham_u32_t size);

    // adds a free chunk to the freelist
    void add_to_freelist(PBlobPageHeader *header, ham_u32_t offset,
                    ham_u32_t size);

    // searches the freelist for a free chunk; if available, returns |true|
    // and stores the offset in |poffset|.
    bool alloc_from_freelist(PBlobPageHeader *header, ham_u32_t size,
                    ham_u64_t *poffset);

    // verifies the integrity of the freelist
    bool check_integrity(PBlobPageHeader *header) const;
};

} // namespace hamsterdb

#endif /* HAM_BLOB_MANAGER_DISK_H__ */
