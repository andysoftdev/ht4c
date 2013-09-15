/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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
#ifndef HAM_OS_WIN32
#  include <libgen.h>
#endif

#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "log.h"
#include "mem.h"
#include "os.h"
#include "txn.h"
#include "util.h"
#include "page_manager.h"

namespace hamsterdb {

ham_status_t
Log::create()
{
  Log::PHeader header;
  ham_status_t st;
  std::string path = get_path();

  /* create the files */
  st=os_create(path.c_str(), 0, 0644, &m_fd);
  if (st)
    return (st);

  /* write the file header with the magic */
  header.magic = HEADER_MAGIC;

  st = os_write(m_fd, &header, sizeof(header));
  if (st) {
    close();
    return (st);
  }

  return (0);
}

ham_status_t
Log::open()
{
  Log::PHeader header;
  std::string path = get_path();
  ham_status_t st;

  /* open the file */
  st = os_open(path.c_str(), 0, &m_fd);
  if (st) {
    close();
    return (st);
  }

  /* check the file header with the magic */
  st = os_pread(m_fd, 0, &header, sizeof(header));
  if (st) {
    close();
    return (st);
  }
  if (header.magic != HEADER_MAGIC) {
    ham_trace(("logfile has unknown magic or is corrupt"));
    close();
    return (HAM_LOG_INV_FILE_HEADER);
  }

  /* store the lsn */
  m_lsn = header.lsn;

  return (0);
}

ham_status_t
Log::get_entry(Log::Iterator *iter, Log::PEntry *entry, ham_u8_t **data)
{
  ham_status_t st;

  *data = 0;

  /* if the iterator is initialized and was never used before: read
   * the file size */
  if (!*iter) {
    st = os_get_filesize(m_fd, iter);
    if (st)
      return (st);
  }

  /* if the current file is empty: no need to continue */
  if (*iter <= sizeof(Log::PHeader)) {
    entry->lsn = 0;
    return (0);
  }

  /* otherwise read the Log::PEntry header (without extended data)
   * from the file */
  *iter -= sizeof(Log::PEntry);

  st = os_pread(m_fd, *iter, entry, sizeof(*entry));
  if (st)
    return (st);

  /* now read the extended data, if it's available */
  if (entry->data_size) {
    ham_u64_t pos = (*iter) - entry->data_size;
    pos -= (pos % 8);

    *data = Memory::allocate<ham_u8_t>((ham_size_t)entry->data_size);
    if (!*data)
      return (HAM_OUT_OF_MEMORY);

    st = os_pread(m_fd, pos, *data, (ham_size_t)entry->data_size);
    if (st) {
      Memory::release(*data);
      *data = 0;
      return (st);
    }

    *iter = pos;
  }
  else
    *data = 0;

  return (0);
}

ham_status_t
Log::close(bool noclear)
{
  ham_status_t st = 0;
  Log::PHeader header;

  if (m_fd == HAM_INVALID_FD)
    return (0);

  /* write the file header with the magic and the last used lsn */
  header.magic = HEADER_MAGIC;
  header.lsn = m_lsn;

  st = os_pwrite(m_fd, 0, &header, sizeof(header));
  if (st)
    return (st);

  if (!noclear)
    clear();

  if ((st = os_close(m_fd)))
    return (st);
  m_fd = HAM_INVALID_FD;

  return (0);
}

ham_status_t
Log::append_page(Page *page, ham_u64_t lsn, ham_size_t page_count)
{
  ham_status_t st = 0;
  ham_u8_t *p = (ham_u8_t *)page->get_raw_payload();
  ham_size_t size = m_env->get_pagesize();

  if (st == 0)
    st = append_write(lsn, page_count == 0 ? CHANGESET_IS_COMPLETE : 0,
            page->get_self(), p, size);

  if (p != page->get_raw_payload())
    Memory::release(p);

  return (st);
}

ham_status_t
Log::recover()
{
  ham_status_t st;
  Page *page;
  Device *device = m_env->get_device();
  Log::PEntry entry;
  Iterator it = 0;
  ham_u8_t *data = 0;
  ham_u64_t filesize;
  bool first_loop = true;

  /* get the file size of the database; otherwise we do not know if we
   * modify an existing page or if one of the pages has to be allocated */
  st = device->get_filesize(&filesize);
  if (st)
    return (st);

  /* temporarily disable logging */
  m_env->set_flags(m_env->get_flags() & ~HAM_ENABLE_RECOVERY);

  /* now start the loop once more and apply the log */
  while (1) {
    /* clean up memory of the previous loop */
    if (data) {
      Memory::release(data);
      data = 0;
    }

    /* get the next entry in the logfile */
    st = get_entry(&it, &entry, &data);
    if (st)
      goto bail;

    /* first make sure that the log is complete; if not then it will not
     * be applied  */
    if (first_loop) {
      if (entry.flags != CHANGESET_IS_COMPLETE) {
        ham_log(("log is incomplete and will be ignored"));
        goto clear;
      }
      first_loop = false;
    }

    /* reached end of the log file? */
    if (entry.lsn == 0)
      break;

    /*
     * Was the page appended or overwritten?
     *
     * Either way we have to bypass the cache and all upper layers. We
     * cannot call Database::alloc_page() or Database::fetch_page() since there 
     * is no Database handle. env->alloc_page()/env->fetch_page() would work,
     * but then the page ownership is not set correctly (the
     * ownership is verified later, and this would fail).
     */
    if (entry.offset == filesize) {
      /* appended... */
      filesize += entry.data_size;

      page = new Page(m_env);
      st = page->allocate();
      if (st)
        goto bail;
    }
    else {
      /* overwritten... */
      page = new Page(m_env);
      st = page->fetch(entry.offset);
      if (st)
        goto bail;
    }

    ham_assert(page->get_self() == entry.offset);
    ham_assert(m_env->get_pagesize() == entry.data_size);

    /* overwrite the page data */
    memcpy(page->get_pers(), data, entry.data_size);

    /* flush the modified page to disk */
    page->set_dirty(true);
    st = m_env->get_page_manager()->flush_page(page);
    if (st)
      goto bail;
    page->free();
    delete page;

    /* store the lsn in the log - will be needed later when recovering
     * the journal */
    m_lsn = entry.lsn;
  }

clear:
  /* and finally clear the log */
  st = clear();
  if (st) {
    ham_log(("unable to clear logfiles; please manually delete the "
             ".log0 file of this Database, then open again."));
    goto bail;
  }

bail:
  /* re-enable the logging */
  m_env->set_flags(m_env->get_flags() | HAM_ENABLE_RECOVERY);

  /* clean up memory */
  Memory::release(data);
  data = 0;

  return (st);
}

ham_status_t
Log::append_write(ham_u64_t lsn, ham_u32_t flags, ham_u64_t offset,
                ham_u8_t *data, ham_size_t size)
{
  Log::PEntry entry;

  /* store the lsn - it will be needed later when the log file is closed */
  if (lsn)
    m_lsn = lsn;

  entry.lsn = lsn;
  entry.flags = flags;
  entry.offset = offset;
  entry.data_size = size;

  return (os_writev(m_fd, data, size, &entry, sizeof(entry)));
}

std::string
Log::get_path()
{
  std::string path;

  if (m_env->get_log_directory().empty()) {
    path = m_env->get_filename();
  }
  else {
    path = m_env->get_log_directory();
#ifdef HAM_OS_WIN32
    path += "\\";
    char fname[_MAX_FNAME];
    char ext[_MAX_EXT];
    _splitpath(m_env->get_filename().c_str(), 0, 0, fname, ext);
    path += fname;
    path += ext;
#else
    path += "/";
    path += ::basename((char *)m_env->get_filename().c_str());
#endif
  }
  path += ".log0";
  return (path);
}

} // namespace hamsterdb
