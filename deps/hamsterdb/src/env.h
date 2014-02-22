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

#ifndef HAM_ENV_H__
#define HAM_ENV_H__

#include <map>
#include <string>

#include <ham/hamsterdb_int.h>

#include "endianswap.h"
#include "error.h"
#include "mutex.h"

// A helper structure; ham_env_t is declared in ham/hamsterdb.h as an
// opaque C structure, but internally we use a C++ class. The ham_env_t
// struct satisfies the C compiler, and internally we just cast the pointers.
struct ham_env_t {
  int dummy;
};

namespace hamsterdb {

class Database;
class Transaction;

//
// The Environment is the "root" of all hamsterdb objects. It's a container
// for multiple databases and transactions.
//
class Environment
{
  public:
    // A map of all opened Databases
    typedef std::map<ham_u16_t, Database *> DatabaseMap;

    // Constructor
    Environment()
      : m_file_mode(0644), m_context(0), m_flags(0),
        m_oldest_txn(0), m_newest_txn(0) {
    }

    // Virtual destructor can be overwritten in derived classes
    virtual ~Environment() { }

    // Returns the flags which were set when creating/opening the Environment
    ham_u32_t get_flags() const {
      return (m_flags);
    }

    // Sets the flags
    void set_flags(ham_u32_t flags) {
      m_flags = flags;
    }

    // Returns the filename of the Environment; can be empty (i.e.
    // for an in-memory environment)
    const std::string &get_filename() {
      return (m_filename);
    }

    // Sets the filename of the Environment; only for testing!
    void test_set_filename(const std::string &filename) {
      m_filename = filename;
    }

    // Returns the unix file mode
    ham_u32_t get_file_mode() const {
      return (m_file_mode);
    }

    // Returns the user-provided context pointer (ham_env_get_context_data)
    void *get_context_data() {
      return (m_context);
    }

    // Sets the user-provided context pointer (ham_env_set_context_data)
    void set_context_data(void *ctxt) {
      m_context = ctxt;
    }

    // Returns this Environment's mutex
    Mutex &get_mutex() {
      return (m_mutex);
    }

    // Returns the Database Map
    DatabaseMap &get_database_map() {
      return (m_database_map);
    }

    // Creates a new Environment (ham_env_create)
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
                    ham_u32_t mode, ham_u32_t page_size, ham_u64_t cache_size,
                    ham_u16_t maxdbs) = 0;

    // Opens a new Environment (ham_env_open)
    virtual ham_status_t open(const char *filename, ham_u32_t flags,
                    ham_u64_t cache_size) = 0;

    // Renames a database in the Environment (ham_env_rename_db)
    virtual ham_status_t rename_db(ham_u16_t oldname, ham_u16_t newname,
                    ham_u32_t flags) = 0;

    // Erases (deletes) a database from the Environment (ham_env_erase_db)
    virtual ham_status_t erase_db(ham_u16_t name, ham_u32_t flags) = 0;

    // Returns all database names (ham_env_get_database_names)
    virtual ham_status_t get_database_names(ham_u16_t *names,
            ham_u32_t *count) = 0;

    // Returns environment parameters and flags (ham_env_get_parameters)
    virtual ham_status_t get_parameters(ham_parameter_t *param) = 0;

    // Flushes the environment and its databases to disk (ham_env_flush)
    virtual ham_status_t flush(ham_u32_t flags) = 0;

    // Creates a new database in the environment (ham_env_create_db)
    virtual ham_status_t create_db(Database **db, ham_u16_t dbname,
                    ham_u32_t flags, const ham_parameter_t *param) = 0;

    // Opens an existing database in the environment (ham_env_open_db)
    virtual ham_status_t open_db(Database **db, ham_u16_t dbname,
                    ham_u32_t flags, const ham_parameter_t *param) = 0;

    // Begins a new transaction (ham_txn_begin)
    virtual ham_status_t txn_begin(Transaction **txn, const char *name,
                    ham_u32_t flags) = 0;

    // Aborts a transaction (ham_txn_abort)
    virtual ham_status_t txn_abort(Transaction *txn, ham_u32_t flags) = 0;

    // Commits a transaction (ham_txn_commit)
    virtual ham_status_t txn_commit(Transaction *txn, ham_u32_t flags) = 0;

    // Closes the Environment (ham_env_close)
    virtual ham_status_t close(ham_u32_t flags) = 0;

    // Fills in the current metrics
    virtual void get_metrics(ham_env_metrics_t *metrics) const { };

    // Returns the oldest transaction which not yet flushed to disk
    Transaction *get_oldest_txn() {
      return (m_oldest_txn);
    }

    // Returns the newest transaction which not yet flushed to disk
    Transaction *get_newest_txn() {
      return (m_newest_txn);
    }

  protected:
    // Adds a new transaction to this Environment
    void append_txn(Transaction *txn);

    // Removes a transaction from this Environment
    void remove_txn(Transaction *txn);

    // A mutex to serialize access to this Environment
    Mutex m_mutex;

    // The filename/url of this environment
    std::string m_filename;

    // The file access 'mode' parameter of ham_env_create */
    ham_u32_t m_file_mode;

  private:
    // Sets the oldest transaction which not yet flushed to disk
    void set_oldest_txn(Transaction *txn) {
      m_oldest_txn = txn;
    }

    // Sets the newest transaction which not yet flushed to disk
    void set_newest_txn(Transaction *txn) {
      m_newest_txn = txn;
    }

    // The user-provided context data
    void *m_context;

    // A map of all opened Databases
    DatabaseMap m_database_map;

    // The Environment flags - a combination of the persistent flags
    // and runtime flags
    ham_u32_t m_flags;

    // The head of the transaction list (the oldest transaction)
    Transaction *m_oldest_txn;

    // The tail of the transaction list (the youngest/newest transaction)
    Transaction *m_newest_txn;
};

} // namespace hamsterdb

#endif /* HAM_ENV_H__ */
