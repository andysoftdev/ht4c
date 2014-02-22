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

#ifndef HAM_BTREE_FLAGS_H__
#define HAM_BTREE_FLAGS_H__

namespace hamsterdb {

//
// A helper class wrapping key-related constants into a common namespace.
// This class does not contain any logic.
//
struct BtreeKey
{
  // persisted btree key flags; also used in combination
  // with ham_key_t._flags and with the BtreeRecord flags below.
  enum {
    // key is extended with overflow area
    kExtendedKey          = 0x01,

    // key has duplicates in an overflow area
    kExtendedDuplicates   = 0x02,

    // key is initialized and empty (with one record)
    kInitialized          = 0x04,

    // this key has no records attached (this flag is used if the key does
    // not have a separate "record counter" field
    kHasNoRecords         = 0x08
  };

  // flags used with the ham_key_t::_flags (note the underscore - this
  // field is for INTERNAL USE!)
  //
  // Note: these flags should NOT overlap with the persisted flags above!
  //
  // As these flags NEVER will be persisted, they should be located outside
  // the range of a ham_u16_t, i.e. outside the mask 0x0000ffff.
  enum {
    // Actual key is lower than the requested key
    kLower               = 0x00010000,

    // Actual key is greater than the requested key
    kGreater             = 0x00020000,

    // Actual key is an "approximate match"
    kApproximate         = (kLower | kGreater)
  };
};

//
// A helper class wrapping record-related constants into a common namespace.
// This class does not contain any logic.
//
struct BtreeRecord
{
  enum {
    // record size < 8; length is encoded at byte[7] of key->ptr
    kBlobSizeTiny         = 0x10,

    // record size == 8; record is stored in key->ptr
    kBlobSizeSmall        = 0x20,

    // record size == 0; key->ptr == 0
    kBlobSizeEmpty        = 0x40
  };
};

} // namespace hamsterdb

#endif /* HAM_BTREE_FLAGS_H__ */
