/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_DIRECT_MAP_H
#define FAISS_DIRECT_MAP_H

#include <faiss/InvertedLists.h>
#include <unordered_map>


namespace faiss {

// When offsets list id + offset are encoded in an uint64
// we call this LO = list-offset

inline uint64_t lo_build (uint64_t list_id, uint64_t offset) {
    return  list_id << 32 | offset;
}

inline uint64_t lo_listno (uint64_t lo) {
    return lo >> 32;
}

inline uint64_t lo_offset (uint64_t lo) {
    return lo & 0xffffffff;
}

/**
 * Direct map: a way to map back from ids to inverted lists
 */
struct DirectMap {
    typedef Index::idx_t idx_t;

    enum Type {
       NoMap = 0,     // default
       Array = 1,     // sequential ids (only for add, no add_with_ids)
       Hashtable = 2  // arbitrary ids
    };
    Type type;

    /// map for direct access to the elements. Map ids to LO-encoded entries.
    std::vector <idx_t> array;
    std::unordered_map <idx_t, idx_t> hashtable;

    DirectMap();

    /// set type and initialize
    void set_type (Type new_type, const InvertedLists *invlists, size_t ntotal);

    /// get an entry
    idx_t get (idx_t id) const;

    /// for quick checks
    bool no () const {return type == NoMap; }

    /**
     * update the direct_map
     */

    /// throw if Array and ids is not NULL
    void check_can_add (const idx_t *ids);

    /// non thread-safe version
    void add_single_id (idx_t id, idx_t list_no, size_t offset);

    /// remove all entries
    void clear();

    /**
     * operations on inverted lists that require translation with a DirectMap
     */

    /// remove ids from the InvertedLists, possibly using the direct map
    size_t remove_ids(const IDSelector& sel, InvertedLists *invlists);

    /// update entries, using the direct map
    void update_codes (InvertedLists *invlists,
                       int n, const idx_t *ids,
                       const idx_t *list_nos,
                       const uint8_t *codes);



};

/// Thread-safe way of updating the direct_map
struct DirectMapAdd {

    typedef Index::idx_t idx_t;

    using Type = DirectMap::Type;

    DirectMap &direct_map;
    DirectMap::Type type;
    size_t ntotal;
    size_t n;
    const idx_t *xids;

    std::vector<idx_t> all_ofs;

    DirectMapAdd (DirectMap &direct_map, size_t n, const idx_t *xids);

    /// add vector i (with id xids[i]) at list_no and offset
    void add (size_t i, idx_t list_no, size_t offset);

    ~DirectMapAdd ();
};



}


#endif
