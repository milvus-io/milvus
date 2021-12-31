/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_ON_DISK_INVERTED_LISTS_H
#define FAISS_ON_DISK_INVERTED_LISTS_H

#include <vector>
#include <list>

#include <faiss/IndexIVF.h>

namespace faiss {


struct LockLevels;

/** On-disk storage of inverted lists.
 *
 * The data is stored in a mmapped chunk of memory (base ptointer ptr,
 * size totsize). Each list is a range of memory that contains (object
 * List) that contains:
 *
 * - uint8_t codes[capacity * code_size]
 * - followed by idx_t ids[capacity]
 *
 * in each of the arrays, the size <= capacity first elements are
 * used, the rest is not initialized.
 *
 * Addition and resize are supported by:
 * - roundind up the capacity of the lists to a power of two
 * - maintaining a list of empty slots, sorted by size.
 * - resizing the mmapped block is adjusted as needed.
 *
 * An OnDiskInvertedLists is compact if the size == capacity for all
 * lists and there are no available slots.
 *
 * Addition to the invlists is slow. For incremental add it is better
 * to use a default ArrayInvertedLists object and convert it to an
 * OnDisk with merge_from.
 *
 * When it is known that a set of lists will be accessed, it is useful
 * to call prefetch_lists, that launches a set of threads to read the
 * lists in parallel.
 */
struct OnDiskInvertedLists: InvertedLists {

    struct List {
        size_t size;     // size of inverted list (entries)
        size_t capacity; // allocated size (entries)
        size_t offset;   // offset in buffer (bytes)
        List ();
    };

    // size nlist
    std::vector<List> lists;

    struct Slot {
        size_t offset;    // bytes
        size_t capacity;  // bytes
        Slot (size_t offset, size_t capacity);
        Slot ();
    };

    // size whatever space remains
    std::list<Slot> slots;

    std::string filename;
    size_t totsize;
    uint8_t *ptr; // mmap base pointer
    bool read_only;  /// are inverted lists mapped read-only

    OnDiskInvertedLists (size_t nlist, size_t code_size,
                         const char *filename);

    size_t list_size(size_t list_no) const override;
    const uint8_t * get_codes (size_t list_no) const override;
    const idx_t * get_ids (size_t list_no) const override;

    size_t add_entries (
           size_t list_no, size_t n_entry,
           const idx_t* ids, const uint8_t *code) override;

    void update_entries (size_t list_no, size_t offset, size_t n_entry,
                         const idx_t *ids, const uint8_t *code) override;

    void resize (size_t list_no, size_t new_size) override;

    // copy all inverted lists into *this, in compact form (without
    // allocating slots)
    size_t merge_from (const InvertedLists **ils, int n_il, bool verbose=false);

    /// restrict the inverted lists to l0:l1 without touching the mmapped region
    void crop_invlists(size_t l0, size_t l1);

    void prefetch_lists (const idx_t *list_nos, int nlist) const override;

    virtual ~OnDiskInvertedLists ();

    // private

    LockLevels * locks;

    // encapsulates the threads that are busy prefeteching
    struct OngoingPrefetch;
    OngoingPrefetch *pf;
    int prefetch_nthread;

    void do_mmap ();
    void update_totsize (size_t new_totsize);
    void resize_locked (size_t list_no, size_t new_size);
    size_t allocate_slot (size_t capacity);
    void free_slot (size_t offset, size_t capacity);

    // empty constructor for the I/O functions
    OnDiskInvertedLists ();
};


} // namespace faiss

#endif
