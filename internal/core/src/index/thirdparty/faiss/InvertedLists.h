/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_INVERTEDLISTS_IVF_H
#define FAISS_INVERTEDLISTS_IVF_H

/**
 * Definition of inverted lists + a few common classes that implement
 * the interface.
 */

#include <memory>
#include <vector>
#include <faiss/Index.h>

#ifndef USE_CPU
namespace faiss {

struct PageLockMemory {
public:
    PageLockMemory() : data(nullptr), nbytes(0) {}

    PageLockMemory(size_t size);

    ~PageLockMemory();

    PageLockMemory(const PageLockMemory& other);

    PageLockMemory(PageLockMemory &&other);

    inline size_t size() {
        return nbytes;
    }

    void *data;
    size_t nbytes;
};
using PageLockMemoryPtr = std::shared_ptr<PageLockMemory>;
}
#endif

namespace faiss {

/** Table of inverted lists
 * multithreading rules:
 * - concurrent read accesses are allowed
 * - concurrent update accesses are allowed
 * - for resize and add_entries, only concurrent access to different lists
 *   are allowed
 */
struct InvertedLists {
    typedef Index::idx_t idx_t;

    size_t nlist;             ///< number of possible key values
    size_t code_size;         ///< code size per vector in bytes

    InvertedLists (size_t nlist, size_t code_size);

    /*************************
     *  Read only functions */

    /// get the size of a list
    virtual size_t list_size(size_t list_no) const = 0;

    /** get the codes for an inverted list
     * must be released by release_codes
     *
     * @return codes    size list_size * code_size
     */
    virtual const uint8_t * get_codes (size_t list_no) const = 0;

    /** get the ids for an inverted list
     * must be released by release_ids
     *
     * @return ids      size list_size
     */
    virtual const idx_t * get_ids (size_t list_no) const = 0;

    /// release codes returned by get_codes (default implementation is nop
    virtual void release_codes (size_t list_no, const uint8_t *codes) const;

    /// release ids returned by get_ids
    virtual void release_ids (size_t list_no, const idx_t *ids) const;

    /// @return a single id in an inverted list
    virtual idx_t get_single_id (size_t list_no, size_t offset) const;

    /// @return a single code in an inverted list
    /// (should be deallocated with release_codes)
    virtual const uint8_t * get_single_code (
                size_t list_no, size_t offset) const;

    /// prepare the following lists (default does nothing)
    /// a list can be -1 hence the signed long
    virtual void prefetch_lists (const idx_t *list_nos, int nlist) const;

    /*************************
     * writing functions     */

    /// add one entry to an inverted list
    virtual size_t add_entry (size_t list_no, idx_t theid,
                              const uint8_t *code);

    virtual size_t add_entries (
           size_t list_no, size_t n_entry,
           const idx_t* ids, const uint8_t *code) = 0;

    /// add one entry to an inverted list without codes
    virtual size_t add_entry_without_codes (size_t list_no, idx_t theid);

    virtual size_t add_entries_without_codes ( size_t list_no, size_t n_entry,
                                               const idx_t* ids);

    virtual void update_entry (size_t list_no, size_t offset,
                               idx_t id, const uint8_t *code);

    virtual void update_entries (size_t list_no, size_t offset, size_t n_entry,
                                 const idx_t *ids, const uint8_t *code) = 0;

    virtual void resize (size_t list_no, size_t new_size) = 0;

    virtual void reset ();

    virtual InvertedLists* to_readonly();

    virtual InvertedLists* to_readonly_without_codes();

    virtual bool is_readonly() const;

    /// move all entries from oivf (empty on output)
    void merge_from (InvertedLists *oivf, size_t add_id);

    virtual ~InvertedLists ();

    /*************************
     * statistics            */

    /// 1= perfectly balanced, >1: imbalanced
    double imbalance_factor () const;

    /// display some stats about the inverted lists
    void print_stats () const;

    /// sum up list sizes
    size_t compute_ntotal () const;

    /**************************************
     * Scoped inverted lists (for automatic deallocation)
     *
     * instead of writing:
     *
     *     uint8_t * codes = invlists->get_codes (10);
     *     ... use codes
     *     invlists->release_codes(10, codes)
     *
     * write:
     *
     *    ScopedCodes codes (invlists, 10);
     *    ... use codes.get()
     *    // release called automatically when codes goes out of scope
     *
     * the following function call also works:
     *
     *    foo (123, ScopedCodes (invlists, 10).get(), 456);
     *
     */

    struct ScopedIds {
        const InvertedLists *il;
        const idx_t *ids;
        size_t list_no;

        ScopedIds (const InvertedLists *il, size_t list_no):
        il (il), ids (il->get_ids (list_no)), list_no (list_no)
        {}

        const idx_t *get() {return ids; }

        idx_t operator [] (size_t i) const {
            return ids[i];
        }

        ~ScopedIds () {
            il->release_ids (list_no, ids);
        }
    };

    struct ScopedCodes {
        const InvertedLists *il;
        const uint8_t *codes;
        size_t list_no;

        ScopedCodes (const InvertedLists *il, size_t list_no):
            il (il), codes (il->get_codes (list_no)), list_no (list_no)
        {}

        ScopedCodes (const InvertedLists *il, size_t list_no, size_t offset):
            il (il), codes (il->get_single_code (list_no, offset)),
            list_no (list_no)
        {}

        // For codes outside
        ScopedCodes (const InvertedLists *il, size_t list_no, const uint8_t *original_codes):
            il (il), codes (original_codes), list_no (list_no)
        {}

        const uint8_t *get() {return codes; }

        ~ScopedCodes () {
            il->release_codes (list_no, codes);
        }
    };


};


/// simple (default) implementation as an array of inverted lists
struct ArrayInvertedLists: InvertedLists {
    std::vector < std::vector<uint8_t> > codes; // binary codes, size nlist
    std::vector < std::vector<idx_t> > ids;  ///< Inverted lists for indexes

    ArrayInvertedLists (size_t nlist, size_t code_size);

    size_t list_size(size_t list_no) const override;
    const uint8_t * get_codes (size_t list_no) const override;
    const idx_t * get_ids (size_t list_no) const override;

    size_t add_entries (
           size_t list_no, size_t n_entry,
           const idx_t* ids, const uint8_t *code) override;

    size_t add_entries_without_codes ( 
           size_t list_no, size_t n_entry,
           const idx_t* ids) override;

    void update_entries (size_t list_no, size_t offset, size_t n_entry,
                         const idx_t *ids, const uint8_t *code) override;

    void resize (size_t list_no, size_t new_size) override;

    InvertedLists* to_readonly() override;

    InvertedLists* to_readonly_without_codes() override;

    virtual ~ArrayInvertedLists ();
};

struct ReadOnlyArrayInvertedLists: InvertedLists {
#ifdef USE_CPU
    std::vector <uint8_t> readonly_codes;
    std::vector <idx_t> readonly_ids;
#else
    PageLockMemoryPtr pin_readonly_codes;
    PageLockMemoryPtr pin_readonly_ids;
#endif

    std::vector <size_t> readonly_length;
    std::vector <size_t> readonly_offset;
    bool valid;

    ReadOnlyArrayInvertedLists(size_t nlist, size_t code_size, const std::vector<size_t>& list_length);
    explicit ReadOnlyArrayInvertedLists(const ArrayInvertedLists& other);
    explicit ReadOnlyArrayInvertedLists(const ArrayInvertedLists& other, bool offset);

    // Use default copy construct, just copy pointer, DON'T COPY pin_readonly_codes AND pin_readonly_ids
//    explicit ReadOnlyArrayInvertedLists(const ReadOnlyArrayInvertedLists &);
//    explicit ReadOnlyArrayInvertedLists(ReadOnlyArrayInvertedLists &&);
    virtual ~ReadOnlyArrayInvertedLists();

    size_t list_size(size_t list_no) const override;
    const uint8_t * get_codes (size_t list_no) const override;
    const idx_t * get_ids (size_t list_no) const override;

    const uint8_t * get_all_codes() const;
    const idx_t * get_all_ids() const;
    const std::vector<size_t>& get_list_length() const;

    size_t add_entries (
            size_t list_no, size_t n_entry,
            const idx_t* ids, const uint8_t *code) override;

    size_t add_entries_without_codes ( 
            size_t list_no, size_t n_entry,
            const idx_t* ids) override;

    void update_entries (size_t list_no, size_t offset, size_t n_entry,
                         const idx_t *ids, const uint8_t *code) override;

    void resize (size_t list_no, size_t new_size) override;

    bool is_readonly() const override;

    bool is_valid();
};

/*****************************************************************
 * Meta-inverted lists
 *
 * About terminology: the inverted lists are seen as a sparse matrix,
 * that can be stacked horizontally, vertically and sliced.
 *****************************************************************/

struct ReadOnlyInvertedLists: InvertedLists {

    ReadOnlyInvertedLists (size_t nlist, size_t code_size):
    InvertedLists (nlist, code_size) {}

    size_t add_entries (
           size_t list_no, size_t n_entry,
           const idx_t* ids, const uint8_t *code) override;

    size_t add_entries_without_codes ( 
           size_t list_no, size_t n_entry,
           const idx_t* ids) override;

    void update_entries (size_t list_no, size_t offset, size_t n_entry,
                         const idx_t *ids, const uint8_t *code) override;

    void resize (size_t list_no, size_t new_size) override;

};


/// Horizontal stack of inverted lists
struct HStackInvertedLists: ReadOnlyInvertedLists {

    std::vector<const InvertedLists *>ils;

    /// build InvertedLists by concatenating nil of them
    HStackInvertedLists (int nil, const InvertedLists **ils);

    size_t list_size(size_t list_no) const override;
    const uint8_t * get_codes (size_t list_no) const override;
    const idx_t * get_ids (size_t list_no) const override;

    void prefetch_lists (const idx_t *list_nos, int nlist) const override;

    void release_codes (size_t list_no, const uint8_t *codes) const override;
    void release_ids (size_t list_no, const idx_t *ids) const override;

    idx_t get_single_id (size_t list_no, size_t offset) const override;

    const uint8_t * get_single_code (
           size_t list_no, size_t offset) const override;

};

using ConcatenatedInvertedLists = HStackInvertedLists;


/// vertical slice of indexes in another InvertedLists
struct SliceInvertedLists: ReadOnlyInvertedLists {
    const InvertedLists *il;
    idx_t i0, i1;

    SliceInvertedLists(const InvertedLists *il, idx_t i0, idx_t i1);

    size_t list_size(size_t list_no) const override;
    const uint8_t * get_codes (size_t list_no) const override;
    const idx_t * get_ids (size_t list_no) const override;

    void release_codes (size_t list_no, const uint8_t *codes) const override;
    void release_ids (size_t list_no, const idx_t *ids) const override;

    idx_t get_single_id (size_t list_no, size_t offset) const override;

    const uint8_t * get_single_code (
           size_t list_no, size_t offset) const override;

    void prefetch_lists (const idx_t *list_nos, int nlist) const override;
};


struct VStackInvertedLists: ReadOnlyInvertedLists {
    std::vector<const InvertedLists *>ils;
    std::vector<idx_t> cumsz;

    /// build InvertedLists by concatenating nil of them
    VStackInvertedLists (int nil, const InvertedLists **ils);

    size_t list_size(size_t list_no) const override;
    const uint8_t * get_codes (size_t list_no) const override;
    const idx_t * get_ids (size_t list_no) const override;

    void release_codes (size_t list_no, const uint8_t *codes) const override;
    void release_ids (size_t list_no, const idx_t *ids) const override;

    idx_t get_single_id (size_t list_no, size_t offset) const override;

    const uint8_t * get_single_code (
           size_t list_no, size_t offset) const override;

    void prefetch_lists (const idx_t *list_nos, int nlist) const override;

};


/** use the first inverted lists if they are non-empty otherwise use the second
 *
 * This is useful if il1 has a few inverted lists that are too long,
 * and that il0 has replacement lists for those, with empty lists for
 * the others. */
struct MaskedInvertedLists: ReadOnlyInvertedLists {

    const InvertedLists *il0;
    const InvertedLists *il1;

    MaskedInvertedLists (const InvertedLists *il0,
                         const InvertedLists *il1);

    size_t list_size(size_t list_no) const override;
    const uint8_t * get_codes (size_t list_no) const override;
    const idx_t * get_ids (size_t list_no) const override;

    void release_codes (size_t list_no, const uint8_t *codes) const override;
    void release_ids (size_t list_no, const idx_t *ids) const override;

    idx_t get_single_id (size_t list_no, size_t offset) const override;

    const uint8_t * get_single_code (
           size_t list_no, size_t offset) const override;

    void prefetch_lists (const idx_t *list_nos, int nlist) const override;

};

} // namespace faiss


#endif
