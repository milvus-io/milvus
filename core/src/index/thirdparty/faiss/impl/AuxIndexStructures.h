/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

// Auxiliary index structures, that are used in indexes but that can
// be forward-declared

#ifndef FAISS_AUX_INDEX_STRUCTURES_H
#define FAISS_AUX_INDEX_STRUCTURES_H

#include <stdint.h>

#include <vector>
#include <unordered_set>
#include <memory>
#include <mutex>

#include <faiss/Index.h>

namespace faiss {

/** The objective is to have a simple result structure while
 *  minimizing the number of mem copies in the result. The method
 *  do_allocation can be overloaded to allocate the result tables in
 *  the matrix type of a scripting language like Lua or Python. */
struct RangeSearchResult {
    size_t nq;      ///< nb of queries
    size_t *lims;   ///< size (nq + 1)

    typedef Index::idx_t idx_t;

    idx_t *labels;     ///< result for query i is labels[lims[i]:lims[i+1]]
    float *distances;  ///< corresponding distances (not sorted)

    size_t buffer_size; ///< size of the result buffers used

    /// lims must be allocated on input to range_search.
    explicit RangeSearchResult (idx_t nq, bool alloc_lims=true);

    /// called when lims contains the nb of elements result entries
    /// for each query

    virtual void do_allocation ();

    virtual ~RangeSearchResult ();
};


/** Encapsulates a set of ids to remove. */
struct IDSelector {
    typedef Index::idx_t idx_t;
    virtual bool is_member (idx_t id) const = 0;
    virtual ~IDSelector() {}
};



/** remove ids between [imni, imax) */
struct IDSelectorRange: IDSelector {
    idx_t imin, imax;

    IDSelectorRange (idx_t imin, idx_t imax);
    bool is_member(idx_t id) const override;
    ~IDSelectorRange() override {}
};

/** simple list of elements to remove
 *
 * this is inefficient in most cases, except for IndexIVF with
 * maintain_direct_map
 */
struct IDSelectorArray: IDSelector {
    size_t n;
    const idx_t *ids;

    IDSelectorArray (size_t n, const idx_t *ids);
    bool is_member(idx_t id) const override;
    ~IDSelectorArray() override {}
};

/** Remove ids from a set. Repetitions of ids in the indices set
 * passed to the constructor does not hurt performance. The hash
 * function used for the bloom filter and GCC's implementation of
 * unordered_set are just the least significant bits of the id. This
 * works fine for random ids or ids in sequences but will produce many
 * hash collisions if lsb's are always the same */
struct IDSelectorBatch: IDSelector {

    std::unordered_set<idx_t> set;

    typedef unsigned char uint8_t;
    std::vector<uint8_t> bloom; // assumes low bits of id are a good hash value
    int nbits;
    idx_t mask;

    IDSelectorBatch (size_t n, const idx_t *indices);
    bool is_member(idx_t id) const override;
    ~IDSelectorBatch() override {}
};

/****************************************************************
 * Result structures for range search.
 *
 * The main constraint here is that we want to support parallel
 * queries from different threads in various ways: 1 thread per query,
 * several threads per query. We store the actual results in blocks of
 * fixed size rather than exponentially increasing memory. At the end,
 * we copy the block content to a linear result array.
 *****************************************************************/

/** List of temporary buffers used to store results before they are
 *  copied to the RangeSearchResult object. */
struct BufferList {
    typedef Index::idx_t idx_t;

    // buffer sizes in # entries
    size_t buffer_size;

    struct Buffer {
        idx_t *ids;
        float *dis;
    };

    std::vector<Buffer> buffers;
    size_t wp; ///< write pointer in the last buffer.

    explicit BufferList (size_t buffer_size);

    ~BufferList ();

    /// create a new buffer
    void append_buffer ();

    /// add one result, possibly appending a new buffer if needed
    void add (idx_t id, float dis);

    /// copy elemnts ofs:ofs+n-1 seen as linear data in the buffers to
    /// tables dest_ids, dest_dis
    void copy_range (size_t ofs, size_t n,
                     idx_t * dest_ids, float *dest_dis);

};

struct RangeSearchPartialResult;

/// result structure for a single query
struct RangeQueryResult {
    using idx_t = Index::idx_t;
    idx_t qno;    //< id of the query
    size_t nres;  //< nb of results for this query
    RangeSearchPartialResult * pres;

    /// called by search function to report a new result
    void add (float dis, idx_t id);
};

/// the entries in the buffers are split per query
struct RangeSearchPartialResult: BufferList {
    RangeSearchResult * res;

    /// eventually the result will be stored in res_in
    explicit RangeSearchPartialResult (RangeSearchResult * res_in);

    /// query ids + nb of results per query.
    std::vector<RangeQueryResult> queries;

    /// begin a new result
    RangeQueryResult & new_result (idx_t qno);

    /*****************************************
     * functions used at the end of the search to merge the result
     * lists */
    void finalize ();

    /// called by range_search before do_allocation
    void set_lims ();

    /// called by range_search after do_allocation
    void copy_result (bool incremental = false);

    /// merge a set of PartialResult's into one RangeSearchResult
    /// on ouptut the partialresults are empty!
    static void merge (std::vector <RangeSearchPartialResult *> &
                       partial_results, bool do_delete=true);

};


/***********************************************************
 * The distance computer maintains a current query and computes
 * distances to elements in an index that supports random access.
 *
 * The DistanceComputer is not intended to be thread-safe (eg. because
 * it maintains counters) so the distance functions are not const,
 * instanciate one from each thread if needed.
 ***********************************************************/
struct DistanceComputer {
     using idx_t = Index::idx_t;

     /// called before computing distances
     virtual void set_query(const float *x) = 0;

     /// compute distance of vector i to current query
     virtual float operator () (idx_t i) = 0;

     /// compute distance between two stored vectors
     virtual float symmetric_dis (idx_t i, idx_t j) = 0;

     virtual ~DistanceComputer() {}
};

/***********************************************************
 * Interrupt callback
 ***********************************************************/

struct InterruptCallback {
    virtual bool want_interrupt () = 0;
    virtual ~InterruptCallback() {}

    // lock that protects concurrent calls to is_interrupted
    static std::mutex lock;

    static std::unique_ptr<InterruptCallback> instance;

    static void clear_instance ();

    /** check if:
     * - an interrupt callback is set
     * - the callback retuns true
     * if this is the case, then throw an exception. Should not be called
     * from multiple threds.
     */
    static void check ();

    /// same as check() but return true if is interrupted instead of
    /// throwing. Can be called from multiple threads.
    static bool is_interrupted ();

    /** assuming each iteration takes a certain number of flops, what
     * is a reasonable interval to check for interrupts?
     */
    static size_t get_period_hint (size_t flops);

};



}; // namespace faiss



#endif
