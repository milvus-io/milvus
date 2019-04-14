/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
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


#include "Index.h"

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

    IDSelectorBatch (long n, const idx_t *indices);
    bool is_member(idx_t id) const override;
    ~IDSelectorBatch() override {}
};


// Below are structures used only by Index implementations



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

    // create a new buffer
    void append_buffer ();

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
    idx_t qno;
    size_t nres;
    RangeSearchPartialResult * pres;

    void add (float dis, idx_t id);
};

/// the entries in the buffers are split per query
struct RangeSearchPartialResult: BufferList {
    RangeSearchResult * res;

    explicit RangeSearchPartialResult (RangeSearchResult * res_in);

    std::vector<RangeQueryResult> queries;

    /// begin a new result
    RangeQueryResult & new_result (idx_t qno);

    void finalize ();

    /// called by range_search before do_allocation
    void set_lims ();

    /// called by range_search after do_allocation
    void set_result (bool incremental = false);

};

/***********************************************************
 * Abstract I/O objects
 ***********************************************************/

struct IOReader {
    // name that can be used in error messages
    std::string name;

    // fread
    virtual size_t operator()(
         void *ptr, size_t size, size_t nitems) = 0;

    // return a file number that can be memory-mapped
    virtual int fileno ();

    virtual ~IOReader() {}
};

struct IOWriter {
    // name that can be used in error messages
    std::string name;

    // fwrite
    virtual size_t operator()(
         const void *ptr, size_t size, size_t nitems) = 0;

    // return a file number that can be memory-mapped
    virtual int fileno ();

    virtual ~IOWriter() {}
};


struct VectorIOReader:IOReader {
    std::vector<uint8_t> data;
    size_t rp = 0;
    size_t operator()(void *ptr, size_t size, size_t nitems) override;
};

struct VectorIOWriter:IOWriter {
    std::vector<uint8_t> data;
    size_t operator()(const void *ptr, size_t size, size_t nitems) override;
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

    static std::unique_ptr<InterruptCallback> instance;

    /** check if:
     * - an interrupt callback is set
     * - the callback retuns true
     * if this is the case, then throw an exception
     */
    static void check ();

    /// same as check() but return true if is interrupted instead of
    /// throwing
    static bool is_interrupted ();

    /** assuming each iteration takes a certain number of flops, what
     * is a reasonable interval to check for interrupts?
     */
    static size_t get_period_hint (size_t flops);

};



}; // namespace faiss



#endif
