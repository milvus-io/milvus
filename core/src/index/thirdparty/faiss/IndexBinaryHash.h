/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_BINARY_HASH_H
#define FAISS_BINARY_HASH_H



#include <vector>
#include <unordered_map>

#include <faiss/IndexBinary.h>
#include <faiss/IndexBinaryFlat.h>
#include <faiss/utils/Heap.h>


namespace faiss {

struct RangeSearchResult;


/** just uses the b first bits as a hash value */
struct IndexBinaryHash : IndexBinary {

    struct InvertedList {
        std::vector<idx_t> ids;
        std::vector<uint8_t> vecs;

        void add (idx_t id, size_t code_size, const uint8_t *code);
    };

    using InvertedListMap = std::unordered_map<idx_t, InvertedList>;
    InvertedListMap invlists;

    int b, nflip;

    IndexBinaryHash(int d, int b);

    IndexBinaryHash();

    void reset() override;

    void add(idx_t n, const uint8_t *x) override;

    void add_with_ids(idx_t n, const uint8_t *x, const idx_t *xids) override;

    void range_search(idx_t n, const uint8_t *x, int radius,
                      RangeSearchResult *result) const override;

    void search(idx_t n, const uint8_t *x, idx_t k,
                int32_t *distances, idx_t *labels) const override;

    void display() const;
    size_t hashtable_size() const;

};

struct IndexBinaryHashStats {
    size_t nq;       // nb of queries run
    size_t n0;       // nb of empty lists
    size_t nlist;    // nb of non-empty inverted lists scanned
    size_t ndis;     // nb of distancs computed

    IndexBinaryHashStats () {reset (); }
    void reset ();
};

extern IndexBinaryHashStats indexBinaryHash_stats;


/** just uses the b first bits as a hash value */
struct IndexBinaryMultiHash: IndexBinary {

    // where the vectors are actually stored
    IndexBinaryFlat *storage;
    bool own_fields;

    // maps hash values to the ids that hash to them
    using Map = std::unordered_map<idx_t, std::vector<idx_t> >;

    // the different hashes, size nhash
    std::vector<Map> maps;

    int nhash; ///< nb of hash maps
    int b; ///< nb bits per hash map
    int nflip; ///< nb bit flips to use at search time

    IndexBinaryMultiHash(int d, int nhash, int b);

    IndexBinaryMultiHash();

    ~IndexBinaryMultiHash();

    void reset() override;

    void add(idx_t n, const uint8_t *x) override;

    void range_search(idx_t n, const uint8_t *x, int radius,
                      RangeSearchResult *result) const override;

     void search(idx_t n, const uint8_t *x, idx_t k,
                int32_t *distances, idx_t *labels) const override;

    size_t hashtable_size() const;

};

}

#endif
