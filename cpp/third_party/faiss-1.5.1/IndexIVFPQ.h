/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_INDEX_IVFPQ_H
#define FAISS_INDEX_IVFPQ_H


#include <vector>

#include "IndexIVF.h"
#include "IndexPQ.h"


namespace faiss {

struct IVFPQSearchParameters: IVFSearchParameters {
    size_t scan_table_threshold;   ///< use table computation or on-the-fly?
    int polysemous_ht;             ///< Hamming thresh for polysemous filtering
    ~IVFPQSearchParameters () {}
};




/** Inverted file with Product Quantizer encoding. Each residual
 * vector is encoded as a product quantizer code.
 */
struct IndexIVFPQ: IndexIVF {
    bool by_residual;              ///< Encode residual or plain vector?

    ProductQuantizer pq;           ///< produces the codes

    bool do_polysemous_training;   ///< reorder PQ centroids after training?
    PolysemousTraining *polysemous_training; ///< if NULL, use default

    // search-time parameters
    size_t scan_table_threshold;   ///< use table computation or on-the-fly?
    int polysemous_ht;             ///< Hamming thresh for polysemous filtering

    /** Precompute table that speed up query preprocessing at some
     * memory cost
     * =-1: force disable
     * =0: decide heuristically (default: use tables only if they are
     *     < precomputed_tables_max_bytes)
     * =1: tables that work for all quantizers (size 256 * nlist * M)
     * =2: specific version for MultiIndexQuantizer (much more compact)
     */
    int use_precomputed_table;     ///< if by_residual, build precompute tables
    static size_t precomputed_table_max_bytes;

    /// if use_precompute_table
    /// size nlist * pq.M * pq.ksub
    std::vector <float> precomputed_table;

    IndexIVFPQ (
            Index * quantizer, size_t d, size_t nlist,
            size_t M, size_t nbits_per_idx);

    void add_with_ids(idx_t n, const float* x, const long* xids = nullptr)
        override;

    void encode_vectors(idx_t n, const float* x,
                        const idx_t *list_nos,
                        uint8_t * codes) const override;

    /// same as add_core, also:
    /// - output 2nd level residuals if residuals_2 != NULL
    /// - use precomputed list numbers if precomputed_idx != NULL
    void add_core_o (idx_t n, const float *x,
                     const long *xids, float *residuals_2,
                     const long *precomputed_idx = nullptr);

    /// trains the product quantizer
    void train_residual(idx_t n, const float* x) override;

    /// same as train_residual, also output 2nd level residuals
    void train_residual_o (idx_t n, const float *x, float *residuals_2);

    void reconstruct_from_offset (long list_no, long offset,
                                  float* recons) const override;

    /** Find exact duplicates in the dataset.
     *
     * the duplicates are returned in pre-allocated arrays (see the
     * max sizes).
     *
     * @params lims   limits between groups of duplicates
     *                (max size ntotal / 2 + 1)
     * @params ids    ids[lims[i]] : ids[lims[i+1]-1] is a group of
     *                duplicates (max size ntotal)
     * @return n      number of groups found
     */
    size_t find_duplicates (idx_t *ids, size_t *lims) const;

    // map a vector to a binary code knowning the index
    void encode (long key, const float * x, uint8_t * code) const;

    /** Encode multiple vectors
     *
     * @param n       nb vectors to encode
     * @param keys    posting list ids for those vectors (size n)
     * @param x       vectors (size n * d)
     * @param codes   output codes (size n * code_size)
     * @param compute_keys  if false, assume keys are precomputed,
     *                      otherwise compute them
     */
    void encode_multiple (size_t n, long *keys,
                          const float * x, uint8_t * codes,
                          bool compute_keys = false) const;

    /// inverse of encode_multiple
    void decode_multiple (size_t n, const long *keys,
                          const uint8_t * xcodes, float * x) const;

    InvertedListScanner *get_InvertedListScanner (bool store_pairs)
        const override;

    /// build precomputed table
    void precompute_table ();

    IndexIVFPQ ();

};


/// statistics are robust to internal threading, but not if
/// IndexIVFPQ::search_preassigned is called by multiple threads
struct IndexIVFPQStats {
    size_t nrefine;  // nb of refines (IVFPQR)

    size_t n_hamming_pass;
    // nb of passed Hamming distance tests (for polysemous)

    // timings measured with the CPU RTC
    // on all threads
    size_t search_cycles;
    size_t refine_cycles; // only for IVFPQR

    IndexIVFPQStats () {reset (); }
    void reset ();
};

// global var that collects them all
extern IndexIVFPQStats indexIVFPQ_stats;



/** Index with an additional level of PQ refinement */
struct IndexIVFPQR: IndexIVFPQ {
    ProductQuantizer refine_pq;           ///< 3rd level quantizer
    std::vector <uint8_t> refine_codes;   ///< corresponding codes

    /// factor between k requested in search and the k requested from the IVFPQ
    float k_factor;

    IndexIVFPQR (
            Index * quantizer, size_t d, size_t nlist,
            size_t M, size_t nbits_per_idx,
            size_t M_refine, size_t nbits_per_idx_refine);

    void reset() override;

    long remove_ids(const IDSelector& sel) override;

    /// trains the two product quantizers
    void train_residual(idx_t n, const float* x) override;

    void add_with_ids(idx_t n, const float* x, const long* xids) override;

    /// same as add_with_ids, but optionally use the precomputed list ids
    void add_core (idx_t n, const float *x, const long *xids,
                     const long *precomputed_idx = nullptr);

    void reconstruct_from_offset (long list_no, long offset,
                                  float* recons) const override;

    void merge_from (IndexIVF &other, idx_t add_id) override;


    void search_preassigned (idx_t n, const float *x, idx_t k,
                             const idx_t *assign,
                             const float *centroid_dis,
                             float *distances, idx_t *labels,
                             bool store_pairs,
                             const IVFSearchParameters *params=nullptr
                             ) const override;

    IndexIVFPQR();
};



/** Same as an IndexIVFPQ without the inverted lists: codes are stored sequentially
 *
 * The class is mainly inteded to store encoded vectors that can be
 * accessed randomly, the search function is not implemented.
 */
struct Index2Layer: Index {
    /// first level quantizer
    Level1Quantizer q1;

    /// second level quantizer is always a PQ
    ProductQuantizer pq;

    /// Codes. Size ntotal * code_size.
    std::vector<uint8_t> codes;

    /// size of the code for the first level (ceil(log8(q1.nlist)))
    size_t code_size_1;

    /// size of the code for the second level
    size_t code_size_2;

    /// code_size_1 + code_size_2
    size_t code_size;

    Index2Layer (Index * quantizer, size_t nlist,
                 int M, MetricType metric = METRIC_L2);

    Index2Layer ();
    ~Index2Layer ();

    void train(idx_t n, const float* x) override;

    void add(idx_t n, const float* x) override;

    /// not implemented
    void search(
        idx_t n,
        const float* x,
        idx_t k,
        float* distances,
        idx_t* labels) const override;

    void reconstruct_n(idx_t i0, idx_t ni, float* recons) const override;

    void reconstruct(idx_t key, float* recons) const override;

    void reset() override;

    /// transfer the flat codes to an IVFPQ index
    void transfer_to_IVFPQ(IndexIVFPQ & other) const;

};


} // namespace faiss


#endif
