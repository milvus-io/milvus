/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_INDEX_BINARY_IVF_H
#define FAISS_INDEX_BINARY_IVF_H


#include <vector>

#include <faiss/IndexBinary.h>
#include <faiss/IndexIVF.h>
#include <faiss/Clustering.h>
#include <faiss/utils/Heap.h>


namespace faiss {

struct BinaryInvertedListScanner;

/** Index based on a inverted file (IVF)
 *
 * In the inverted file, the quantizer (an IndexBinary instance) provides a
 * quantization index for each vector to be added. The quantization
 * index maps to a list (aka inverted list or posting list), where the
 * id of the vector is stored.
 *
 * Otherwise the object is similar to the IndexIVF
 */
struct IndexBinaryIVF : IndexBinary {
    /// Acess to the actual data
    InvertedLists *invlists;
    bool own_invlists;

    size_t nprobe;            ///< number of probes at query time
    size_t max_codes;         ///< max nb of codes to visit to do a query

    /** Select between using a heap or counting to select the k smallest values
     * when scanning inverted lists.
     */
    bool use_heap = true;

    /// map for direct access to the elements. Enables reconstruct().
    DirectMap direct_map;

    IndexBinary *quantizer;   ///< quantizer that maps vectors to inverted lists
    size_t nlist;             ///< number of possible key values

    bool own_fields;          ///< whether object owns the quantizer

    ClusteringParameters cp; ///< to override default clustering params
    Index *clustering_index; ///< to override index used during clustering

    /** The Inverted file takes a quantizer (an IndexBinary) on input,
     * which implements the function mapping a vector to a list
     * identifier. The pointer is borrowed: the quantizer should not
     * be deleted while the IndexBinaryIVF is in use.
     */
    IndexBinaryIVF(IndexBinary *quantizer, size_t d, size_t nlist);

    IndexBinaryIVF();

    ~IndexBinaryIVF() override;

    void reset() override;

    /// Trains the quantizer
    void train(idx_t n, const uint8_t *x) override;

    void add(idx_t n, const uint8_t *x) override;

    void add_with_ids(idx_t n, const uint8_t *x, const idx_t *xids) override;

    /// same as add_with_ids, with precomputed coarse quantizer
    void add_core (idx_t n, const uint8_t * x, const idx_t *xids,
                   const idx_t *precomputed_idx);

    /** Search a set of vectors, that are pre-quantized by the IVF
     *  quantizer. Fill in the corresponding heaps with the query
     *  results. search() calls this.
     *
     * @param n      nb of vectors to query
     * @param x      query vectors, size nx * d
     * @param assign coarse quantization indices, size nx * nprobe
     * @param centroid_dis
     *               distances to coarse centroids, size nx * nprobe
     * @param distance
     *               output distances, size n * k
     * @param labels output labels, size n * k
     * @param store_pairs store inv list index + inv list offset
     *                     instead in upper/lower 32 bit of result,
     *                     instead of ids (used for reranking).
     * @param params used to override the object's search parameters
     */
    void search_preassigned(idx_t n, const uint8_t *x, idx_t k,
                            const idx_t *assign,
                            const int32_t *centroid_dis,
                            int32_t *distances, idx_t *labels,
                            bool store_pairs,
                            const IVFSearchParameters *params=nullptr
                            ) const;

    virtual BinaryInvertedListScanner *get_InvertedListScanner (
                                         bool store_pairs=false) const;

    /** assign the vectors, then call search_preassign */
    void search(idx_t n, const uint8_t *x, idx_t k,
                int32_t *distances, idx_t *labels) const override;

    void range_search(idx_t n, const uint8_t *x, int radius,
                      RangeSearchResult *result) const override;

    void reconstruct(idx_t key, uint8_t *recons) const override;

    /** Reconstruct a subset of the indexed vectors.
     *
     * Overrides default implementation to bypass reconstruct() which requires
     * direct_map to be maintained.
     *
     * @param i0     first vector to reconstruct
     * @param ni     nb of vectors to reconstruct
     * @param recons output array of reconstructed vectors, size ni * d / 8
     */
    void reconstruct_n(idx_t i0, idx_t ni, uint8_t *recons) const override;

    /** Similar to search, but also reconstructs the stored vectors (or an
     * approximation in the case of lossy coding) for the search results.
     *
     * Overrides default implementation to avoid having to maintain direct_map
     * and instead fetch the code offsets through the `store_pairs` flag in
     * search_preassigned().
     *
     * @param recons      reconstructed vectors size (n, k, d / 8)
     */
    void search_and_reconstruct(idx_t n, const uint8_t *x, idx_t k,
                                int32_t *distances, idx_t *labels,
                                uint8_t *recons) const override;

    /** Reconstruct a vector given the location in terms of (inv list index +
     * inv list offset) instead of the id.
     *
     * Useful for reconstructing when the direct_map is not maintained and
     * the inv list offset is computed by search_preassigned() with
     * `store_pairs` set.
     */
    virtual void reconstruct_from_offset(idx_t list_no, idx_t offset,
                                         uint8_t* recons) const;


    /// Dataset manipulation functions
    size_t remove_ids(const IDSelector& sel) override;

    /** moves the entries from another dataset to self. On output,
     * other is empty. add_id is added to all moved ids (for
     * sequential ids, this would be this->ntotal */
    virtual void merge_from(IndexBinaryIVF& other, idx_t add_id);

    size_t get_list_size(size_t list_no) const
    { return invlists->list_size(list_no); }

    /** intialize a direct map
     *
     * @param new_maintain_direct_map    if true, create a direct map,
     *                                   else clear it
     */
    void make_direct_map(bool new_maintain_direct_map=true);

    void set_direct_map_type (DirectMap::Type type);

    void replace_invlists(InvertedLists *il, bool own=false);
};


struct BinaryInvertedListScanner {

    using idx_t = Index::idx_t;

    /// from now on we handle this query.
    virtual void set_query (const uint8_t *query_vector) = 0;

    /// following codes come from this inverted list
    virtual void set_list (idx_t list_no, uint8_t coarse_dis) = 0;

    /// compute a single query-to-code distance
    virtual uint32_t distance_to_code (const uint8_t *code) const = 0;

    /** compute the distances to codes. (distances, labels) should be
     * organized as a min- or max-heap
     *
     * @param n      number of codes to scan
     * @param codes  codes to scan (n * code_size)
     * @param ids        corresponding ids (ignored if store_pairs)
     * @param distances  heap distances (size k)
     * @param labels     heap labels (size k)
     * @param k          heap size
     */
    virtual size_t scan_codes (size_t n,
                               const uint8_t *codes,
                               const idx_t *ids,
                               int32_t *distances, idx_t *labels,
                               size_t k) const = 0;

    virtual void scan_codes_range (size_t n,
                                   const uint8_t *codes,
                                   const idx_t *ids,
                                   int radius,
                                   RangeQueryResult &result) const = 0;

    virtual ~BinaryInvertedListScanner () {}

};


}  // namespace faiss

#endif  // FAISS_INDEX_BINARY_IVF_H
