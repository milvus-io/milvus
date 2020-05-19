/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_INDEX_BINARY_H
#define FAISS_INDEX_BINARY_H

#include <cstdio>
#include <typeinfo>
#include <string>
#include <sstream>

#include <faiss/impl/FaissAssert.h>
#include <faiss/Index.h>


namespace faiss {


/// Forward declarations see AuxIndexStructures.h
struct IDSelector;
struct RangeSearchResult;

/** Abstract structure for a binary index.
 *
 * Supports adding vertices and searching them.
 *
 * All queries are symmetric because there is no distinction between codes and
 * vectors.
 */
struct IndexBinary {
  using idx_t = Index::idx_t;    ///< all indices are this type
  using component_t = uint8_t;
  using distance_t = int32_t;

  int d;                 ///< vector dimension
  int code_size;   ///< number of bytes per vector ( = d / 8 )
  idx_t ntotal;          ///< total nb of indexed vectors
  bool verbose;          ///< verbosity level

  /// set if the Index does not require training, or if training is done already
  bool is_trained;

  /// type of metric this index uses for search
  MetricType metric_type;

  explicit IndexBinary(idx_t d = 0, MetricType metric = METRIC_L2)
      : d(d),
        code_size(d / 8),
        ntotal(0),
        verbose(false),
        is_trained(true),
        metric_type(metric) {
        FAISS_THROW_IF_NOT(d % 8 == 0);
      }

  virtual ~IndexBinary();


  /** Perform training on a representative set of vectors.
   *
   * @param n      nb of training vectors
   * @param x      training vecors, size n * d / 8
   */
  virtual void train(idx_t n, const uint8_t *x);

  /** Add n vectors of dimension d to the index.
   *
   * Vectors are implicitly assigned labels ntotal .. ntotal + n - 1
   * @param x      input matrix, size n * d / 8
   */
  virtual void add(idx_t n, const uint8_t *x) = 0;

  /** Same as add, but stores xids instead of sequential ids.
   *
   * The default implementation fails with an assertion, as it is
   * not supported by all indexes.
   *
   * @param xids if non-null, ids to store for the vectors (size n)
   */
  virtual void add_with_ids(idx_t n, const uint8_t *x, const idx_t *xids);

  /** Query n vectors of dimension d to the index.
   *
   * return at most k vectors. If there are not enough results for a
   * query, the result array is padded with -1s.
   *
   * @param x           input vectors to search, size n * d / 8
   * @param labels      output labels of the NNs, size n*k
   * @param distances   output pairwise distances, size n*k
   */
  virtual void search(idx_t n, const uint8_t *x, idx_t k,
                      int32_t *distances, idx_t *labels) const = 0;

  /** Query n vectors of dimension d to the index.
   *
   * return all vectors with distance < radius. Note that many indexes
   * do not implement the range_search (only the k-NN search is
   * mandatory). The distances are converted to float to reuse the
   * RangeSearchResult structure, but they are integer. By convention,
   * only distances < radius (strict comparison) are returned,
   * ie. radius = 0 does not return any result and 1 returns only
   * exact same vectors.
   *
   * @param x           input vectors to search, size n * d / 8
   * @param radius      search radius
   * @param result      result table
   */
  virtual void range_search(idx_t n, const uint8_t *x, int radius,
                            RangeSearchResult *result) const;

  /** Return the indexes of the k vectors closest to the query x.
   *
   * This function is identical to search but only returns labels of neighbors.
   * @param x           input vectors to search, size n * d / 8
   * @param labels      output labels of the NNs, size n*k
   */
  void assign(idx_t n, const uint8_t *x, idx_t *labels, idx_t k = 1);

  /// Removes all elements from the database.
  virtual void reset() = 0;

  /** Removes IDs from the index. Not supported by all indexes.
   */
  virtual size_t remove_ids(const IDSelector& sel);

  /** Reconstruct a stored vector.
   *
   * This function may not be defined for some indexes.
   * @param key         id of the vector to reconstruct
   * @param recons      reconstucted vector (size d / 8)
   */
  virtual void reconstruct(idx_t key, uint8_t *recons) const;


  /** Reconstruct vectors i0 to i0 + ni - 1.
   *
   * This function may not be defined for some indexes.
   * @param recons      reconstucted vectors (size ni * d / 8)
   */
  virtual void reconstruct_n(idx_t i0, idx_t ni, uint8_t *recons) const;

  /** Similar to search, but also reconstructs the stored vectors (or an
   * approximation in the case of lossy coding) for the search results.
   *
   * If there are not enough results for a query, the resulting array
   * is padded with -1s.
   *
   * @param recons      reconstructed vectors size (n, k, d)
   **/
  virtual void search_and_reconstruct(idx_t n, const uint8_t *x, idx_t k,
                                      int32_t *distances, idx_t *labels,
                                      uint8_t *recons) const;

  /** Display the actual class name and some more info. */
  void display() const;
};


}  // namespace faiss

#endif  // FAISS_INDEX_BINARY_H
