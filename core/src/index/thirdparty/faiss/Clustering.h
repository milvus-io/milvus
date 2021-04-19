/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_CLUSTERING_H
#define FAISS_CLUSTERING_H
#include <faiss/Index.h>

#include <vector>

namespace faiss {


/** Class for the clustering parameters. Can be passed to the
 * constructor of the Clustering object.
 */
struct ClusteringParameters {
    int niter;          ///< clustering iterations
    int nredo;          ///< redo clustering this many times and keep best

    bool verbose;
    bool spherical;     ///< do we want normalized centroids?
    bool int_centroids; ///< round centroids coordinates to integer
    bool update_index;  ///< re-train index after each iteration?
    bool frozen_centroids;  ///< use the centroids provided as input and do not change them during iterations

    int min_points_per_centroid; ///< otherwise you get a warning
    int max_points_per_centroid;  ///< to limit size of dataset

    int seed; ///< seed for the random number generator

    size_t decode_block_size;  ///< how many vectors at a time to decode

    /// sets reasonable defaults
    ClusteringParameters ();
};


struct ClusteringIterationStats {
    float obj;               ///< objective values (sum of distances reported by index)
    double time;             ///< seconds for iteration
    double time_search;      ///< seconds for just search
    double imbalance_factor; ///< imbalance factor of iteration
    int nsplit;              ///< number of cluster splits
};


/** K-means clustering based on assignment - centroid update iterations
 *
 * The clustering is based on an Index object that assigns training
 * points to the centroids. Therefore, at each iteration the centroids
 * are added to the index.
 *
 * On output, the centoids table is set to the latest version
 * of the centroids and they are also added to the index. If the
 * centroids table it is not empty on input, it is also used for
 * initialization.
 *
 */
struct Clustering: ClusteringParameters {
    typedef Index::idx_t idx_t;
    size_t d;              ///< dimension of the vectors
    size_t k;              ///< nb of centroids

    /** centroids (k * d)
     * if centroids are set on input to train, they will be used as initialization
     */
    std::vector<float> centroids;

    /// stats at every iteration of clustering
    std::vector<ClusteringIterationStats> iteration_stats;

    Clustering (int d, int k);
    Clustering (int d, int k, const ClusteringParameters &cp);

    /** run k-means training
     *
     * @param x          training vectors, size n * d
     * @param index      index used for assignment
     * @param x_weights  weight associated to each vector: NULL or size n
     */
    virtual void train (idx_t n, const float * x, faiss::Index & index,
                        const float *x_weights = nullptr);


    /** run with encoded vectors
     *
     * win addition to train()'s parameters takes a codec as parameter
     * to decode the input vectors.
     *
     * @param codec      codec used to decode the vectors (nullptr =
     *                   vectors are in fact floats)     *
     */
    void train_encoded (idx_t nx, const uint8_t *x_in,
                        const Index * codec, Index & index,
                        const float *weights = nullptr);

    /// Post-process the centroids after each centroid update.
    /// includes optional L2 normalization and nearest integer rounding
    void post_process_centroids ();

    virtual ~Clustering() {}
};


/** simplified interface
 *
 * @param d dimension of the data
 * @param n nb of training vectors
 * @param k nb of output centroids
 * @param x training set (size n * d)
 * @param centroids output centroids (size k * d)
 * @return final quantization error
 */
float kmeans_clustering (size_t d, size_t n, size_t k,
                         const float *x,
                         float *centroids);



}


#endif
