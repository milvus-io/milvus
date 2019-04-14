/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved
// -*- c -*-

#ifndef FAISS_CLUSTERING_C_H
#define FAISS_CLUSTERING_C_H

#include "Index_c.h"
#include "faiss_c.h"

#ifdef __cplusplus
extern "C" {
#endif

/** Class for the clustering parameters. Can be passed to the
 * constructor of the Clustering object.
 */
typedef struct FaissClusteringParameters {
    int niter;          ///< clustering iterations
    int nredo;          ///< redo clustering this many times and keep best

    int verbose;       ///< (bool)
    int spherical;     ///< (bool) do we want normalized centroids?
    int update_index;  ///< (bool) update index after each iteration?
    int frozen_centroids;  ///< (bool) use the centroids provided as input and do not change them during iterations

    int min_points_per_centroid; ///< otherwise you get a warning
    int max_points_per_centroid;  ///< to limit size of dataset

    int seed; ///< seed for the random number generator
} FaissClusteringParameters;


/// Sets the ClusteringParameters object with reasonable defaults
void faiss_ClusteringParameters_init(FaissClusteringParameters* params);


/** clustering based on assignment - centroid update iterations
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
 * To do several clusterings, just call train() several times on
 * different training sets, clearing the centroid table in between.
 */
FAISS_DECLARE_CLASS(Clustering)

FAISS_DECLARE_GETTER(Clustering, int, niter)
FAISS_DECLARE_GETTER(Clustering, int, nredo)
FAISS_DECLARE_GETTER(Clustering, int, verbose)
FAISS_DECLARE_GETTER(Clustering, int, spherical)
FAISS_DECLARE_GETTER(Clustering, int, update_index)
FAISS_DECLARE_GETTER(Clustering, int, frozen_centroids)

FAISS_DECLARE_GETTER(Clustering, int, min_points_per_centroid)
FAISS_DECLARE_GETTER(Clustering, int, max_points_per_centroid)

FAISS_DECLARE_GETTER(Clustering, int, seed)

/// getter for d
FAISS_DECLARE_GETTER(Clustering, size_t, d)

/// getter for k
FAISS_DECLARE_GETTER(Clustering, size_t, k)

/// getter for centroids (size = k * d)
void faiss_Clustering_centroids(
    FaissClustering* clustering, float** centroids, size_t* size);

/// getter for objective values (sum of distances reported by index)
/// over iterations
void faiss_Clustering_obj(
    FaissClustering* clustering, float** obj, size_t* size);

/// the only mandatory parameters are k and d
int faiss_Clustering_new(FaissClustering** p_clustering, int d, int k);

int faiss_Clustering_new_with_params(
    FaissClustering** p_clustering, int d, int k, const FaissClusteringParameters* cp);

int faiss_Clustering_train(
    FaissClustering* clustering, idx_t n, const float* x, FaissIndex* index);

void faiss_Clustering_free(FaissClustering* clustering);

/** simplified interface
 *
 * @param d dimension of the data
 * @param n nb of training vectors
 * @param k nb of output centroids
 * @param x training set (size n * d)
 * @param centroids output centroids (size k * d)
 * @param q_error final quantization error
 * @return error code
 */
int faiss_kmeans_clustering (size_t d, size_t n, size_t k,
                       const float *x,
                       float *centroids,
                       float *q_error);

#ifdef __cplusplus
}
#endif

#endif
