/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// Copyright 2004-present Facebook. All Rights Reserved.
// -*- c++ -*-

#include "Clustering_c.h"
#include "Clustering.h"
#include "Index.h"
#include <vector>
#include "macros_impl.h"

extern "C" {

using faiss::Clustering;
using faiss::ClusteringParameters;
using faiss::Index;
using faiss::ClusteringIterationStats;

DEFINE_GETTER(Clustering, int, niter)
DEFINE_GETTER(Clustering, int, nredo)
DEFINE_GETTER(Clustering, int, verbose)
DEFINE_GETTER(Clustering, int, spherical)
DEFINE_GETTER(Clustering, int, update_index)
DEFINE_GETTER(Clustering, int, frozen_centroids)

DEFINE_GETTER(Clustering, int, min_points_per_centroid)
DEFINE_GETTER(Clustering, int, max_points_per_centroid)

DEFINE_GETTER(Clustering, int, seed)

/// getter for d
DEFINE_GETTER(Clustering, size_t, d)

/// getter for k
DEFINE_GETTER(Clustering, size_t, k)

DEFINE_GETTER(ClusteringIterationStats, float, obj)
DEFINE_GETTER(ClusteringIterationStats, double, time)
DEFINE_GETTER(ClusteringIterationStats, double, time_search)
DEFINE_GETTER(ClusteringIterationStats, double, imbalance_factor)
DEFINE_GETTER(ClusteringIterationStats, int, nsplit)

void faiss_ClusteringParameters_init(FaissClusteringParameters* params) {
    ClusteringParameters d;
    params->frozen_centroids = d.frozen_centroids;
    params->max_points_per_centroid = d.max_points_per_centroid;
    params->min_points_per_centroid = d.min_points_per_centroid;
    params->niter = d.niter;
    params->nredo = d.nredo;
    params->seed = d.seed;
    params->spherical = d.spherical;
    params->update_index = d.update_index;
    params->verbose = d.verbose;   
}

// This conversion is required because the two types are not memory-compatible
inline ClusteringParameters from_faiss_c(const FaissClusteringParameters* params) {
    ClusteringParameters o;
    o.frozen_centroids = params->frozen_centroids;
    o.max_points_per_centroid = params->max_points_per_centroid;
    o.min_points_per_centroid = params->min_points_per_centroid;
    o.niter = params->niter;
    o.nredo = params->nredo;
    o.seed = params->seed;
    o.spherical = params->spherical;
    o.update_index = params->update_index;
    o.verbose = params->verbose;
    return o;
}

/// getter for centroids (size = k * d)
void faiss_Clustering_centroids(
    FaissClustering* clustering, float** centroids, size_t* size) {
    std::vector<float>& v = reinterpret_cast<Clustering*>(clustering)->centroids;
    if (centroids) {
        *centroids = v.data();
    }
    if (size) {
        *size = v.size();
    }
}

/// getter for iteration stats
void faiss_Clustering_iteration_stats(
    FaissClustering* clustering, FaissClusteringIterationStats** iteration_stats, size_t* size) {
    std::vector<ClusteringIterationStats>& v = reinterpret_cast<Clustering*>(clustering)->iteration_stats;
    if (iteration_stats) {
        *iteration_stats = reinterpret_cast<FaissClusteringIterationStats*>(v.data());
    }
    if (size) {
        *size = v.size();
    }
}

/// the only mandatory parameters are k and d
int faiss_Clustering_new(FaissClustering** p_clustering, int d, int k) {
    try {
        Clustering* c = new Clustering(d, k);
        *p_clustering = reinterpret_cast<FaissClustering*>(c);
        return 0;
    } CATCH_AND_HANDLE
}

int faiss_Clustering_new_with_params(
    FaissClustering** p_clustering, int d, int k, const FaissClusteringParameters* cp) {
    try {
        Clustering* c = new Clustering(d, k, from_faiss_c(cp));
        *p_clustering = reinterpret_cast<FaissClustering*>(c);
        return 0;
    } CATCH_AND_HANDLE
}

/// Index is used during the assignment stage
int faiss_Clustering_train(
    FaissClustering* clustering, idx_t n, const float* x, FaissIndex* index) {
    try {
        reinterpret_cast<Clustering*>(clustering)->train(
            n, x, *reinterpret_cast<Index*>(index));
        return 0;
    } CATCH_AND_HANDLE
}

void faiss_Clustering_free(FaissClustering* clustering) {
    delete reinterpret_cast<Clustering*>(clustering);
}

int faiss_kmeans_clustering (size_t d, size_t n, size_t k,
                             const float *x,
                             float *centroids,
                             float *q_error) {
    try {
        float out = faiss::kmeans_clustering(d, n, k, x, centroids);
        if (q_error) {
            *q_error = out;
        }
        return 0;
    } CATCH_AND_HANDLE
}

}
