/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include "Clustering.h"
#include "AuxIndexStructures.h"


#include <cmath>
#include <cstdio>
#include <cstring>

#include "utils.h"
#include "FaissAssert.h"
#include "IndexFlat.h"

namespace faiss {

ClusteringParameters::ClusteringParameters ():
    niter(25),
    nredo(1),
    verbose(false),
    spherical(false),
    int_centroids(false),
    update_index(false),
    frozen_centroids(false),
    min_points_per_centroid(39),
    max_points_per_centroid(256),
    seed(1234)
{}
// 39 corresponds to 10000 / 256 -> to avoid warnings on PQ tests with randu10k


Clustering::Clustering (int d, int k):
    d(d), k(k) {}

Clustering::Clustering (int d, int k, const ClusteringParameters &cp):
    ClusteringParameters (cp), d(d), k(k) {}



static double imbalance_factor (int n, int k, long *assign) {
    std::vector<int> hist(k, 0);
    for (int i = 0; i < n; i++)
        hist[assign[i]]++;

    double tot = 0, uf = 0;

    for (int i = 0 ; i < k ; i++) {
        tot += hist[i];
        uf += hist[i] * (double) hist[i];
    }
    uf = uf * k / (tot * tot);

    return uf;
}

void Clustering::post_process_centroids ()
{

    if (spherical) {
        fvec_renorm_L2 (d, k, centroids.data());
    }

    if (int_centroids) {
        for (size_t i = 0; i < centroids.size(); i++)
            centroids[i] = roundf (centroids[i]);
    }
}


void Clustering::train (idx_t nx, const float *x_in, Index & index) {
    FAISS_THROW_IF_NOT_FMT (nx >= k,
             "Number of training points (%ld) should be at least "
             "as large as number of clusters (%ld)", nx, k);

    double t0 = getmillisecs();

    // yes it is the user's responsibility, but it may spare us some
    // hard-to-debug reports.
    for (size_t i = 0; i < nx * d; i++) {
      FAISS_THROW_IF_NOT_MSG (finite (x_in[i]),
                        "input contains NaN's or Inf's");
    }

    const float *x = x_in;
    ScopeDeleter<float> del1;

    if (nx > k * max_points_per_centroid) {
        if (verbose)
            printf("Sampling a subset of %ld / %ld for training\n",
                   k * max_points_per_centroid, nx);
        std::vector<int> perm (nx);
        rand_perm (perm.data (), nx, seed);
        nx = k * max_points_per_centroid;
        float * x_new = new float [nx * d];
        for (idx_t i = 0; i < nx; i++)
            memcpy (x_new + i * d, x + perm[i] * d, sizeof(x_new[0]) * d);
        x = x_new;
        del1.set (x);
    } else if (nx < k * min_points_per_centroid) {
        fprintf (stderr,
                 "WARNING clustering %ld points to %ld centroids: "
                 "please provide at least %ld training points\n",
                 nx, k, idx_t(k) * min_points_per_centroid);
    }


    if (nx == k) {
        if (verbose) {
            printf("Number of training points (%ld) same as number of "
                   "clusters, just copying\n", nx);
        }
        // this is a corner case, just copy training set to clusters
        centroids.resize (d * k);
        memcpy (centroids.data(), x_in, sizeof (*x_in) * d * k);
        index.reset();
        index.add(k, x_in);
        return;
    }


    if (verbose)
        printf("Clustering %d points in %ldD to %ld clusters, "
               "redo %d times, %d iterations\n",
               int(nx), d, k, nredo, niter);

    idx_t * assign = new idx_t[nx];
    ScopeDeleter<idx_t> del (assign);
    float * dis = new float[nx];
    ScopeDeleter<float> del2(dis);

    // for redo
    float best_err = HUGE_VALF;
    std::vector<float> best_obj;
    std::vector<float> best_centroids;

    // support input centroids

    FAISS_THROW_IF_NOT_MSG (
       centroids.size() % d == 0,
       "size of provided input centroids not a multiple of dimension");

    size_t n_input_centroids = centroids.size() / d;

    if (verbose && n_input_centroids > 0) {
        printf ("  Using %zd centroids provided as input (%sfrozen)\n",
                n_input_centroids, frozen_centroids ? "" : "not ");
    }

    double t_search_tot = 0;
    if (verbose) {
        printf("  Preprocessing in %.2f s\n",
               (getmillisecs() - t0) / 1000.);
    }
    t0 = getmillisecs();

    for (int redo = 0; redo < nredo; redo++) {

        if (verbose && nredo > 1) {
            printf("Outer iteration %d / %d\n", redo, nredo);
        }

        // initialize remaining centroids with random points from the dataset
        centroids.resize (d * k);
        std::vector<int> perm (nx);

        rand_perm (perm.data(), nx, seed + 1 + redo * 15486557L);
        for (int i = n_input_centroids; i < k ; i++)
            memcpy (&centroids[i * d], x + perm[i] * d,
                    d * sizeof (float));

        post_process_centroids ();

        if (index.ntotal != 0) {
            index.reset();
        }

        if (!index.is_trained) {
            index.train (k, centroids.data());
        }

        index.add (k, centroids.data());
        float err = 0;
        for (int i = 0; i < niter; i++) {
            double t0s = getmillisecs();
            index.search (nx, x, 1, dis, assign);
            InterruptCallback::check();
            t_search_tot += getmillisecs() - t0s;

            err = 0;
            for (int j = 0; j < nx; j++)
                err += dis[j];
            obj.push_back (err);

            int nsplit = km_update_centroids (
                  x, centroids.data(),
                  assign, d, k, nx, frozen_centroids ? n_input_centroids : 0);

            if (verbose) {
                printf ("  Iteration %d (%.2f s, search %.2f s): "
                        "objective=%g imbalance=%.3f nsplit=%d       \r",
                        i, (getmillisecs() - t0) / 1000.0,
                        t_search_tot / 1000,
                        err, imbalance_factor (nx, k, assign),
                        nsplit);
                fflush (stdout);
            }

            post_process_centroids ();

            index.reset ();
            if (update_index)
                index.train (k, centroids.data());

            assert (index.ntotal == 0);
            index.add (k, centroids.data());
            InterruptCallback::check ();
        }
        if (verbose) printf("\n");
        if (nredo > 1) {
            if (err < best_err) {
                if (verbose)
                    printf ("Objective improved: keep new clusters\n");
                best_centroids = centroids;
                best_obj = obj;
                best_err = err;
            }
            index.reset ();
        }
    }
    if (nredo > 1) {
        centroids = best_centroids;
        obj = best_obj;
        index.reset();
        index.add(k, best_centroids.data());
    }

}

float kmeans_clustering (size_t d, size_t n, size_t k,
                         const float *x,
                         float *centroids)
{
    Clustering clus (d, k);
    clus.verbose = d * n * k > (1L << 30);
    // display logs if > 1Gflop per iteration
    IndexFlatL2 index (d);
    clus.train (n, x, index);
    memcpy(centroids, clus.centroids.data(), sizeof(*centroids) * d * k);
    return clus.obj.back();
}

} // namespace faiss
