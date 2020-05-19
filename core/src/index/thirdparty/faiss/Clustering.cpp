/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/Clustering.h>
#include <faiss/impl/AuxIndexStructures.h>

#include <cmath>
#include <cstdio>
#include <cstring>

#include <omp.h>

#include <faiss/utils/utils.h>
#include <faiss/utils/random.h>
#include <faiss/utils/distances.h>
#include <faiss/impl/FaissAssert.h>
#include <faiss/IndexFlat.h>

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
    seed(1234),
    decode_block_size(32768)
{}
// 39 corresponds to 10000 / 256 -> to avoid warnings on PQ tests with randu10k


Clustering::Clustering (int d, int k):
    d(d), k(k) {}

Clustering::Clustering (int d, int k, const ClusteringParameters &cp):
    ClusteringParameters (cp), d(d), k(k) {}



static double imbalance_factor (int n, int k, int64_t *assign) {
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


void Clustering::train (idx_t nx, const float *x_in, Index & index,
                        const float *weights) {
    train_encoded (nx, reinterpret_cast<const uint8_t *>(x_in), nullptr,
                   index, weights);
}


namespace {

using idx_t = Clustering::idx_t;

idx_t subsample_training_set(
          const Clustering &clus, idx_t nx, const uint8_t *x,
          size_t line_size, const float * weights,
          uint8_t **x_out,
          float **weights_out
)
{
    if (clus.verbose) {
        printf("Sampling a subset of %ld / %ld for training\n",
               clus.k * clus.max_points_per_centroid, nx);
    }
    std::vector<int> perm (nx);
    rand_perm (perm.data (), nx, clus.seed);
    nx = clus.k * clus.max_points_per_centroid;
    uint8_t * x_new = new uint8_t [nx * line_size];
    *x_out = x_new;
    for (idx_t i = 0; i < nx; i++) {
        memcpy (x_new + i * line_size, x + perm[i] * line_size, line_size);
    }
    if (weights) {
        float *weights_new = new float[nx];
        for (idx_t i = 0; i < nx; i++) {
            weights_new[i] = weights[perm[i]];
        }
        *weights_out = weights_new;
    } else {
        *weights_out = nullptr;
    }
    return nx;
}

/** compute centroids as (weighted) sum of training points
 *
 * @param x            training vectors, size n * code_size (from codec)
 * @param codec        how to decode the vectors (if NULL then cast to float*)
 * @param weights      per-training vector weight, size n (or NULL)
 * @param assign       nearest centroid for each training vector, size n
 * @param k_frozen     do not update the k_frozen first centroids
 * @param centroids    centroid vectors (output only), size k * d
 * @param hassign      histogram of assignments per centroid (size k),
 *                     should be 0 on input
 *
 */

void compute_centroids (size_t d, size_t k, size_t n,
                       size_t k_frozen,
                       const uint8_t * x, const Index *codec,
                       const int64_t * assign,
                       const float * weights,
                       float * hassign,
                       float * centroids)
{
    k -= k_frozen;
    centroids += k_frozen * d;

    memset (centroids, 0, sizeof(*centroids) * d * k);

    size_t line_size = codec ? codec->sa_code_size() : d * sizeof (float);

#pragma omp parallel
    {
        int nt = omp_get_num_threads();
        int rank = omp_get_thread_num();

        // this thread is taking care of centroids c0:c1
        size_t c0 = (k * rank) / nt;
        size_t c1 = (k * (rank + 1)) / nt;
        std::vector<float> decode_buffer (d);

        for (size_t i = 0; i < n; i++) {
            int64_t ci = assign[i];
            assert (ci >= 0 && ci < k + k_frozen);
            ci -= k_frozen;
            if (ci >= c0 && ci < c1)  {
                float * c = centroids + ci * d;
                const float * xi;
                if (!codec) {
                    xi = reinterpret_cast<const float*>(x + i * line_size);
                } else {
                    float *xif = decode_buffer.data();
                    codec->sa_decode (1, x + i * line_size, xif);
                    xi = xif;
                }
                if (weights) {
                    float w = weights[i];
                    hassign[ci] += w;
                    for (size_t j = 0; j < d; j++) {
                        c[j] += xi[j] * w;
                    }
                } else {
                    hassign[ci] += 1.0;
                    for (size_t j = 0; j < d; j++) {
                        c[j] += xi[j];
                    }
                }
            }
        }

    }

#pragma omp parallel for
    for (size_t ci = 0; ci < k; ci++) {
        if (hassign[ci] == 0) {
            continue;
        }
        float norm = 1 / hassign[ci];
        float * c = centroids + ci * d;
        for (size_t j = 0; j < d; j++) {
            c[j] *= norm;
        }
    }

}

// a bit above machine epsilon for float16
#define EPS (1 / 1024.)

/** Handle empty clusters by splitting larger ones.
 *
 * It works by slightly changing the centroids to make 2 clusters from
 * a single one. Takes the same arguements as compute_centroids.
 *
 * @return           nb of spliting operations (larger is worse)
 */
int split_clusters (size_t d, size_t k, size_t n,
                    size_t k_frozen,
                    float * hassign,
                    float * centroids)
{
    k -= k_frozen;
    centroids += k_frozen * d;

    /* Take care of void clusters */
    size_t nsplit = 0;
    RandomGenerator rng (1234);
    for (size_t ci = 0; ci < k; ci++) {
        if (hassign[ci] == 0) { /* need to redefine a centroid */
            size_t cj;
            for (cj = 0; 1; cj = (cj + 1) % k) {
                /* probability to pick this cluster for split */
                float p = (hassign[cj] - 1.0) / (float) (n - k);
                float r = rng.rand_float ();
                if (r < p) {
                    break; /* found our cluster to be split */
                }
            }
            memcpy (centroids+ci*d, centroids+cj*d, sizeof(*centroids) * d);

            /* small symmetric pertubation */
            for (size_t j = 0; j < d; j++) {
                if (j % 2 == 0) {
                    centroids[ci * d + j] *= 1 + EPS;
                    centroids[cj * d + j] *= 1 - EPS;
                } else {
                    centroids[ci * d + j] *= 1 - EPS;
                    centroids[cj * d + j] *= 1 + EPS;
                }
            }

            /* assume even split of the cluster */
            hassign[ci] = hassign[cj] / 2;
            hassign[cj] -= hassign[ci];
            nsplit++;
        }
    }

    return nsplit;

}



};


void Clustering::train_encoded (idx_t nx, const uint8_t *x_in,
                                const Index * codec, Index & index,
                                const float *weights) {

    FAISS_THROW_IF_NOT_FMT (nx >= k,
             "Number of training points (%ld) should be at least "
             "as large as number of clusters (%ld)", nx, k);

    FAISS_THROW_IF_NOT_FMT ((!codec || codec->d == d),
             "Codec dimension %d not the same as data dimension %d",
             int(codec->d), int(d));

    FAISS_THROW_IF_NOT_FMT (index.d == d,
            "Index dimension %d not the same as data dimension %d",
            int(index.d), int(d));

    double t0 = getmillisecs();

    if (!codec) {
        // Check for NaNs in input data. Normally it is the user's
        // responsibility, but it may spare us some hard-to-debug
        // reports.
        const float *x = reinterpret_cast<const float *>(x_in);
        for (size_t i = 0; i < nx * d; i++) {
            FAISS_THROW_IF_NOT_MSG (finite (x[i]),
                                    "input contains NaN's or Inf's");
        }
    }

    const uint8_t *x = x_in;
    std::unique_ptr<uint8_t []> del1;
    std::unique_ptr<float []> del3;
    size_t line_size = codec ? codec->sa_code_size() : sizeof(float) * d;

    if (nx > k * max_points_per_centroid) {
        uint8_t *x_new;
        float *weights_new;
        nx = subsample_training_set (*this, nx, x, line_size, weights,
                                &x_new, &weights_new);
        del1.reset (x_new); x = x_new;
        del3.reset (weights_new); weights = weights_new;
    } else if (nx < k * min_points_per_centroid) {
        fprintf (stderr,
                 "WARNING clustering %ld points to %ld centroids: "
                 "please provide at least %ld training points\n",
                 nx, k, idx_t(k) * min_points_per_centroid);
    }

    if (nx == k) {
        // this is a corner case, just copy training set to clusters
        if (verbose) {
            printf("Number of training points (%ld) same as number of "
                   "clusters, just copying\n", nx);
        }
        centroids.resize (d * k);
        if (!codec) {
            memcpy (centroids.data(), x_in, sizeof (float) * d * k);
        } else {
            codec->sa_decode (nx, x_in, centroids.data());
        }

        // one fake iteration...
        ClusteringIterationStats stats = { 0.0, 0.0, 0.0, 1.0, 0 };
        iteration_stats.push_back (stats);

        index.reset();
        index.add(k, centroids.data());
        return;
    }


    if (verbose) {
        printf("Clustering %d points in %ldD to %ld clusters, "
               "redo %d times, %d iterations\n",
               int(nx), d, k, nredo, niter);
        if (codec) {
            printf("Input data encoded in %ld bytes per vector\n",
                   codec->sa_code_size ());
        }
    }

    std::unique_ptr<idx_t []> assign(new idx_t[nx]);
    std::unique_ptr<float []> dis(new float[nx]);

    // remember best iteration for redo
    float best_err = HUGE_VALF;
    std::vector<ClusteringIterationStats> best_obj;
    std::vector<float> best_centroids;

    // support input centroids

    FAISS_THROW_IF_NOT_MSG (
       centroids.size() % d == 0,
       "size of provided input centroids not a multiple of dimension"
    );

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

    // temporary buffer to decode vectors during the optimization
    std::vector<float> decode_buffer
        (codec ? d * decode_block_size : 0);

    for (int redo = 0; redo < nredo; redo++) {

        if (verbose && nredo > 1) {
            printf("Outer iteration %d / %d\n", redo, nredo);
        }

        // initialize (remaining) centroids with random points from the dataset
        centroids.resize (d * k);
        std::vector<int> perm (nx);

        rand_perm (perm.data(), nx, seed + 1 + redo * 15486557L);

        if (!codec) {
            for (int i = n_input_centroids; i < k ; i++) {
                memcpy (&centroids[i * d], x + perm[i] * line_size, line_size);
            }
        } else {
            for (int i = n_input_centroids; i < k ; i++) {
                codec->sa_decode (1, x + perm[i] * line_size, &centroids[i * d]);
            }
        }

        post_process_centroids ();

        // prepare the index

        if (index.ntotal != 0) {
            index.reset();
        }

        if (!index.is_trained) {
            index.train (k, centroids.data());
        }

        index.add (k, centroids.data());

        // k-means iterations

        float err = 0;
        for (int i = 0; i < niter; i++) {
            double t0s = getmillisecs();

            if (!codec) {
                index.search (nx, reinterpret_cast<const float *>(x), 1,
                              dis.get(), assign.get());
            } else {
                // search by blocks of decode_block_size vectors
                size_t code_size = codec->sa_code_size ();
                for (size_t i0 = 0; i0 < nx; i0 += decode_block_size) {
                    size_t i1 = i0 + decode_block_size;
                    if (i1 > nx) { i1 = nx; }
                    codec->sa_decode (i1 - i0, x + code_size * i0,
                                      decode_buffer.data ());
                    index.search (i1 - i0, decode_buffer.data (), 1,
                                  dis.get() + i0, assign.get() + i0);
                }
            }

            InterruptCallback::check();
            t_search_tot += getmillisecs() - t0s;

            // accumulate error
            err = 0;
            for (int j = 0; j < nx; j++) {
                err += dis[j];
            }

            // update the centroids
            std::vector<float> hassign (k);

            size_t k_frozen = frozen_centroids ? n_input_centroids : 0;
            compute_centroids (
                  d, k, nx, k_frozen,
                  x, codec, assign.get(), weights,
                  hassign.data(), centroids.data()
            );

            int nsplit = split_clusters (
                  d, k, nx, k_frozen,
                  hassign.data(), centroids.data()
            );

            // collect statistics
            ClusteringIterationStats stats =
                { err, (getmillisecs() - t0) / 1000.0,
                  t_search_tot / 1000, imbalance_factor (nx, k, assign.get()),
                  nsplit };
            iteration_stats.push_back(stats);

            if (verbose) {
                printf ("  Iteration %d (%.2f s, search %.2f s): "
                        "objective=%g imbalance=%.3f nsplit=%d       \r",
                        i, stats.time, stats.time_search, stats.obj,
                        stats.imbalance_factor, nsplit);
                fflush (stdout);
            }

            post_process_centroids ();

            // add centroids to index for the next iteration (or for output)

            index.reset ();
            if (update_index) {
                index.train (k, centroids.data());
            }

            index.add (k, centroids.data());
            InterruptCallback::check ();
        }

        if (verbose) printf("\n");
        if (nredo > 1) {
            if (err < best_err) {
                if (verbose) {
                    printf ("Objective improved: keep new clusters\n");
                }
                best_centroids = centroids;
                best_obj = iteration_stats;
                best_err = err;
            }
            index.reset ();
        }
    }
    if (nredo > 1) {
        centroids = best_centroids;
        iteration_stats = best_obj;
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
    return clus.iteration_stats.back().obj;
}

} // namespace faiss
