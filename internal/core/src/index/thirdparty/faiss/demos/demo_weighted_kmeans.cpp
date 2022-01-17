/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <cstdlib>

#include <faiss/Clustering.h>
#include <faiss/utils/random.h>
#include <faiss/utils/distances.h>
#include <faiss/IndexFlat.h>
#include <faiss/IndexHNSW.h>


namespace {


enum WeightedKMeansType {
    WKMT_FlatL2,
    WKMT_FlatIP,
    WKMT_FlatIP_spherical,
    WKMT_HNSW,
};


float weighted_kmeans_clustering (size_t d, size_t n, size_t k,
                                  const float *input,
                                  const float *weights,
                                  float *centroids,
                                  WeightedKMeansType index_num)
{
    using namespace faiss;
    Clustering clus (d, k);
    clus.verbose = true;

    std::unique_ptr<Index> index;

    switch (index_num) {
    case WKMT_FlatL2:
        index.reset(new IndexFlatL2 (d));
        break;
    case WKMT_FlatIP:
        index.reset(new IndexFlatIP (d));
        break;
    case WKMT_FlatIP_spherical:
        index.reset(new IndexFlatIP (d));
        clus.spherical = true;
        break;
    case WKMT_HNSW:
        IndexHNSWFlat *ihnsw = new IndexHNSWFlat (d, 32);
        ihnsw->hnsw.efSearch = 128;
        index.reset(ihnsw);
        break;
    }

    clus.train(n, input, *index.get(), weights);
    // on output the index contains the centroids.
    memcpy(centroids, clus.centroids.data(), sizeof(*centroids) * d * k);
    return clus.iteration_stats.back().obj;
}


int d = 32;
float sigma = 0.1;

#define BIGTEST

#ifdef BIGTEST
// the production setup = setting of https://fb.quip.com/CWgnAAYbwtgs
int nc = 200000;
int n_big = 4;
int n_small = 2;
#else
int nc = 5;
int n_big = 100;
int n_small = 10;
#endif

int n; // number of training points

void generate_trainset (std::vector<float> & ccent,
                        std::vector<float> & x,
                        std::vector<float> & weights)
{
    // same sampling as test_build_blocks.py test_weighted

    ccent.resize (d * 2 * nc);
    faiss::float_randn (ccent.data(), d * 2 * nc, 123);
    faiss::fvec_renorm_L2 (d, 2 * nc, ccent.data());
    n = nc * n_big + nc * n_small;
    x.resize(d * n);
    weights.resize(n);
    faiss::float_randn (x.data(), x.size(), 1234);

    float *xi = x.data();
    float *w = weights.data();
    for (int ci = 0; ci < nc * 2; ci++) { // loop over centroids
        int np = ci < nc ? n_big : n_small; // nb of points around this centroid
        for (int i = 0; i < np; i++) {
            for (int j = 0; j < d; j++) {
                xi[j] = xi[j] * sigma + ccent[ci * d + j];
            }
            *w++ = ci < nc ? 0.1 : 10;
            xi += d;
        }
    }
}

}


int main(int argc, char **argv) {
    std::vector<float> ccent;
    std::vector<float> x;
    std::vector<float> weights;

    printf("generate training set\n");
    generate_trainset(ccent, x, weights);

    std::vector<float> centroids;
    centroids.resize(nc * d);

    int the_index_num = -1;
    int the_with_weights = -1;

    if (argc == 3) {
        the_index_num = atoi(argv[1]);
        the_with_weights = atoi(argv[2]);
    }


    for (int index_num = WKMT_FlatL2;
         index_num <= WKMT_HNSW;
         index_num++) {

        if (the_index_num >= 0 && index_num != the_index_num) {
            continue;
        }

        for (int with_weights = 0; with_weights <= 1; with_weights++) {
            if (the_with_weights >= 0 && with_weights != the_with_weights) {
                continue;
            }

            printf("=================== index_num=%d Run %s weights\n",
                   index_num, with_weights ? "with" : "without");

            weighted_kmeans_clustering (
                 d, n, nc, x.data(),
                 with_weights ? weights.data() : nullptr,
                 centroids.data(), (WeightedKMeansType)index_num
            );

            { // compute distance of points to centroids
                faiss::IndexFlatL2 cent_index(d);
                cent_index.add(nc, centroids.data());
                std::vector<float> dis (n);
                std::vector<faiss::Index::idx_t> idx (n);

                cent_index.search (nc * 2, ccent.data(), 1,
                                   dis.data(), idx.data());

                float dis1 = 0, dis2 = 0;
                for (int i = 0; i < nc ; i++) {
                    dis1 += dis[i];
                }
                printf("average distance of points from big clusters: %g\n",
                       dis1 / nc);

                for (int i = 0; i < nc ; i++) {
                    dis2 += dis[i + nc];
                }

                printf("average distance of points from small clusters: %g\n",
                       dis2 / nc);

            }

        }
    }
    return 0;
}
