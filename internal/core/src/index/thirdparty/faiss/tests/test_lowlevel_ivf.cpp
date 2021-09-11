/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <cstdlib>

#include <memory>
#include <vector>
#include <thread>

#include <gtest/gtest.h>

#include <faiss/IndexIVF.h>
#include <faiss/IndexBinaryIVF.h>
#include <faiss/IndexPreTransform.h>
#include <faiss/AutoTune.h>
#include <faiss/index_factory.h>
#include <faiss/index_io.h>
#include <faiss/IVFlib.h>
#include <faiss/VectorTransform.h>

using namespace faiss;

namespace {

typedef Index::idx_t idx_t;


// dimension of the vectors to index
int d = 32;

// nb of training vectors
size_t nt = 5000;

// size of the database points per window step
size_t nb = 1000;

// nb of queries
size_t nq = 200;

int k = 10;


std::vector<float> make_data(size_t n)
{
    std::vector <float> database (n * d);
    for (size_t i = 0; i < n * d; i++) {
        database[i] = drand48();
    }
    return database;
}

std::unique_ptr<Index> make_trained_index(const char *index_type,
                                          MetricType metric_type)
{
    auto index = std::unique_ptr<Index>(index_factory(
                     d, index_type, metric_type));
    auto xt = make_data(nt);
    index->train(nt, xt.data());
    ParameterSpace().set_index_parameter (index.get(), "nprobe", 4);
    return index;
}

std::vector<idx_t> search_index(Index *index, const float *xq) {
    std::vector<idx_t> I(k * nq);
    std::vector<float> D(k * nq);
    index->search (nq, xq, k, D.data(), I.data());
    return I;
}




/*************************************************************
 * Test functions for a given index type
 *************************************************************/



void test_lowlevel_access (const char *index_key, MetricType metric) {
    std::unique_ptr<Index> index = make_trained_index(index_key, metric);

    auto xb = make_data (nb);
    index->add(nb, xb.data());

    /** handle the case if we have a preprocessor */

    const IndexPreTransform *index_pt =
        dynamic_cast<const IndexPreTransform*> (index.get());

    int dt = index->d;
    const float * xbt = xb.data();
    std::unique_ptr<float []> del_xbt;

    if (index_pt) {
        dt = index_pt->index->d;
        xbt = index_pt->apply_chain (nb, xb.data());
        if (xbt != xb.data()) {
            del_xbt.reset((float*)xbt);
        }
    }

    IndexIVF * index_ivf = ivflib::extract_index_ivf (index.get());

    /** Test independent encoding
     *
     * Makes it possible to do additions on a custom inverted list
     * implementation. From a set of vectors, computes the inverted
     * list ids + the codes corresponding to each vector.
     */

    std::vector<idx_t> list_nos (nb);
    std::vector<uint8_t> codes (index_ivf->code_size * nb);
    index_ivf->quantizer->assign(nb, xbt, list_nos.data());
    index_ivf->encode_vectors (nb, xbt, list_nos.data(), codes.data());

    // compare with normal IVF addition

    const InvertedLists *il = index_ivf->invlists;

    for (int list_no = 0; list_no < index_ivf->nlist; list_no++) {
        InvertedLists::ScopedCodes ivf_codes (il, list_no);
        InvertedLists::ScopedIds ivf_ids (il, list_no);
        size_t list_size = il->list_size (list_no);
        for (int i = 0; i < list_size; i++) {
            const uint8_t *ref_code = ivf_codes.get() + i * il->code_size;
            const uint8_t *new_code =
                codes.data() + ivf_ids[i] * il->code_size;
            EXPECT_EQ (memcmp(ref_code, new_code, il->code_size), 0);
        }
    }

    /** Test independent search
     *
     * Manually scans through inverted lists, computing distances and
     * ordering results organized in a heap.
     */

    // sample some example queries and get reference search results.
    auto xq = make_data (nq);
    auto ref_I = search_index (index.get(), xq.data());

    // handle preprocessing
    const float * xqt = xq.data();
    std::unique_ptr<float []> del_xqt;

    if (index_pt) {
        xqt = index_pt->apply_chain (nq, xq.data());
        if (xqt != xq.data()) {
            del_xqt.reset((float*)xqt);
        }
    }

    // quantize the queries to get the inverted list ids to visit.
    int nprobe = index_ivf->nprobe;

    std::vector<idx_t> q_lists (nq * nprobe);
    std::vector<float> q_dis (nq * nprobe);

    index_ivf->quantizer->search (nq, xqt, nprobe,
                                  q_dis.data(), q_lists.data());

    // object that does the scanning and distance computations.
    std::unique_ptr<InvertedListScanner> scanner (
                   index_ivf->get_InvertedListScanner());

    for (int i = 0; i < nq; i++) {
        std::vector<idx_t> I (k, -1);
        float default_dis = metric == METRIC_L2 ? HUGE_VAL : -HUGE_VAL;
        std::vector<float> D (k, default_dis);

        scanner->set_query (xqt + i * dt);

        for (int j = 0; j < nprobe; j++) {
            int list_no = q_lists[i * nprobe + j];
            if (list_no < 0) continue;
            scanner->set_list (list_no, q_dis[i * nprobe + j]);

            // here we get the inverted lists from the InvertedLists
            // object but they could come from anywhere

            scanner->scan_codes (
                 il->list_size (list_no),
                 InvertedLists::ScopedCodes(il, list_no).get(),
                 InvertedLists::ScopedIds(il, list_no).get(),
                 D.data(), I.data(), k);

            if (j == 0) {
                // all results so far come from list_no, so let's check if
                // the distance function works
                for (int jj = 0; jj < k; jj++) {
                    int vno = I[jj];
                    if (vno < 0) break; // heap is not full yet

                    // we have the codes from the addition test
                    float computed_D = scanner->distance_to_code (
                                 codes.data() + vno * il->code_size);

                    EXPECT_EQ (computed_D, D[jj]);
                }
            }
        }

        // re-order heap
        if (metric == METRIC_L2) {
            maxheap_reorder (k, D.data(), I.data());
        } else {
            minheap_reorder (k, D.data(), I.data());
        }

        // check that we have the same results as the reference search
        for (int j = 0; j < k; j++) {
            EXPECT_EQ (I[j], ref_I[i * k + j]);
        }
    }


}

} // anonymous namespace



/*************************************************************
 * Test entry points
 *************************************************************/

TEST(TestLowLevelIVF, IVFFlatL2) {
    test_lowlevel_access ("IVF32,Flat", METRIC_L2);
}

TEST(TestLowLevelIVF, PCAIVFFlatL2) {
    test_lowlevel_access ("PCAR16,IVF32,Flat", METRIC_L2);
}

TEST(TestLowLevelIVF, IVFFlatIP) {
    test_lowlevel_access ("IVF32,Flat", METRIC_INNER_PRODUCT);
}

TEST(TestLowLevelIVF, IVFSQL2) {
    test_lowlevel_access ("IVF32,SQ8", METRIC_L2);
}

TEST(TestLowLevelIVF, IVFSQIP) {
    test_lowlevel_access ("IVF32,SQ8", METRIC_INNER_PRODUCT);
}


TEST(TestLowLevelIVF, IVFPQL2) {
    test_lowlevel_access ("IVF32,PQ4np", METRIC_L2);
}

TEST(TestLowLevelIVF, IVFPQIP) {
    test_lowlevel_access ("IVF32,PQ4np", METRIC_INNER_PRODUCT);
}


/*************************************************************
 * Same for binary (a bit simpler)
 *************************************************************/

namespace {

int nbit = 256;

// here d is used the number of ints -> d=32 means 128 bits

std::vector<uint8_t> make_data_binary(size_t n)
{

    std::vector <uint8_t> database (n * nbit / 8);
    for (size_t i = 0; i < n * d; i++) {
        database[i] = lrand48();
    }
    return database;
}

std::unique_ptr<IndexBinary> make_trained_index_binary(const char *index_type)
{
    auto index = std::unique_ptr<IndexBinary>(index_binary_factory(
                     nbit, index_type));
    auto xt = make_data_binary (nt);
    index->train(nt, xt.data());
    return index;
}


void test_lowlevel_access_binary (const char *index_key) {
    std::unique_ptr<IndexBinary> index =
        make_trained_index_binary (index_key);

    IndexBinaryIVF * index_ivf = dynamic_cast<IndexBinaryIVF*>
        (index.get());
    assert (index_ivf);

    index_ivf->nprobe = 4;

    auto xb = make_data_binary (nb);
    index->add(nb, xb.data());

    std::vector<idx_t> list_nos (nb);
    index_ivf->quantizer->assign(nb, xb.data(), list_nos.data());

    /* For binary there is no test for encoding because binary vectors
     * are copied verbatim to the inverted lists */

    const InvertedLists *il = index_ivf->invlists;

    /** Test independent search
     *
     * Manually scans through inverted lists, computing distances and
     * ordering results organized in a heap.
     */

    // sample some example queries and get reference search results.
    auto xq = make_data_binary (nq);

    std::vector<idx_t> I_ref(k * nq);
    std::vector<int32_t> D_ref(k * nq);
    index->search (nq, xq.data(), k, D_ref.data(), I_ref.data());

    // quantize the queries to get the inverted list ids to visit.
    int nprobe = index_ivf->nprobe;

    std::vector<idx_t> q_lists (nq * nprobe);
    std::vector<int32_t> q_dis (nq * nprobe);

    // quantize queries
    index_ivf->quantizer->search (nq, xq.data(), nprobe,
                                  q_dis.data(), q_lists.data());

    // object that does the scanning and distance computations.
    std::unique_ptr<BinaryInvertedListScanner> scanner (
                   index_ivf->get_InvertedListScanner());

    for (int i = 0; i < nq; i++) {
        std::vector<idx_t> I (k, -1);
        uint32_t default_dis = 1 << 30;
        std::vector<int32_t> D (k, default_dis);

        scanner->set_query (xq.data() + i * index_ivf->code_size);

        for (int j = 0; j < nprobe; j++) {
            int list_no = q_lists[i * nprobe + j];
            if (list_no < 0) continue;
            scanner->set_list (list_no, q_dis[i * nprobe + j]);

            // here we get the inverted lists from the InvertedLists
            // object but they could come from anywhere

            scanner->scan_codes (
                 il->list_size (list_no),
                 InvertedLists::ScopedCodes(il, list_no).get(),
                 InvertedLists::ScopedIds(il, list_no).get(),
                 D.data(), I.data(), k);

            if (j == 0) {
                // all results so far come from list_no, so let's check if
                // the distance function works
                for (int jj = 0; jj < k; jj++) {
                    int vno = I[jj];
                    if (vno < 0) break; // heap is not full yet

                    // we have the codes from the addition test
                    float computed_D = scanner->distance_to_code (
                               xb.data() + vno * il->code_size);

                    EXPECT_EQ (computed_D, D[jj]);
                }
            }
        }

        printf("new before reroder: [");
        for (int j = 0; j < k; j++)
            printf("%ld,%d ", I[j], D[j]);
        printf("]\n");

        // re-order heap
        heap_reorder<CMax<int32_t, idx_t> > (k, D.data(), I.data());

        printf("ref: [");
        for (int j = 0; j < k; j++)
            printf("%ld,%d ", I_ref[j], D_ref[j]);
        printf("]\nnew: [");
        for (int j = 0; j < k; j++)
            printf("%ld,%d ", I[j], D[j]);
        printf("]\n");

        // check that we have the same results as the reference search
        for (int j = 0; j < k; j++) {
            // here the order is not guaranteed to be the same
            // so we scan through ref results
            // EXPECT_EQ (I[j], I_ref[i * k + j]);
            EXPECT_LE (D[j], D_ref[i * k + k - 1]);
            if (D[j] < D_ref[i * k + k - 1]) {
                int j2 = 0;
                while (j2 < k) {
                    if (I[j] == I_ref[i * k + j2]) break;
                    j2++;
                }
                EXPECT_LT(j2, k); // it was found
                if (j2 < k) {
                    EXPECT_EQ(D[j], D_ref[i * k + j2]);
                }
            }

        }

    }


}

} // anonymous namespace


TEST(TestLowLevelIVF, IVFBinary) {
    test_lowlevel_access_binary ("BIVF32");
}


namespace {

void test_threaded_search (const char *index_key, MetricType metric) {
    std::unique_ptr<Index> index = make_trained_index(index_key, metric);

    auto xb = make_data (nb);
    index->add(nb, xb.data());

    /** handle the case if we have a preprocessor */

    const IndexPreTransform *index_pt =
        dynamic_cast<const IndexPreTransform*> (index.get());

    int dt = index->d;
    const float * xbt = xb.data();
    std::unique_ptr<float []> del_xbt;

    if (index_pt) {
        dt = index_pt->index->d;
        xbt = index_pt->apply_chain (nb, xb.data());
        if (xbt != xb.data()) {
            del_xbt.reset((float*)xbt);
        }
    }

    IndexIVF * index_ivf = ivflib::extract_index_ivf (index.get());

    /** Test independent search
     *
     * Manually scans through inverted lists, computing distances and
     * ordering results organized in a heap.
     */

    // sample some example queries and get reference search results.
    auto xq = make_data (nq);
    auto ref_I = search_index (index.get(), xq.data());

    // handle preprocessing
    const float * xqt = xq.data();
    std::unique_ptr<float []> del_xqt;

    if (index_pt) {
        xqt = index_pt->apply_chain (nq, xq.data());
        if (xqt != xq.data()) {
            del_xqt.reset((float*)xqt);
        }
    }

    // quantize the queries to get the inverted list ids to visit.
    int nprobe = index_ivf->nprobe;

    std::vector<idx_t> q_lists (nq * nprobe);
    std::vector<float> q_dis (nq * nprobe);

    index_ivf->quantizer->search (nq, xqt, nprobe,
                                  q_dis.data(), q_lists.data());

    // now run search in this many threads
    int nproc = 3;


    for (int i = 0; i < nq; i++) {

        // one result table per thread
        std::vector<idx_t> I (k * nproc, -1);
        float default_dis = metric == METRIC_L2 ? HUGE_VAL : -HUGE_VAL;
        std::vector<float> D (k * nproc, default_dis);

        auto search_function = [index_ivf, &I, &D, dt, i, nproc,
                                xqt, nprobe, &q_dis, &q_lists]
            (int rank) {
            const InvertedLists *il = index_ivf->invlists;

            // object that does the scanning and distance computations.
            std::unique_ptr<InvertedListScanner> scanner (
                   index_ivf->get_InvertedListScanner());

            idx_t *local_I = I.data() + rank * k;
            float *local_D = D.data() + rank * k;

            scanner->set_query (xqt + i * dt);

            for (int j = rank; j < nprobe; j += nproc) {
                int list_no = q_lists[i * nprobe + j];
                if (list_no < 0) continue;
                scanner->set_list (list_no, q_dis[i * nprobe + j]);

                scanner->scan_codes (
                     il->list_size (list_no),
                     InvertedLists::ScopedCodes(il, list_no).get(),
                     InvertedLists::ScopedIds(il, list_no).get(),
                     local_D, local_I, k);
            }
        };

        // start the threads. Threads are numbered rank=0..nproc-1 (a la MPI)
        // thread rank takes care of inverted lists
        // rank, rank+nproc, rank+2*nproc,...
        std::vector<std::thread> threads;
        for (int rank = 0; rank < nproc; rank++) {
            threads.emplace_back(search_function, rank);
        }

        // join threads, merge heaps
        for (int rank = 0; rank < nproc; rank++) {
            threads[rank].join();
            if (rank == 0) continue; // nothing to merge
            // merge into first result
            if (metric == METRIC_L2) {
                maxheap_addn (k, D.data(), I.data(),
                              D.data() + rank * k,
                              I.data() + rank * k, k);
            } else {
                minheap_addn (k, D.data(), I.data(),
                              D.data() + rank * k,
                              I.data() + rank * k, k);
            }
        }

        // re-order heap
        if (metric == METRIC_L2) {
            maxheap_reorder (k, D.data(), I.data());
        } else {
            minheap_reorder (k, D.data(), I.data());
        }

        // check that we have the same results as the reference search
        for (int j = 0; j < k; j++) {
            EXPECT_EQ (I[j], ref_I[i * k + j]);
        }
    }


}

} // anonymous namepace


TEST(TestLowLevelIVF, ThreadedSearch) {
    test_threaded_search ("IVF32,Flat", METRIC_L2);
}
