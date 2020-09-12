/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <cstdlib>

#include <omp.h>

#include <unordered_map>
#include <pthread.h>

#include <gtest/gtest.h>

#include <faiss/OnDiskInvertedLists.h>
#include <faiss/IndexIVFFlat.h>
#include <faiss/IndexFlat.h>
#include <faiss/utils/random.h>
#include <faiss/index_io.h>


namespace {

struct Tempfilename {

    static pthread_mutex_t mutex;

    std::string filename;

    Tempfilename (const char *prefix = nullptr) {
        pthread_mutex_lock (&mutex);
        char *cfname = tempnam (nullptr, prefix);
        filename = cfname;
        free(cfname);
        pthread_mutex_unlock (&mutex);
    }

    ~Tempfilename () {
        if (access (filename.c_str(), F_OK)) {
            unlink (filename.c_str());
        }
    }

    const char *c_str() {
        return filename.c_str();
    }

};

pthread_mutex_t Tempfilename::mutex = PTHREAD_MUTEX_INITIALIZER;

}  // namespace


TEST(ONDISK, make_invlists) {
    int nlist = 100;
    int code_size = 32;
    int nadd = 1000000;
    std::unordered_map<int, int> listnos;

    Tempfilename filename;

    faiss::OnDiskInvertedLists ivf (
                nlist, code_size,
                filename.c_str());

    {
        std::vector<uint8_t> code(32);
        for (int i = 0; i < nadd; i++) {
            double d = drand48();
            int list_no = int(nlist * d * d); // skewed distribution
            int * ar = (int*)code.data();
            ar[0] = i;
            ar[1] = list_no;
            ivf.add_entry (list_no, i, code.data());
            listnos[i] = list_no;
        }
    }

    int ntot = 0;
    for (int i = 0; i < nlist; i++) {
        int size = ivf.list_size(i);
        const faiss::Index::idx_t *ids = ivf.get_ids (i);
        const uint8_t *codes = ivf.get_codes (i);
        for (int j = 0; j < size; j++) {
            faiss::Index::idx_t id = ids[j];
            const int * ar = (const int*)&codes[code_size * j];
            EXPECT_EQ (ar[0], id);
            EXPECT_EQ (ar[1], i);
            EXPECT_EQ (listnos[id], i);
            ntot ++;
        }
    }
    EXPECT_EQ (ntot, nadd);
};


TEST(ONDISK, test_add) {
    int d = 8;
    int nlist = 30, nq = 200, nb = 1500, k = 10;
    faiss::IndexFlatL2 quantizer(d);
    {
        std::vector<float> x(d * nlist);
        faiss::float_rand(x.data(), d * nlist, 12345);
        quantizer.add(nlist, x.data());
    }
    std::vector<float> xb(d * nb);
    faiss::float_rand(xb.data(), d * nb, 23456);

    faiss::IndexIVFFlat index(&quantizer, d, nlist);
    index.add(nb, xb.data());

    std::vector<float> xq(d * nb);
    faiss::float_rand(xq.data(), d * nq, 34567);

    std::vector<float> ref_D (nq * k);
    std::vector<faiss::Index::idx_t> ref_I (nq * k);

    index.search (nq, xq.data(), k,
                  ref_D.data(), ref_I.data());

    Tempfilename filename, filename2;

    // test add + search
    {
        faiss::IndexIVFFlat index2(&quantizer, d, nlist);

        faiss::OnDiskInvertedLists ivf (
                index.nlist, index.code_size,
                filename.c_str());

        index2.replace_invlists(&ivf);

        index2.add(nb, xb.data());

        std::vector<float> new_D (nq * k);
        std::vector<faiss::Index::idx_t> new_I (nq * k);

        index2.search (nq, xq.data(), k,
                       new_D.data(), new_I.data());

        EXPECT_EQ (ref_D, new_D);
        EXPECT_EQ (ref_I, new_I);

        write_index(&index2, filename2.c_str());

    }

    // test io
    {
        faiss::Index *index3 = faiss::read_index(filename2.c_str());

        std::vector<float> new_D (nq * k);
        std::vector<faiss::Index::idx_t> new_I (nq * k);

        index3->search (nq, xq.data(), k,
                        new_D.data(), new_I.data());

        EXPECT_EQ (ref_D, new_D);
        EXPECT_EQ (ref_I, new_I);

        delete index3;
    }

};



// WARN this thest will run multithreaded only in opt mode
TEST(ONDISK, make_invlists_threaded) {
    int nlist = 100;
    int code_size = 32;
    int nadd = 1000000;

    Tempfilename filename;

    faiss::OnDiskInvertedLists ivf (
                nlist, code_size,
                filename.c_str());

    std::vector<int> list_nos (nadd);

    for (int i = 0; i < nadd; i++) {
        double d = drand48();
        list_nos[i] = int(nlist * d * d); // skewed distribution
    }

#pragma omp parallel
    {
        std::vector<uint8_t> code(32);
#pragma omp for
        for (int i = 0; i < nadd; i++) {
            int list_no = list_nos[i];
            int * ar = (int*)code.data();
            ar[0] = i;
            ar[1] = list_no;
            ivf.add_entry (list_no, i, code.data());
        }
    }

    int ntot = 0;
    for (int i = 0; i < nlist; i++) {
        int size = ivf.list_size(i);
        const faiss::Index::idx_t *ids = ivf.get_ids (i);
        const uint8_t *codes = ivf.get_codes (i);
        for (int j = 0; j < size; j++) {
            faiss::Index::idx_t id = ids[j];
            const int * ar = (const int*)&codes[code_size * j];
            EXPECT_EQ (ar[0], id);
            EXPECT_EQ (ar[1], i);
            EXPECT_EQ (list_nos[id], i);
            ntot ++;
        }
    }
    EXPECT_EQ (ntot, nadd);

};
