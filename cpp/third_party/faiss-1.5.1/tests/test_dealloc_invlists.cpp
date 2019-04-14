/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <cstdio>
#include <cstdlib>

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include <faiss/IndexIVF.h>
#include <faiss/AutoTune.h>
#include <faiss/index_io.h>
#include <faiss/IVFlib.h>

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


int total_size = 40;
int window_size = 10;





std::vector<float> make_data(size_t n)
{
    std::vector <float> database (n * d);
    for (size_t i = 0; i < n * d; i++) {
        database[i] = drand48();
    }
    return database;
}

std::unique_ptr<Index> make_trained_index(const char *index_type)
{
    auto index = std::unique_ptr<Index>(index_factory(d, index_type));
    auto xt = make_data(nt * d);
    index->train(nt, xt.data());
    ParameterSpace().set_index_parameter (index.get(), "nprobe", 4);
    return index;
}

std::vector<idx_t> search_index(Index *index, const float *xq) {
    int k = 10;
    std::vector<idx_t> I(k * nq);
    std::vector<float> D(k * nq);
    index->search (nq, xq, k, D.data(), I.data());
    return I;
}





/*************************************************************
 * Test functions for a given index type
 *************************************************************/

struct EncapsulateInvertedLists: InvertedLists {

    const InvertedLists *il;

    EncapsulateInvertedLists(const InvertedLists *il):
        InvertedLists(il->nlist, il->code_size),
        il(il)
    {}

    static void * memdup (const void *m, size_t size) {
        if (size == 0) return nullptr;
        return memcpy (malloc(size), m, size);
    }

    size_t list_size(size_t list_no) const override {
        return il->list_size (list_no);
    }

    const uint8_t * get_codes (size_t list_no) const override {
        return (uint8_t*)memdup (il->get_codes(list_no),
                                 list_size(list_no) * code_size);
    }

    const idx_t * get_ids (size_t list_no) const override {
        return (idx_t*)memdup (il->get_ids(list_no),
                               list_size(list_no) * sizeof(idx_t));
    }

    void release_codes (const uint8_t *codes) const override {
        free ((void*)codes);
    }

    void release_ids (const idx_t *ids) const override {
        free ((void*)ids);
    }

    const uint8_t * get_single_code (size_t list_no, size_t offset)
        const override {
        return (uint8_t*)memdup (il->get_single_code(list_no, offset),
                                 code_size);
    }

    size_t add_entries(size_t, size_t, const idx_t*, const uint8_t*) override {
      assert(!"not implemented");
      return 0;
    }

    void update_entries(size_t, size_t, size_t, const idx_t*, const uint8_t*)
        override {
      assert(!"not implemented");
    }

    void resize(size_t, size_t) override {
      assert(!"not implemented");
    }

    ~EncapsulateInvertedLists() override {}
};



int test_dealloc_invlists (const char *index_key) {

    std::unique_ptr<Index> index = make_trained_index(index_key);
    IndexIVF * index_ivf = ivflib::extract_index_ivf (index.get());

    auto xb = make_data (nb * d);
    index->add(nb, xb.data());

    auto xq = make_data (nq * d);

    auto ref_res = search_index (index.get(), xq.data());

    EncapsulateInvertedLists eil(index_ivf->invlists);

    index_ivf->own_invlists = false;
    index_ivf->replace_invlists (&eil, false);

    // TEST: this could crash or leak mem
    auto new_res = search_index (index.get(), xq.data());

    // delete explicitly
    delete eil.il;

    // just to make sure
    EXPECT_EQ (ref_res, new_res);
    return 0;
}

} // anonymous namespace



/*************************************************************
 * Test entry points
 *************************************************************/

TEST(TestIvlistDealloc, IVFFlat) {
    test_dealloc_invlists ("IVF32,Flat");
}

TEST(TestIvlistDealloc, IVFSQ) {
    test_dealloc_invlists ("IVF32,SQ8");
}

TEST(TestIvlistDealloc, IVFPQ) {
    test_dealloc_invlists ("IVF32,PQ4np");
}
