/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

// I/O code for indexes

#pragma once



namespace faiss {

struct Index;
struct IndexIVF;
struct VectorTransform;

namespace gpu {
struct GpuIndexFlat;
}

/* cloning functions */
Index *clone_index (const Index *);

struct IndexComposition {
    Index *index = nullptr;
    gpu::GpuIndexFlat *quantizer = nullptr;
    long mode = 0; // 0: all data, 1: copy quantizer, 2: copy data
};

/** Cloner class, useful to override classes with other cloning
 * functions. The cloning function above just calls
 * Cloner::clone_Index. */
struct Cloner {
    virtual VectorTransform *clone_VectorTransform (const VectorTransform *);
    virtual Index *clone_Index (const Index *);
    virtual Index *clone_Index (IndexComposition* index_composition);
    virtual IndexIVF *clone_IndexIVF (const IndexIVF *);
    virtual ~Cloner() {}
};



} // namespace faiss
