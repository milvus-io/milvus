/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-
#ifndef FAISS_LATTICE_ZN_H
#define FAISS_LATTICE_ZN_H

#include <vector>
#include <stddef.h>
#include <stdint.h>

namespace faiss {

/** returns the nearest vertex in the sphere to a query. Returns only
 * the coordinates, not an id.
 *
 * Algorithm: all points are derived from a one atom vector up to a
 * permutation and sign changes. The search function finds the most
 * appropriate atom and transformation.
 */
struct ZnSphereSearch {
    int dimS, r2;
    int natom;

    /// size dim * ntatom
    std::vector<float> voc;

    ZnSphereSearch(int dim, int r2);

    /// find nearest centroid. x does not need to be normalized
    float search(const float *x, float *c) const;

    /// full call. Requires externally-allocated temp space
    float search(const float *x, float *c,
                 float *tmp, // size 2 *dim
                 int *tmp_int, // size dim
                 int *ibest_out = nullptr
                 ) const;

    // multi-threaded
    void search_multi(int n, const float *x,
                      float *c_out,
                      float *dp_out);

};


/***************************************************************************
 * Support ids as well.
 *
 * Limitations: ids are limited to 64 bit
 ***************************************************************************/

struct EnumeratedVectors {
    /// size of the collection
    uint64_t nv;
    int dim;

    explicit EnumeratedVectors(int dim): nv(0), dim(dim) {}

    /// encode a vector from a collection
    virtual uint64_t encode(const float *x) const = 0;

    /// decode it
    virtual void decode(uint64_t code, float *c) const = 0;

    // call encode on nc vectors
    void encode_multi (size_t nc, const float *c,
                       uint64_t * codes) const;

    // call decode on nc codes
    void decode_multi (size_t nc, const uint64_t * codes,
                       float *c) const;

    // find the nearest neighbor of each xq
    // (decodes and computes distances)
    void find_nn (size_t n, const uint64_t * codes,
                  size_t nq, const float *xq,
                  long *idx, float *dis);

    virtual ~EnumeratedVectors() {}

};

struct Repeat {
    float val;
    int n;
};

/** Repeats: used to encode a vector that has n occurrences of
 *  val. Encodes the signs and permutation of the vector. Useful for
 *  atoms.
 */
struct Repeats {
    int dim;
    std::vector<Repeat> repeats;

    // initialize from a template of the atom.
    Repeats(int dim = 0, const float *c = nullptr);

    // count number of possible codes for this atom
    long count() const;

    long encode(const float *c) const;

    void decode(uint64_t code, float *c) const;
};


/** codec that can return ids for the encoded vectors
 *
 * uses the ZnSphereSearch to encode the vector by encoding the
 * permutation and signs. Depends on ZnSphereSearch because it uses
 * the atom numbers */
struct ZnSphereCodec: ZnSphereSearch, EnumeratedVectors {

    struct CodeSegment:Repeats {
        explicit CodeSegment(const Repeats & r): Repeats(r) {}
        uint64_t c0; // first code assigned to segment
        int signbits;
    };

    std::vector<CodeSegment> code_segments;
    uint64_t nv;
    size_t code_size;

    ZnSphereCodec(int dim, int r2);

    uint64_t search_and_encode(const float *x) const;

    void decode(uint64_t code, float *c) const override;

    /// takes vectors that do not need to be centroids
    uint64_t encode(const float *x) const override;

};

/** recursive sphere codec
 *
 * Uses a recursive decomposition on the dimensions to encode
 * centroids found by the ZnSphereSearch. The codes are *not*
 * compatible with the ones of ZnSpehreCodec
 */
struct ZnSphereCodecRec: EnumeratedVectors {

    int r2;

    int log2_dim;
    int code_size;

    ZnSphereCodecRec(int dim, int r2);

    uint64_t encode_centroid(const float *c) const;

    void decode(uint64_t code, float *c) const override;

    /// vectors need to be centroids (does not work on arbitrary
    /// vectors)
    uint64_t encode(const float *x) const override;

    std::vector<uint64_t> all_nv;
    std::vector<uint64_t> all_nv_cum;

    int decode_cache_ld;
    std::vector<std::vector<float> > decode_cache;

    // nb of vectors in the sphere in dim 2^ld with r2 radius
    uint64_t get_nv(int ld, int r2a) const;

    // cumulative version
    uint64_t get_nv_cum(int ld, int r2t, int r2a) const;
    void set_nv_cum(int ld, int r2t, int r2a, uint64_t v);

};


/** Codec that uses the recursive codec if dim is a power of 2 and
 * the regular one otherwise */
struct ZnSphereCodecAlt: ZnSphereCodec {
    bool use_rec;
    ZnSphereCodecRec znc_rec;

    ZnSphereCodecAlt (int dim, int r2);

    uint64_t encode(const float *x) const override;

    void decode(uint64_t code, float *c) const override;

};


};


#endif
