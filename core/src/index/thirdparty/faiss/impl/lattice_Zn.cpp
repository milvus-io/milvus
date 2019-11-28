/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/impl/lattice_Zn.h>

#include <cstdlib>
#include <cmath>
#include <cstring>
#include <cassert>

#include <queue>
#include <unordered_set>
#include <unordered_map>
#include <algorithm>

#include <faiss/utils/distances.h>

namespace faiss {

/********************************************
 * small utility functions
 ********************************************/

namespace {

inline float sqr(float x) {
    return x * x;
}


typedef std::vector<float> point_list_t;

struct Comb {
    std::vector<uint64_t> tab; // Pascal's triangle
    int nmax;

    explicit Comb(int nmax): nmax(nmax) {
        tab.resize(nmax * nmax, 0);
        tab[0] = 1;
        for(int i = 1; i < nmax; i++) {
            tab[i * nmax] = 1;
            for(int j = 1; j <= i; j++) {
                tab[i * nmax + j] =
                    tab[(i - 1) * nmax + j] +
                    tab[(i - 1) * nmax + (j - 1)];
            }

        }
    }

    uint64_t operator()(int n, int p) const {
        assert (n < nmax && p < nmax);
        if (p > n) return 0;
        return tab[n * nmax + p];
    }
};

Comb comb(100);



// compute combinations of n integer values <= v that sum up to total (squared)
point_list_t sum_of_sq (float total, int v, int n, float add = 0) {
    if (total < 0) {
        return point_list_t();
    } else if (n == 1) {
        while (sqr(v + add) > total) v--;
        if (sqr(v + add) == total) {
            return point_list_t(1, v + add);
        } else {
            return point_list_t();
        }
    } else {
        point_list_t res;
        while (v >= 0) {
            point_list_t sub_points =
                sum_of_sq (total - sqr(v + add), v, n - 1, add);
            for (size_t i = 0; i < sub_points.size(); i += n - 1) {
                res.push_back (v + add);
                for (int j = 0; j < n - 1; j++) {
                    res.push_back(sub_points[i + j]);
                }
            }
            v--;
        }
        return res;
    }
}

int decode_comb_1 (uint64_t *n, int k1, int r) {
    while (comb(r, k1) > *n) {
        r--;
    }
    *n -= comb(r, k1);
    return r;
}

// optimized version for < 64 bits
long repeats_encode_64 (
     const std::vector<Repeat> & repeats,
     int dim, const float *c)
{
    uint64_t coded = 0;
    int nfree = dim;
    uint64_t code = 0, shift = 1;
    for (auto r = repeats.begin(); r != repeats.end(); ++r) {
        int rank = 0, occ = 0;
        uint64_t code_comb = 0;
        uint64_t tosee = ~coded;
        for(;;) {
            // directly jump to next available slot.
            int i = __builtin_ctzl(tosee);
            tosee &= ~(1UL << i) ;
            if (c[i] == r->val) {
                code_comb += comb(rank, occ + 1);
                occ++;
                coded |= 1UL << i;
                if (occ == r->n) break;
            }
            rank++;
        }
        uint64_t max_comb = comb(nfree, r->n);
        code += shift * code_comb;
        shift *= max_comb;
        nfree -= r->n;
    }
    return code;
}


void repeats_decode_64(
     const std::vector<Repeat> & repeats,
     int dim, uint64_t code, float *c)
{
    uint64_t decoded = 0;
    int nfree = dim;
    for (auto r = repeats.begin(); r != repeats.end(); ++r) {
        uint64_t max_comb = comb(nfree, r->n);
        uint64_t code_comb = code % max_comb;
        code /= max_comb;

        int occ = 0;
        int rank = nfree;
        int next_rank = decode_comb_1 (&code_comb, r->n, rank);
        uint64_t tosee = ((1UL << dim) - 1) ^ decoded;
        for(;;) {
            int i = 63 - __builtin_clzl(tosee);
            tosee &= ~(1UL << i);
            rank--;
            if (rank == next_rank) {
                decoded |= 1UL << i;
                c[i] = r->val;
                occ++;
                if (occ == r->n) break;
                next_rank = decode_comb_1 (
                   &code_comb, r->n - occ, next_rank);
            }
        }
        nfree -= r->n;
    }

}



} // anonymous namespace

Repeats::Repeats (int dim, const float *c): dim(dim)
{
    for(int i = 0; i < dim; i++) {
        int j = 0;
        for(;;) {
            if (j == repeats.size()) {
                repeats.push_back(Repeat{c[i], 1});
                break;
            }
            if (repeats[j].val == c[i]) {
                repeats[j].n++;
                break;
            }
            j++;
        }
    }
}


long Repeats::count () const
{
    long accu = 1;
    int remain = dim;
    for (int i = 0; i < repeats.size(); i++) {
        accu *= comb(remain, repeats[i].n);
        remain -= repeats[i].n;
    }
    return accu;
}



// version with a bool vector that works for > 64 dim
long Repeats::encode(const float *c) const
{
    if (dim < 64) {
        return repeats_encode_64 (repeats, dim, c);
    }
    std::vector<bool> coded(dim, false);
    int nfree = dim;
    uint64_t code = 0, shift = 1;
    for (auto r = repeats.begin(); r != repeats.end(); ++r) {
        int rank = 0, occ = 0;
        uint64_t code_comb = 0;
        for (int i = 0; i < dim; i++) {
            if (!coded[i]) {
                if (c[i] == r->val) {
                    code_comb += comb(rank, occ + 1);
                    occ++;
                    coded[i] = true;
                    if (occ == r->n) break;
                }
                rank++;
            }
        }
        uint64_t max_comb = comb(nfree, r->n);
        code += shift * code_comb;
        shift *= max_comb;
        nfree -= r->n;
    }
    return code;
}



void Repeats::decode(uint64_t code, float *c) const
{
    if (dim < 64) {
        repeats_decode_64 (repeats, dim, code, c);
        return;
    }

    std::vector<bool> decoded(dim, false);
    int nfree = dim;
    for (auto r = repeats.begin(); r != repeats.end(); ++r) {
        uint64_t max_comb = comb(nfree, r->n);
        uint64_t code_comb = code % max_comb;
        code /= max_comb;

        int occ = 0;
        int rank = nfree;
        int next_rank = decode_comb_1 (&code_comb, r->n, rank);
        for (int i = dim - 1; i >= 0; i--) {
            if (!decoded[i]) {
                rank--;
                if (rank == next_rank) {
                    decoded[i] = true;
                    c[i] = r->val;
                    occ++;
                    if (occ == r->n) break;
                    next_rank = decode_comb_1 (
                         &code_comb, r->n - occ, next_rank);
                }
            }
        }
        nfree -= r->n;
    }

}



/********************************************
 * EnumeratedVectors functions
 ********************************************/


void EnumeratedVectors::encode_multi(size_t n, const float *c,
                               uint64_t * codes) const
{
#pragma omp parallel if (n > 1000)
    {
#pragma omp for
        for(int i = 0; i < n; i++) {
            codes[i] = encode(c + i * dim);
        }
    }
}


void EnumeratedVectors::decode_multi(size_t n, const uint64_t * codes,
                               float *c) const
{
#pragma omp parallel if (n > 1000)
    {
#pragma omp for
        for(int i = 0; i < n; i++) {
            decode(codes[i], c + i * dim);
        }
    }
}

void EnumeratedVectors::find_nn (
                  size_t nc, const uint64_t * codes,
                  size_t nq, const float *xq,
                  long *labels, float *distances)
{
    for (long i = 0; i < nq; i++) {
        distances[i] = -1e20;
        labels[i] = -1;
    }

    float c[dim];
    for(long i = 0; i < nc; i++) {
        uint64_t code = codes[nc];
        decode(code, c);
        for (long j = 0; j < nq; j++) {
            const float *x = xq + j * dim;
            float dis = fvec_inner_product(x, c, dim);
            if (dis > distances[j]) {
                distances[j] = dis;
                labels[j] = i;
            }
        }
    }

}


/**********************************************************
 * ZnSphereSearch
 **********************************************************/


ZnSphereSearch::ZnSphereSearch(int dim, int r2): dimS(dim), r2(r2) {
    voc = sum_of_sq(r2, int(ceil(sqrt(r2)) + 1), dim);
    natom = voc.size() / dim;
}

float ZnSphereSearch::search(const float *x, float *c) const {
    float tmp[dimS * 2];
    int tmp_int[dimS];
    return search(x, c, tmp, tmp_int);
}

float ZnSphereSearch::search(const float *x, float *c,
                             float *tmp, // size 2 *dim
                             int *tmp_int, // size dim
                             int *ibest_out
                             ) const {
    int dim = dimS;
    assert (natom > 0);
    int *o = tmp_int;
    float *xabs = tmp;
    float *xperm = tmp + dim;

    // argsort
    for (int i = 0; i < dim; i++) {
        o[i] = i;
        xabs[i] = fabsf(x[i]);
    }
    std::sort(o, o + dim, [xabs](int a, int b) {
            return xabs[a] > xabs[b];
        });
    for (int i = 0; i < dim; i++) {
        xperm[i] = xabs[o[i]];
    }
    // find best
    int ibest = -1;
    float dpbest = -100;
    for (int i = 0; i < natom; i++) {
        float dp = fvec_inner_product (voc.data() + i * dim, xperm, dim);
        if (dp > dpbest) {
            dpbest = dp;
            ibest = i;
        }
    }
    // revert sort
    const float *cin = voc.data() + ibest * dim;
    for (int i = 0; i < dim; i++) {
        c[o[i]] = copysignf (cin[i], x[o[i]]);
    }
    if (ibest_out) {
        *ibest_out = ibest;
    }
    return dpbest;
}

void ZnSphereSearch::search_multi(int n, const float *x,
                                  float *c_out,
                                  float *dp_out) {
#pragma omp parallel if (n > 1000)
    {
#pragma omp for
        for(int i = 0; i < n; i++) {
            dp_out[i] = search(x + i * dimS, c_out + i * dimS);
        }
    }
}


/**********************************************************
 * ZnSphereCodec
 **********************************************************/

ZnSphereCodec::ZnSphereCodec(int dim, int r2):
    ZnSphereSearch(dim, r2),
    EnumeratedVectors(dim)
{
    nv = 0;
    for (int i = 0; i < natom; i++) {
        Repeats repeats(dim, &voc[i * dim]);
        CodeSegment cs(repeats);
        cs.c0 = nv;
        Repeat &br = repeats.repeats.back();
        cs.signbits = br.val == 0 ? dim - br.n : dim;
        code_segments.push_back(cs);
        nv += repeats.count() << cs.signbits;
    }

    uint64_t nvx = nv;
    code_size = 0;
    while (nvx > 0) {
        nvx >>= 8;
        code_size++;
    }
}

uint64_t ZnSphereCodec::search_and_encode(const float *x) const {
    float tmp[dim * 2];
    int tmp_int[dim];
    int ano; // atom number
    float c[dim];
    search(x, c, tmp, tmp_int, &ano);
    uint64_t signs = 0;
    float cabs[dim];
    int nnz = 0;
    for (int i = 0; i < dim; i++) {
        cabs[i] = fabs(c[i]);
        if (c[i] != 0) {
            if (c[i] < 0) {
                signs |= 1UL << nnz;
            }
            nnz ++;
        }
    }
    const CodeSegment &cs = code_segments[ano];
    assert(nnz == cs.signbits);
    uint64_t code = cs.c0 + signs;
    code += cs.encode(cabs) << cs.signbits;
    return code;
}

uint64_t ZnSphereCodec::encode(const float *x) const
{
    return search_and_encode(x);
}


void ZnSphereCodec::decode(uint64_t code, float *c) const {
    int i0 = 0, i1 = natom;
    while (i0 + 1 < i1) {
        int imed = (i0 + i1) / 2;
        if (code_segments[imed].c0 <= code) i0 = imed;
        else i1 = imed;
    }
    const CodeSegment &cs = code_segments[i0];
    code -= cs.c0;
    uint64_t signs = code;
    code >>= cs.signbits;
    cs.decode(code, c);

    int nnz = 0;
    for (int i = 0; i < dim; i++) {
        if (c[i] != 0) {
            if (signs & (1UL << nnz)) {
                c[i] = -c[i];
            }
            nnz ++;
        }
    }
}


/**************************************************************
 * ZnSphereCodecRec
 **************************************************************/

uint64_t ZnSphereCodecRec::get_nv(int ld, int r2a) const
{
    return all_nv[ld * (r2 + 1) + r2a];
}


uint64_t ZnSphereCodecRec::get_nv_cum(int ld, int r2t, int r2a) const
{
    return all_nv_cum[(ld * (r2 + 1) + r2t) * (r2 + 1) + r2a];
}

void ZnSphereCodecRec::set_nv_cum(int ld, int r2t, int r2a, uint64_t cum)
{
    all_nv_cum[(ld * (r2 + 1) + r2t) * (r2 + 1) + r2a] = cum;
}


ZnSphereCodecRec::ZnSphereCodecRec(int dim, int r2):
    EnumeratedVectors(dim), r2(r2)
{
    log2_dim = 0;
    while (dim > (1 << log2_dim)) {
        log2_dim++;
    }
    assert(dim == (1 << log2_dim) ||
           !"dimension must be a power of 2");

    all_nv.resize((log2_dim + 1) * (r2 + 1));
    all_nv_cum.resize((log2_dim + 1) * (r2 + 1) * (r2 + 1));

    for (int r2a = 0; r2a <= r2; r2a++) {
        int r = int(sqrt(r2a));
        if (r * r == r2a) {
            all_nv[r2a] = r == 0 ? 1 : 2;
        } else {
            all_nv[r2a] = 0;
        }
    }

    for (int ld = 1; ld <= log2_dim; ld++) {

        for (int r2sub = 0; r2sub <= r2; r2sub++) {
            uint64_t nv = 0;
            for (int r2a = 0; r2a <= r2sub; r2a++) {
                int r2b = r2sub - r2a;
                set_nv_cum(ld, r2sub, r2a, nv);
                nv += get_nv(ld - 1, r2a) * get_nv(ld - 1, r2b);
            }
            all_nv[ld * (r2 + 1) + r2sub] = nv;
        }
    }
    nv = get_nv(log2_dim, r2);

    uint64_t nvx = nv;
    code_size = 0;
    while (nvx > 0) {
        nvx >>= 8;
        code_size++;
    }

    int cache_level = std::min(3, log2_dim - 1);
    decode_cache_ld = 0;
    assert(cache_level <= log2_dim);
    decode_cache.resize((r2 + 1));

    for (int r2sub = 0; r2sub <= r2; r2sub++) {
        int ld = cache_level;
        uint64_t nvi = get_nv(ld, r2sub);
        std::vector<float> &cache = decode_cache[r2sub];
        int dimsub = (1 << cache_level);
        cache.resize (nvi * dimsub);
        float c[dim];
        uint64_t code0 = get_nv_cum(cache_level + 1, r2,
                                 r2 - r2sub);
        for (int i = 0; i < nvi; i++) {
            decode(i + code0, c);
            memcpy(&cache[i * dimsub], c + dim - dimsub,
                   dimsub * sizeof(*c));
        }
    }
    decode_cache_ld = cache_level;
}

uint64_t ZnSphereCodecRec::encode(const float *c) const
{
    return encode_centroid(c);
}



uint64_t ZnSphereCodecRec::encode_centroid(const float *c) const
{
    uint64_t codes[dim];
    int norm2s[dim];
    for(int i = 0; i < dim; i++) {
        if (c[i] == 0) {
            codes[i] = 0;
            norm2s[i] = 0;
        } else {
            int r2i = int(c[i] * c[i]);
            norm2s[i] = r2i;
            codes[i] = c[i] >= 0 ? 0 : 1;
        }
    }
    int dim2 = dim / 2;
    for(int ld = 1; ld <= log2_dim; ld++) {
        for (int i = 0; i < dim2; i++) {
            int r2a = norm2s[2 * i];
            int r2b = norm2s[2 * i + 1];

            uint64_t code_a = codes[2 * i];
            uint64_t code_b = codes[2 * i + 1];

            codes[i] =
                get_nv_cum(ld, r2a + r2b, r2a) +
                code_a * get_nv(ld - 1, r2b) +
                code_b;
            norm2s[i] = r2a + r2b;
        }
        dim2 /= 2;
    }
    return codes[0];
}



void ZnSphereCodecRec::decode(uint64_t code, float *c) const
{
    uint64_t codes[dim];
    int norm2s[dim];
    codes[0] = code;
    norm2s[0] = r2;

    int dim2 = 1;
    for(int ld = log2_dim; ld > decode_cache_ld; ld--) {
        for (int i = dim2 - 1; i >= 0; i--) {
            int r2sub = norm2s[i];
            int i0 = 0, i1 = r2sub + 1;
            uint64_t codei = codes[i];
            const uint64_t *cum =
                &all_nv_cum[(ld * (r2 + 1) + r2sub) * (r2 + 1)];
            while (i1 > i0 + 1) {
                int imed = (i0 + i1) / 2;
                if (cum[imed] <= codei)
                    i0 = imed;
                else
                    i1 = imed;
            }
            int r2a = i0, r2b = r2sub - i0;
            codei -= cum[r2a];
            norm2s[2 * i] = r2a;
            norm2s[2 * i + 1] = r2b;

            uint64_t code_a = codei / get_nv(ld - 1, r2b);
            uint64_t code_b = codei % get_nv(ld - 1, r2b);

            codes[2 * i] = code_a;
            codes[2 * i + 1] = code_b;

        }
        dim2 *= 2;
    }

    if (decode_cache_ld == 0) {
        for(int i = 0; i < dim; i++) {
            if (norm2s[i] == 0) {
                c[i] = 0;
            } else {
                float r = sqrt(norm2s[i]);
                assert(r * r == norm2s[i]);
                c[i] = codes[i] == 0 ? r : -r;
            }
        }
    } else {
        int subdim = 1 << decode_cache_ld;
        assert ((dim2 * subdim) == dim);

        for(int i = 0; i < dim2; i++) {

            const std::vector<float> & cache =
                decode_cache[norm2s[i]];
            assert(codes[i] < cache.size());
            memcpy(c + i * subdim,
                   &cache[codes[i] * subdim],
                   sizeof(*c)* subdim);
        }
    }
}

// if not use_rec, instanciate an arbitrary harmless znc_rec
ZnSphereCodecAlt::ZnSphereCodecAlt (int dim, int r2):
    ZnSphereCodec (dim, r2),
    use_rec ((dim & (dim - 1)) == 0),
    znc_rec (use_rec ? dim : 8,
             use_rec ? r2 : 14)
{}

uint64_t ZnSphereCodecAlt::encode(const float *x) const
{
    if (!use_rec) {
        // it's ok if the vector is not normalized
        return ZnSphereCodec::encode(x);
    } else {
        // find nearest centroid
        std::vector<float> centroid(dim);
        search (x, centroid.data());
        return znc_rec.encode(centroid.data());
    }
}

void ZnSphereCodecAlt::decode(uint64_t code, float *c) const
{
    if (!use_rec) {
        ZnSphereCodec::decode (code, c);
    } else {
        znc_rec.decode (code, c);
    }
}


} // namespace faiss
