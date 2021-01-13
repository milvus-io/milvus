/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <faiss/utils/partitioning.h>

#include <cmath>
#include <cassert>

#include <faiss/impl/FaissAssert.h>

#include <faiss/utils/ordered_key_value.h>

namespace faiss {

namespace partitioning {

template<typename T>
T median3(T a, T b, T c) {
    if (a > b) {
        std::swap(a, b);
    }
    if (c > b) {
        return b;
    }
    if (c > a) {
        return c;
    }
    return a;
}


template<class C>
typename C::T sample_threshold_median3(
    const typename C::T * vals, int n,
    typename C::T thresh_inf, typename C::T thresh_sup
) {
    using T = typename C::T;
    size_t big_prime = 6700417;
    T val3[3];
    int vi = 0;

    for (size_t i = 0; i < n; i++) {
        T v = vals[(i * big_prime) % n];
        // thresh_inf < v < thresh_sup (for CMax)
        if (C::cmp(v, thresh_inf) && C::cmp(thresh_sup, v)) {
            val3[vi++] = v;
            if (vi == 3) {
                break;
            }
        }
    }

    if (vi == 3) {
        return median3(val3[0], val3[1], val3[2]);
    } else if (vi != 0) {
        return val3[0];
    } else {
        FAISS_THROW_MSG("too few values to compute a median");
    }
}

template<class C>
void count_lt_and_eq(
    const typename C::T * vals, size_t n, typename C::T thresh,
    size_t & n_lt, size_t & n_eq
) {
    n_lt = n_eq = 0;

    for(size_t i = 0; i < n; i++) {
        typename C::T v = *vals++;
        if(C::cmp(thresh, v)) {
            n_lt++;
        } else if(v == thresh) {
            n_eq++;
        }
    }
}


template<class C>
size_t compress_array(
    typename C::T *vals, typename C::TI * ids,
    size_t n, typename C::T thresh, size_t n_eq
) {
    size_t wp = 0;
    for(size_t i = 0; i < n; i++) {
        if (C::cmp(thresh, vals[i])) {
            vals[wp] = vals[i];
            ids[wp] = ids[i];
            wp++;
        } else if (n_eq > 0 && vals[i] == thresh) {
            vals[wp] = vals[i];
            ids[wp] = ids[i];
            wp++;
            n_eq--;
        }
    }
    assert(n_eq == 0);
    return wp;
}


#define IFV if(false)

template<class C>
typename C::T partition_fuzzy_median3(
    typename C::T *vals, typename C::TI * ids, size_t n,
    size_t q_min, size_t q_max, size_t * q_out)
{

    if (q_min == 0) {
        if (q_out) {
            *q_out = C::Crev::neutral();
        }
        return 0;
    }
    if (q_max >= n) {
        if (q_out) {
            *q_out = q_max;
        }
        return C::neutral();
    }

    using T = typename C::T;

    // here we use bissection with a median of 3 to find the threshold and
    // compress the arrays afterwards. So it's a n*log(n) algoirithm rather than
    // qselect's O(n) but it avoids compressing the array.

    FAISS_THROW_IF_NOT(n >= 3);

    T thresh_inf = C::Crev::neutral();
    T thresh_sup = C::neutral();
    T thresh = median3(vals[0], vals[n / 2], vals[n - 1]);

    size_t n_eq = 0, n_lt = 0;
    size_t q = 0;

    for(int it = 0; it < 200; it++) {
        count_lt_and_eq<C>(vals, n, thresh, n_lt, n_eq);

        IFV  printf("   thresh=%g [%g %g] n_lt=%ld n_eq=%ld, q=%ld:%ld/%ld\n",
                    thresh, thresh_inf, thresh_sup, n_lt, n_eq, q_min, q_max, n);

        if (n_lt <= q_min) {
            if (n_lt + n_eq >= q_min) {
                q = q_min;
                break;
            } else {
                thresh_inf = thresh;
            }
        } else if (n_lt <= q_max) {
            q = n_lt;
            break;
        } else {
            thresh_sup = thresh;
        }

        // FIXME avoid a second pass over the array to sample the threshold
        IFV  printf("     sample thresh in [%g %g]\n", thresh_inf, thresh_sup);
        thresh = sample_threshold_median3<C>(vals, n, thresh_inf, thresh_sup);
    }

    int64_t n_eq_1 = q - n_lt;

    IFV printf("shrink: thresh=%g n_eq_1=%ld\n", thresh, n_eq_1);

    if (n_eq_1 < 0) { // happens when > q elements are at lower bound
        q = q_min;
        thresh = C::Crev::nextafter(thresh);
        n_eq_1 = q;
    } else {
        assert(n_eq_1 <= n_eq);
    }

    int wp = compress_array<C>(vals, ids, n, thresh, n_eq_1);

    assert(wp == q);
    if (q_out) {
        *q_out = q;
    }

    return thresh;
}


} // namespace partitioning


template<class C>
typename C::T partition_fuzzy(
    typename C::T *vals, typename C::TI * ids, size_t n,
    size_t q_min, size_t q_max, size_t * q_out)
{

    return partitioning::partition_fuzzy_median3<C>(
        vals, ids, n, q_min, q_max, q_out);
}


// explicit template instanciations

template float partition_fuzzy<CMin<float, int64_t>> (
    float *vals, int64_t * ids, size_t n,
    size_t q_min, size_t q_max, size_t * q_out);

template float partition_fuzzy<CMax<float, int64_t>> (
    float *vals, int64_t * ids, size_t n,
    size_t q_min, size_t q_max, size_t * q_out);



} // namespace faiss