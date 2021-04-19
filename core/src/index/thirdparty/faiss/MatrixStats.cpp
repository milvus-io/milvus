/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-


#include <faiss/MatrixStats.h>


#include <stdarg.h>     /* va_list, va_start, va_arg, va_end */

#include <cmath>
#include <cstdio>
#include <faiss/utils/utils.h>

namespace faiss {

/*********************************************************************
 * MatrixStats
 *********************************************************************/

MatrixStats::PerDimStats::PerDimStats():
    n(0), n_nan(0), n_inf(0), n0(0),
    min(HUGE_VALF), max(-HUGE_VALF),
    sum(0), sum2(0),
    mean(NAN), stddev(NAN)
{}


void MatrixStats::PerDimStats::add (float x)
{
    n++;
    if (std::isnan(x)) {
        n_nan++;
        return;
    }
    if (!std::isfinite(x)) {
        n_inf++;
        return;
    }
    if (x == 0) n0++;
    if (x < min) min = x;
    if (x > max) max = x;
    sum += x;
    sum2 += (double)x * (double)x;
}

void MatrixStats::PerDimStats::compute_mean_std ()
{
    n_valid = n - n_nan - n_inf;
    mean = sum / n_valid;
    double var = sum2 / n_valid - mean * mean;
    if (var < 0) var = 0;
    stddev = sqrt(var);
}


void MatrixStats::do_comment (const char *fmt, ...)
{
    va_list ap;

    /* Determine required size */
    va_start(ap, fmt);
    size_t size = vsnprintf(buf, nbuf, fmt, ap);
    va_end(ap);

    nbuf -= size;
    buf += size;
}



MatrixStats::MatrixStats (size_t n, size_t d, const float *x):
    n(n), d(d),
    n_collision(0), n_valid(0), n0(0),
    min_norm2(HUGE_VAL), max_norm2(0)
{
    std::vector<char> comment_buf (10000);
    buf = comment_buf.data ();
    nbuf = comment_buf.size();

    do_comment ("analyzing %ld vectors of size %ld\n", n, d);

    if (d > 1024) {
        do_comment (
           "indexing this many dimensions is hard, "
           "please consider dimensionality reducution (with PCAMatrix)\n");
    }

    size_t nbytes = sizeof (x[0]) * d;
    per_dim_stats.resize (d);

    for (size_t i = 0; i < n; i++) {
        const float *xi = x + d * i;
        double sum2 = 0;
        for (size_t j = 0; j < d; j++) {
            per_dim_stats[j].add (xi[j]);
            sum2 += xi[j] * (double)xi[j];
        }

        if (std::isfinite (sum2)) {
            n_valid++;
            if (sum2 == 0) {
                n0 ++;
            } else {
                if (sum2 < min_norm2) min_norm2 = sum2;
                if (sum2 > max_norm2) max_norm2 = sum2;
            }
        }

        { // check hash
            uint64_t hash = hash_bytes((const uint8_t*)xi, nbytes);
            auto elt = occurrences.find (hash);
            if (elt == occurrences.end()) {
                Occurrence occ = {i, 1};
                occurrences[hash] = occ;
            } else {
                if (!memcmp (xi, x + elt->second.first * d, nbytes)) {
                    elt->second.count ++;
                } else {
                    n_collision ++;
                    // we should use a list of collisions but overkill
                }
            }
        }
    }

    // invalid vecor stats
    if (n_valid == n) {
        do_comment ("no NaN or Infs in data\n");
    } else {
        do_comment ("%ld vectors contain NaN or Inf "
                 "(or have too large components), "
                 "expect bad results with indexing!\n", n - n_valid);
    }

    // copies in dataset
    if (occurrences.size() == n) {
        do_comment ("all vectors are distinct\n");
    } else {
        do_comment ("%ld vectors are distinct (%.2f%%)\n",
                 occurrences.size(),
                 occurrences.size() * 100.0 / n);

        if (n_collision > 0) {
            do_comment ("%ld collisions in hash table, "
                     "counts may be invalid\n", n_collision);
        }

        Occurrence max = {0, 0};
        for (auto it = occurrences.begin();
             it != occurrences.end(); ++it) {
            if (it->second.count > max.count) {
                max = it->second;
            }
        }
        do_comment ("vector %ld has %ld copies\n", max.first, max.count);
    }

    { // norm stats
        min_norm2 = sqrt (min_norm2);
        max_norm2 = sqrt (max_norm2);
        do_comment ("range of L2 norms=[%g, %g] (%ld null vectors)\n",
                 min_norm2, max_norm2, n0);

        if (max_norm2 < min_norm2 * 1.0001) {
            do_comment ("vectors are normalized, inner product and "
                     "L2  search are equivalent\n");
        }

        if (max_norm2 > min_norm2 * 100) {
            do_comment ("vectors have very large differences in norms, "
                     "is this normal?\n");
        }
    }

    { // per dimension stats

        double max_std = 0, min_std = HUGE_VAL;

        size_t n_dangerous_range = 0, n_0_range = 0, n0 = 0;

        for (size_t j = 0; j < d; j++) {
            PerDimStats &st = per_dim_stats[j];
            st.compute_mean_std ();
            n0 += st.n0;

            if (st.max == st.min) {
                n_0_range ++;
            } else if (st.max < 1.001 * st.min) {
                n_dangerous_range ++;
            }

            if (st.stddev > max_std) max_std = st.stddev;
            if (st.stddev < min_std) min_std = st.stddev;
        }



        if (n0 == 0) {
            do_comment ("matrix contains no 0s\n");
        } else {
            do_comment ("matrix contains %.2f %% 0 entries\n",
                     n0 * 100.0 / (n * d));
        }

        if (n_0_range == 0) {
            do_comment ("no constant dimensions\n");
        } else {
            do_comment ("%ld dimensions are constant: they can be removed\n",
                     n_0_range);
        }

        if (n_dangerous_range == 0) {
            do_comment ("no dimension has a too large mean\n");
        } else {
            do_comment ("%ld dimensions are too large "
                     "wrt. their variance, may loose precision "
                     "in IndexFlatL2 (use CenteringTransform)\n",
                     n_dangerous_range);
        }

        do_comment ("stddevs per dimension are in [%g %g]\n", min_std, max_std);

        size_t n_small_var = 0;

        for (size_t j = 0; j < d; j++) {
            const PerDimStats &st = per_dim_stats[j];
            if (st.stddev < max_std * 1e-4) {
                n_small_var++;
            }
        }

        if (n_small_var > 0) {
            do_comment ("%ld dimensions have negligible stddev wrt. "
                     "the largest dimension, they could be ignored",
                     n_small_var);
        }

    }
    comments = comment_buf.data ();
    buf = nullptr;
    nbuf = 0;
}



} // namespace faiss
