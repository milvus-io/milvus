/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_AUTO_TUNE_H
#define FAISS_AUTO_TUNE_H

#include <vector>
#include <unordered_map>

#include "Index.h"
#include "IndexBinary.h"

namespace faiss {


/**
 * Evaluation criterion. Returns a performance measure in [0,1],
 * higher is better.
 */
struct AutoTuneCriterion {
    typedef Index::idx_t idx_t;
    idx_t nq;  ///< nb of queries this criterion is evaluated on
    idx_t nnn; ///< nb of NNs that the query should request
    idx_t gt_nnn; ///< nb of GT NNs required to evaluate crterion

    std::vector<float> gt_D;  ///< Ground-truth distances (size nq * gt_nnn)
    std::vector<idx_t> gt_I;  ///< Ground-truth indexes (size nq * gt_nnn)

    AutoTuneCriterion (idx_t nq, idx_t nnn);

    /** Intitializes the gt_D and gt_I vectors. Must be called before evaluating
     *
     * @param gt_D_in  size nq * gt_nnn
     * @param gt_I_in  size nq * gt_nnn
     */
    void set_groundtruth (int gt_nnn, const float *gt_D_in,
                          const idx_t *gt_I_in);

    /** Evaluate the criterion.
     *
     * @param D  size nq * nnn
     * @param I  size nq * nnn
     * @return the criterion, between 0 and 1. Larger is better.
     */
    virtual double evaluate (const float *D, const idx_t *I) const = 0;

    virtual ~AutoTuneCriterion () {}

};

struct OneRecallAtRCriterion: AutoTuneCriterion {

    idx_t R;

    OneRecallAtRCriterion (idx_t nq, idx_t R);

    double evaluate(const float* D, const idx_t* I) const override;

    ~OneRecallAtRCriterion() override {}
};


struct IntersectionCriterion: AutoTuneCriterion {

    idx_t R;

    IntersectionCriterion (idx_t nq, idx_t R);

    double evaluate(const float* D, const idx_t* I) const override;

    ~IntersectionCriterion() override {}
};

/**
 * Maintains a list of experimental results. Each operating point is a
 * (perf, t, key) triplet, where higher perf and lower t is
 * better. The key field is an arbitrary identifier for the operating point
 */

struct OperatingPoint {
    double perf;     ///< performance measure (output of a Criterion)
    double t;        ///< corresponding execution time (ms)
    std::string key; ///< key that identifies this op pt
    long cno;        ///< integer identifer
};

struct OperatingPoints {
    /// all operating points
    std::vector<OperatingPoint> all_pts;

    /// optimal operating points, sorted by perf
    std::vector<OperatingPoint> optimal_pts;

    // begins with a single operating point: t=0, perf=0
    OperatingPoints ();

    /// add operating points from other to this, with a prefix to the keys
    int merge_with (const OperatingPoints &other,
                    const std::string & prefix = "");

    void clear ();

    /// add a performance measure. Return whether it is an optimal point
    bool add (double perf, double t, const std::string & key, size_t cno = 0);

    /// get time required to obtain a given performance measure
    double t_for_perf (double perf) const;

    /// easy-to-read output
    void display (bool only_optimal = true) const;

    /// output to a format easy to digest by gnuplot
    void all_to_gnuplot (const char *fname) const;
    void optimal_to_gnuplot (const char *fname) const;

};

/// possible values of a parameter, sorted from least to most expensive/accurate
struct ParameterRange {
    std::string name;
    std::vector<double> values;
};

/** Uses a-priori knowledge on the Faiss indexes to extract tunable parameters.
 */
struct ParameterSpace {
    /// all tunable parameters
    std::vector<ParameterRange> parameter_ranges;

    // exploration parameters

    /// verbosity during exploration
    int verbose;

    /// nb of experiments during optimization (0 = try all combinations)
    int n_experiments;

    /// maximum number of queries to submit at a time.
    size_t batchsize;

    /// use multithreading over batches (useful to benchmark
    /// independent single-searches)
    bool thread_over_batches;

    /// run tests several times until they reach at least this
    /// duration (to avoid jittering in MT mode)
    double min_test_duration;

    ParameterSpace ();

    /// nb of combinations, = product of values sizes
    size_t n_combinations () const;

    /// returns whether combinations c1 >= c2 in the tuple sense
    bool combination_ge (size_t c1, size_t c2) const;

    /// get string representation of the combination
    std::string combination_name (size_t cno) const;

    /// print a description on stdout
    void display () const;

    /// add a new parameter (or return it if it exists)
    ParameterRange &add_range(const char * name);

    /// initialize with reasonable parameters for the index
    virtual void initialize (const Index * index);

    /// set a combination of parameters on an index
    void set_index_parameters (Index *index, size_t cno) const;

    /// set a combination of parameters described by a string
    void set_index_parameters (Index *index, const char *param_string) const;

    /// set one of the parameters
    virtual void set_index_parameter (
        Index * index, const std::string & name, double val) const;

    /** find an upper bound on the performance and a lower bound on t
     * for configuration cno given another operating point op */
    void update_bounds (size_t cno, const OperatingPoint & op,
                        double *upper_bound_perf,
                        double *lower_bound_t) const;

    /** explore operating points
     * @param index   index to run on
     * @param xq      query vectors (size nq * index.d)
     * @param crit    selection criterion
     * @param ops     resutling operating points
     */
    void explore (Index *index,
                  size_t nq, const float *xq,
                  const AutoTuneCriterion & crit,
                  OperatingPoints * ops)  const;

    virtual ~ParameterSpace () {}
};

/** Build and index with the sequence of processing steps described in
 *  the string. */
Index *index_factory (int d, const char *description,
                      MetricType metric = METRIC_L2);

IndexBinary *index_binary_factory (int d, const char *description);


/** Reports some statistics on a dataset and comments on them.
 *
 * It is a class rather than a function so that all stats can also be
 * accessed from code */

struct MatrixStats {
    MatrixStats (size_t n, size_t d, const float *x);
    std::string comments;

    // raw statistics
    size_t n, d;
    size_t n_collision, n_valid, n0;
    double min_norm2, max_norm2;

    struct PerDimStats {
        size_t n, n_nan, n_inf, n0;

        float min, max;
        double sum, sum2;

        size_t n_valid;
        double mean, stddev;

        PerDimStats();
        void add (float x);
        void compute_mean_std ();
    };

    std::vector<PerDimStats> per_dim_stats;
    struct Occurrence {
        size_t first;
        size_t count;
    };
    std::unordered_map<uint64_t, Occurrence> occurrences;

    char *buf;
    size_t nbuf;
    void do_comment (const char *fmt, ...);

};



} // namespace faiss



#endif
