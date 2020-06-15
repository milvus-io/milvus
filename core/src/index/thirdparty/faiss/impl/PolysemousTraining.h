/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#ifndef FAISS_POLYSEMOUS_TRAINING_INCLUDED
#define FAISS_POLYSEMOUS_TRAINING_INCLUDED


#include <faiss/impl/ProductQuantizer.h>


namespace faiss {


/// parameters used for the simulated annealing method
struct SimulatedAnnealingParameters {

    // optimization parameters
    double init_temperature;   // init probaility of accepting a bad swap
    double temperature_decay;  // at each iteration the temp is multiplied by this
    int n_iter; // nb of iterations
    int n_redo; // nb of runs of the simulation
    int seed;   // random seed
    int verbose;
    bool only_bit_flips; // restrict permutation changes to bit flips
    bool init_random; // intialize with a random permutation (not identity)

    // set reasonable defaults
    SimulatedAnnealingParameters ();

};


/// abstract class for the loss function
struct PermutationObjective {

    int n;

    virtual double compute_cost (const int *perm) const = 0;

    // what would the cost update be if iw and jw were swapped?
    // default implementation just computes both and computes the difference
    virtual double cost_update (const int *perm, int iw, int jw) const;

    virtual ~PermutationObjective () {}
};


struct ReproduceDistancesObjective : PermutationObjective {

    double dis_weight_factor;

    static double sqr (double x) { return x * x; }

    // weihgting of distances: it is more important to reproduce small
    // distances well
    double dis_weight (double x) const;

    std::vector<double> source_dis; ///< "real" corrected distances (size n^2)
    const double *      target_dis; ///< wanted distances (size n^2)
    std::vector<double> weights;    ///< weights for each distance (size n^2)

    double get_source_dis (int i, int j) const;

    // cost = quadratic difference between actual distance and Hamming distance
    double compute_cost(const int* perm) const override;

    // what would the cost update be if iw and jw were swapped?
    // computed in O(n) instead of O(n^2) for the full re-computation
    double cost_update(const int* perm, int iw, int jw) const override;

    ReproduceDistancesObjective (
           int n,
           const double *source_dis_in,
           const double *target_dis_in,
           double dis_weight_factor);

    static void compute_mean_stdev (const double *tab, size_t n2,
                                    double *mean_out, double *stddev_out);

    void set_affine_target_dis (const double *source_dis_in);

    ~ReproduceDistancesObjective() override {}
};

struct RandomGenerator;

/// Simulated annealing optimization algorithm for permutations.
 struct SimulatedAnnealingOptimizer: SimulatedAnnealingParameters {

    PermutationObjective *obj;
    int n;         ///< size of the permutation
    FILE *logfile; /// logs values of the cost function

    SimulatedAnnealingOptimizer (PermutationObjective *obj,
                                 const SimulatedAnnealingParameters &p);
    RandomGenerator *rnd;

    /// remember intial cost of optimization
    double init_cost;

    // main entry point. Perform the optimization loop, starting from
    // and modifying permutation in-place
    double optimize (int *perm);

    // run the optimization and return the best result in best_perm
    double run_optimization (int * best_perm);

    virtual ~SimulatedAnnealingOptimizer ();
};




/// optimizes the order of indices in a ProductQuantizer
struct PolysemousTraining: SimulatedAnnealingParameters {

    enum Optimization_type_t {
        OT_None,
        OT_ReproduceDistances_affine,  ///< default
        OT_Ranking_weighted_diff       ///< same as _2, but use rank of y+ - rank of y-
    };
    Optimization_type_t optimization_type;

    /** use 1/4 of the training points for the optimization, with
     * max. ntrain_permutation. If ntrain_permutation == 0: train on
     * centroids */
    int ntrain_permutation;
    double dis_weight_factor; ///< decay of exp that weights distance loss

    // filename pattern for the logging of iterations
    std::string log_pattern;

    // sets default values
    PolysemousTraining ();

    /// reorder the centroids so that the Hamming distace becomes a
    /// good approximation of the SDC distance (called by train)
    void optimize_pq_for_hamming (ProductQuantizer & pq,
                                  size_t n, const float *x) const;

    /// called by optimize_pq_for_hamming
    void optimize_ranking (ProductQuantizer &pq, size_t n, const float *x) const;
    /// called by optimize_pq_for_hamming
    void optimize_reproduce_distances (ProductQuantizer &pq) const;

};


} // namespace faiss


#endif
