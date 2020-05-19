/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

#include <faiss/impl/PolysemousTraining.h>

#include <cstdlib>
#include <cmath>
#include <cstring>
#include <stdint.h>

#include <algorithm>

#include <faiss/utils/random.h>
#include <faiss/utils/utils.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/hamming.h>

#include <faiss/impl/FaissAssert.h>

/*****************************************
 * Mixed PQ / Hamming
 ******************************************/

namespace faiss {


/****************************************************
 * Optimization code
 ****************************************************/

SimulatedAnnealingParameters::SimulatedAnnealingParameters ()
{
    // set some reasonable defaults for the optimization
    init_temperature = 0.7;
    temperature_decay = pow (0.9, 1/500.);
    // reduce by a factor 0.9 every 500 it
    n_iter = 500000;
    n_redo = 2;
    seed = 123;
    verbose = 0;
    only_bit_flips = false;
    init_random = false;
}

// what would the cost update be if iw and jw were swapped?
// default implementation just computes both and computes the difference
double PermutationObjective::cost_update (
        const int *perm, int iw, int jw) const
{
    double orig_cost = compute_cost (perm);

    std::vector<int> perm2 (n);
    for (int i = 0; i < n; i++)
        perm2[i] = perm[i];
    perm2[iw] = perm[jw];
    perm2[jw] = perm[iw];

    double new_cost = compute_cost (perm2.data());
    return new_cost - orig_cost;
}




SimulatedAnnealingOptimizer::SimulatedAnnealingOptimizer (
        PermutationObjective *obj,
        const SimulatedAnnealingParameters &p):
    SimulatedAnnealingParameters (p),
    obj (obj),
    n(obj->n),
    logfile (nullptr)
{
    rnd = new RandomGenerator (p.seed);
    FAISS_THROW_IF_NOT (n < 100000 && n >=0 );
}

SimulatedAnnealingOptimizer::~SimulatedAnnealingOptimizer ()
{
    delete rnd;
}

// run the optimization and return the best result in best_perm
double SimulatedAnnealingOptimizer::run_optimization (int * best_perm)
{
    double min_cost = 1e30;

    // just do a few runs of the annealing and keep the lowest output cost
    for (int it = 0; it < n_redo; it++) {
        std::vector<int> perm(n);
        for (int i = 0; i < n; i++)
            perm[i] = i;
         if (init_random) {
            for (int i = 0; i < n; i++) {
                int j = i + rnd->rand_int (n - i);
                std::swap (perm[i], perm[j]);
            }
        }
         float cost = optimize (perm.data());
        if (logfile) fprintf (logfile, "\n");
        if(verbose > 1) {
            printf ("    optimization run %d: cost=%g %s\n",
                    it, cost, cost < min_cost ? "keep" : "");
        }
        if (cost < min_cost) {
            memcpy (best_perm, perm.data(), sizeof(perm[0]) * n);
            min_cost = cost;
        }
    }
     return min_cost;
}

// perform the optimization loop, starting from and modifying
// permutation in-place
double SimulatedAnnealingOptimizer::optimize (int *perm)
{
    double cost = init_cost = obj->compute_cost (perm);
    int log2n = 0;
    while (!(n <= (1 << log2n))) log2n++;
    double temperature = init_temperature;
     int n_swap = 0, n_hot = 0;
    for (int it = 0; it < n_iter; it++) {
        temperature = temperature * temperature_decay;
        int iw, jw;
        if (only_bit_flips) {
            iw = rnd->rand_int (n);
            jw = iw ^ (1 << rnd->rand_int (log2n));
        } else {
            iw = rnd->rand_int (n);
            jw = rnd->rand_int (n - 1);
            if (jw == iw) jw++;
        }
         double delta_cost = obj->cost_update (perm, iw, jw);
         if (delta_cost < 0 || rnd->rand_float () < temperature) {
            std::swap (perm[iw], perm[jw]);
            cost += delta_cost;
            n_swap++;
            if (delta_cost >= 0) n_hot++;
        }
         if (verbose > 2 || (verbose > 1 && it % 10000 == 0)) {
            printf ("      iteration %d cost %g temp %g n_swap %d "
                    "(%d hot)     \r",
                    it, cost, temperature, n_swap, n_hot);
            fflush(stdout);
        }
        if (logfile) {
            fprintf (logfile, "%d %g %g %d %d\n",
                    it, cost, temperature, n_swap, n_hot);
        }
     }
    if (verbose > 1) printf("\n");
    return cost;
}





/****************************************************
 * Cost functions: ReproduceDistanceTable
 ****************************************************/






static inline int hamming_dis (uint64_t a, uint64_t b)
{
    return __builtin_popcountl (a ^ b);
}

namespace {

/// optimize permutation to reproduce a distance table with Hamming distances
struct ReproduceWithHammingObjective : PermutationObjective {
    int nbits;
    double dis_weight_factor;

    static double sqr (double x) { return x * x; }


    // weihgting of distances: it is more important to reproduce small
    // distances well
    double dis_weight (double x) const
    {
        return exp (-dis_weight_factor * x);
    }

    std::vector<double> target_dis; // wanted distances (size n^2)
    std::vector<double> weights;    // weights for each distance (size n^2)

    // cost = quadratic difference between actual distance and Hamming distance
    double compute_cost(const int* perm) const override {
      double cost = 0;
      for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
          double wanted = target_dis[i * n + j];
          double w = weights[i * n + j];
          double actual = hamming_dis(perm[i], perm[j]);
          cost += w * sqr(wanted - actual);
        }
      }
      return cost;
    }


    // what would the cost update be if iw and jw were swapped?
    // computed in O(n) instead of O(n^2) for the full re-computation
    double cost_update(const int* perm, int iw, int jw) const override {
      double delta_cost = 0;

      for (int i = 0; i < n; i++) {
        if (i == iw) {
          for (int j = 0; j < n; j++) {
            double wanted = target_dis[i * n + j], w = weights[i * n + j];
            double actual = hamming_dis(perm[i], perm[j]);
            delta_cost -= w * sqr(wanted - actual);
            double new_actual =
                hamming_dis(perm[jw], perm[j == iw ? jw : j == jw ? iw : j]);
            delta_cost += w * sqr(wanted - new_actual);
          }
        } else if (i == jw) {
          for (int j = 0; j < n; j++) {
            double wanted = target_dis[i * n + j], w = weights[i * n + j];
            double actual = hamming_dis(perm[i], perm[j]);
            delta_cost -= w * sqr(wanted - actual);
            double new_actual =
                hamming_dis(perm[iw], perm[j == iw ? jw : j == jw ? iw : j]);
            delta_cost += w * sqr(wanted - new_actual);
          }
        } else {
          int j = iw;
          {
            double wanted = target_dis[i * n + j], w = weights[i * n + j];
            double actual = hamming_dis(perm[i], perm[j]);
            delta_cost -= w * sqr(wanted - actual);
            double new_actual = hamming_dis(perm[i], perm[jw]);
            delta_cost += w * sqr(wanted - new_actual);
          }
          j = jw;
          {
            double wanted = target_dis[i * n + j], w = weights[i * n + j];
            double actual = hamming_dis(perm[i], perm[j]);
            delta_cost -= w * sqr(wanted - actual);
            double new_actual = hamming_dis(perm[i], perm[iw]);
            delta_cost += w * sqr(wanted - new_actual);
          }
        }
      }

      return delta_cost;
    }



    ReproduceWithHammingObjective (
           int nbits,
           const std::vector<double> & dis_table,
           double dis_weight_factor):
        nbits (nbits), dis_weight_factor (dis_weight_factor)
    {
        n = 1 << nbits;
        FAISS_THROW_IF_NOT (dis_table.size() == n * n);
        set_affine_target_dis (dis_table);
    }

    void set_affine_target_dis (const std::vector<double> & dis_table)
    {
        double sum = 0, sum2 = 0;
        int n2 = n * n;
        for (int i = 0; i < n2; i++) {
            sum += dis_table [i];
            sum2 += dis_table [i] * dis_table [i];
        }
        double mean = sum / n2;
        double stddev = sqrt(sum2 / n2 - (sum / n2) * (sum / n2));

        target_dis.resize (n2);

        for (int i = 0; i < n2; i++) {
            // the mapping function
            double td = (dis_table [i] - mean) / stddev * sqrt(nbits / 4) +
                nbits / 2;
            target_dis[i] = td;
            // compute a weight
            weights.push_back (dis_weight (td));
        }

    }

    ~ReproduceWithHammingObjective() override {}
};

} // anonymous namespace

// weihgting of distances: it is more important to reproduce small
// distances well
double ReproduceDistancesObjective::dis_weight (double x) const
{
    return exp (-dis_weight_factor * x);
}


double ReproduceDistancesObjective::get_source_dis (int i, int j) const
{
    return source_dis [i * n + j];
}

// cost = quadratic difference between actual distance and Hamming distance
double ReproduceDistancesObjective::compute_cost (const int *perm) const
{
    double cost = 0;
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            double wanted = target_dis [i * n + j];
            double w = weights [i * n + j];
            double actual = get_source_dis (perm[i], perm[j]);
            cost += w * sqr (wanted - actual);
        }
    }
    return cost;
}

// what would the cost update be if iw and jw were swapped?
// computed in O(n) instead of O(n^2) for the full re-computation
double ReproduceDistancesObjective::cost_update(
        const int *perm, int iw, int jw) const
{
    double delta_cost = 0;
     for (int i = 0; i < n; i++) {
        if (i == iw) {
            for (int j = 0; j < n; j++) {
                double wanted = target_dis [i * n + j],
                    w = weights [i * n + j];
                double actual = get_source_dis (perm[i], perm[j]);
                delta_cost -= w * sqr (wanted - actual);
                double new_actual = get_source_dis (
                       perm[jw],
                       perm[j == iw ? jw : j == jw ? iw : j]);
                delta_cost += w * sqr (wanted - new_actual);
            }
        } else if (i == jw) {
            for (int j = 0; j < n; j++) {
                double wanted = target_dis [i * n + j],
                    w = weights [i * n + j];
                double actual = get_source_dis (perm[i], perm[j]);
                delta_cost -= w * sqr (wanted - actual);
                double new_actual = get_source_dis (
                       perm[iw],
                       perm[j == iw ? jw : j == jw ? iw : j]);
                delta_cost += w * sqr (wanted - new_actual);
            }
        } else  {
            int j = iw;
            {
                double wanted = target_dis [i * n + j],
                    w = weights [i * n + j];
                double actual = get_source_dis (perm[i], perm[j]);
                delta_cost -= w * sqr (wanted - actual);
                double new_actual = get_source_dis (perm[i], perm[jw]);
                delta_cost += w * sqr (wanted - new_actual);
            }
            j = jw;
            {
                double wanted = target_dis [i * n + j],
                    w = weights [i * n + j];
                double actual = get_source_dis (perm[i], perm[j]);
                delta_cost -= w * sqr (wanted - actual);
                double new_actual = get_source_dis (perm[i], perm[iw]);
                delta_cost += w * sqr (wanted - new_actual);
            }
        }
    }
     return delta_cost;
}



ReproduceDistancesObjective::ReproduceDistancesObjective (
       int n,
       const double *source_dis_in,
       const double *target_dis_in,
       double dis_weight_factor):
    dis_weight_factor (dis_weight_factor),
    target_dis (target_dis_in)
{
    this->n = n;
    set_affine_target_dis (source_dis_in);
}

void ReproduceDistancesObjective::compute_mean_stdev (
          const double *tab, size_t n2,
          double *mean_out, double *stddev_out)
{
    double sum = 0, sum2 = 0;
    for (int i = 0; i < n2; i++) {
        sum += tab [i];
        sum2 += tab [i] * tab [i];
    }
    double mean = sum / n2;
    double stddev = sqrt(sum2 / n2 - (sum / n2) * (sum / n2));
    *mean_out = mean;
    *stddev_out = stddev;
}

void ReproduceDistancesObjective::set_affine_target_dis (
          const double *source_dis_in)
{
    int n2 = n * n;

    double mean_src, stddev_src;
    compute_mean_stdev (source_dis_in, n2, &mean_src, &stddev_src);

    double mean_target, stddev_target;
    compute_mean_stdev (target_dis, n2, &mean_target, &stddev_target);

    printf ("map mean %g std %g -> mean %g std %g\n",
            mean_src, stddev_src, mean_target, stddev_target);

    source_dis.resize (n2);
    weights.resize (n2);

    for (int i = 0; i < n2; i++) {
        // the mapping function
        source_dis[i] = (source_dis_in[i] - mean_src) / stddev_src
            * stddev_target + mean_target;

        // compute a weight
        weights [i] = dis_weight (target_dis[i]);
    }

}

/****************************************************
 * Cost functions: RankingScore
 ****************************************************/

/// Maintains a 3D table of elementary costs.
/// Accumulates elements based on Hamming distance comparisons
template <typename Ttab, typename Taccu>
struct Score3Computer: PermutationObjective {

    int nc;

    // cost matrix of size nc * nc *nc
    // n_gt (i,j,k) = count of d_gt(x, y-) < d_gt(x, y+)
    // where x has PQ code i, y- PQ code j and y+ PQ code k
    std::vector<Ttab> n_gt;


    /// the cost is a triple loop on the nc * nc * nc matrix of entries.
    ///
    Taccu compute (const int * perm) const
    {
        Taccu accu = 0;
        const Ttab *p = n_gt.data();
        for (int i = 0; i < nc; i++) {
            int ip = perm [i];
            for (int j = 0; j < nc; j++) {
                int jp = perm [j];
                for (int k = 0; k < nc; k++) {
                    int kp = perm [k];
                    if (hamming_dis (ip, jp) <
                        hamming_dis (ip, kp)) {
                        accu += *p; // n_gt [ ( i * nc + j) * nc + k];
                    }
                    p++;
                }
            }
        }
        return accu;
    }


    /** cost update if entries iw and jw of the permutation would be
     * swapped.
     *
     * The computation is optimized by avoiding elements in the
     * nc*nc*nc cube that are known not to change. For nc=256, this
     * reduces the nb of cells to visit to about 6/256 th of the
     * cells. Practical speedup is about 8x, and the code is quite
     * complex :-/
     */
    Taccu compute_update (const int *perm, int iw, int jw) const
    {
        assert (iw != jw);
        if (iw > jw) std::swap (iw, jw);

        Taccu accu = 0;
        const Ttab * n_gt_i = n_gt.data();
        for (int i = 0; i < nc; i++) {
            int ip0 = perm [i];
            int ip = perm [i == iw ? jw : i == jw ? iw : i];

            //accu += update_i (perm, iw, jw, ip0, ip, n_gt_i);

            accu += update_i_cross (perm, iw, jw,
                                    ip0, ip, n_gt_i);

            if (ip != ip0)
                accu += update_i_plane (perm, iw, jw,
                                       ip0, ip, n_gt_i);

            n_gt_i += nc * nc;
        }

        return accu;
    }


    Taccu update_i (const int *perm, int iw, int jw,
                   int ip0, int ip, const Ttab * n_gt_i) const
    {
        Taccu accu = 0;
        const Ttab *n_gt_ij = n_gt_i;
        for (int j = 0; j < nc; j++) {
            int jp0 = perm[j];
            int jp = perm [j == iw ? jw : j == jw ? iw : j];
            for (int k = 0; k < nc; k++) {
                int kp0 = perm [k];
                int kp = perm [k == iw ? jw : k == jw ? iw : k];
                int ng = n_gt_ij [k];
                if (hamming_dis (ip, jp) < hamming_dis (ip, kp)) {
                    accu += ng;
                }
                if (hamming_dis (ip0, jp0) < hamming_dis (ip0, kp0)) {
                    accu -= ng;
                }
            }
            n_gt_ij += nc;
        }
        return accu;
    }

    // 2 inner loops for the case ip0 != ip
    Taccu update_i_plane (const int *perm, int iw, int jw,
                         int ip0, int ip, const Ttab * n_gt_i) const
    {
        Taccu accu = 0;
        const Ttab *n_gt_ij = n_gt_i;

        for (int j = 0; j < nc; j++) {
            if (j != iw && j != jw) {
                int jp = perm[j];
                for (int k = 0; k < nc; k++) {
                    if (k != iw && k != jw) {
                        int kp = perm [k];
                        Ttab ng = n_gt_ij [k];
                        if (hamming_dis (ip, jp) < hamming_dis (ip, kp)) {
                            accu += ng;
                        }
                        if (hamming_dis (ip0, jp) < hamming_dis (ip0, kp)) {
                            accu -= ng;
                        }
                    }
                }
            }
            n_gt_ij += nc;
        }
        return accu;
    }

    /// used for the 8 cells were the 3 indices are swapped
    inline Taccu update_k (const int *perm, int iw, int jw,
                          int ip0, int ip, int jp0, int jp,
                          int k,
                          const Ttab * n_gt_ij) const
    {
        Taccu accu = 0;
        int kp0 = perm [k];
        int kp = perm [k == iw ? jw : k == jw ? iw : k];
        Ttab ng = n_gt_ij [k];
        if (hamming_dis (ip, jp) < hamming_dis (ip, kp)) {
            accu += ng;
        }
        if (hamming_dis (ip0, jp0) < hamming_dis (ip0, kp0)) {
            accu -= ng;
        }
        return accu;
    }

    /// compute update on a line of k's, where i and j are swapped
    Taccu update_j_line (const int *perm, int iw, int jw,
                        int ip0, int ip, int jp0, int jp,
                        const Ttab * n_gt_ij) const
    {
        Taccu accu = 0;
        for (int k = 0; k < nc; k++) {
            if (k == iw || k == jw) continue;
            int kp = perm [k];
            Ttab ng = n_gt_ij [k];
            if (hamming_dis (ip, jp) < hamming_dis (ip, kp)) {
                accu += ng;
            }
            if (hamming_dis (ip0, jp0) < hamming_dis (ip0, kp)) {
                accu -= ng;
            }
        }
        return accu;
    }


    /// considers the 2 pairs of crossing lines j=iw or jw and k = iw or kw
    Taccu update_i_cross (const int *perm, int iw, int jw,
                        int ip0, int ip, const Ttab * n_gt_i) const
    {
        Taccu accu = 0;
        const Ttab *n_gt_ij = n_gt_i;

        for (int j = 0; j < nc; j++) {
            int jp0 = perm[j];
            int jp = perm [j == iw ? jw : j == jw ? iw : j];

            accu += update_k (perm, iw, jw, ip0, ip, jp0, jp, iw, n_gt_ij);
            accu += update_k (perm, iw, jw, ip0, ip, jp0, jp, jw, n_gt_ij);

            if (jp != jp0)
                accu += update_j_line (perm, iw, jw, ip0, ip, jp0, jp, n_gt_ij);

            n_gt_ij += nc;
        }
        return accu;
    }


    /// PermutationObjective implementeation (just negates the scores
    /// for minimization)

    double compute_cost(const int* perm) const override {
      return -compute(perm);
    }

    double cost_update(const int* perm, int iw, int jw) const override {
      double ret = -compute_update(perm, iw, jw);
      return ret;
    }

    ~Score3Computer() override {}
};





struct IndirectSort {
    const float *tab;
    bool operator () (int a, int b) {return tab[a] < tab[b]; }
};



struct RankingScore2: Score3Computer<float, double> {
    int nbits;
    int nq, nb;
    const uint32_t *qcodes, *bcodes;
    const float *gt_distances;

    RankingScore2 (int nbits, int nq, int nb,
                  const uint32_t *qcodes, const uint32_t *bcodes,
                  const float *gt_distances):
        nbits(nbits), nq(nq), nb(nb), qcodes(qcodes),
        bcodes(bcodes), gt_distances(gt_distances)
    {
        n = nc = 1 << nbits;
        n_gt.resize (nc * nc * nc);
        init_n_gt ();
    }


    double rank_weight (int r)
    {
        return 1.0 / (r + 1);
    }

    /// count nb of i, j in a x b st. i < j
    /// a and b should be sorted on input
    /// they are the ranks of j and k respectively.
    /// specific version for diff-of-rank weighting, cannot optimized
    /// with a cumulative table
    double accum_gt_weight_diff (const std::vector<int> & a,
                                 const std::vector<int> & b)
    {
        int nb = b.size(), na = a.size();

        double accu = 0;
        int j = 0;
        for (int i = 0; i < na; i++) {
            int ai = a[i];
            while (j < nb && ai >= b[j]) j++;

            double accu_i = 0;
            for (int k = j; k < b.size(); k++)
                accu_i += rank_weight (b[k] - ai);

            accu += rank_weight (ai) * accu_i;

        }
        return accu;
    }

    void init_n_gt ()
    {
        for (int q = 0; q < nq; q++) {
            const float *gtd = gt_distances + q * nb;
            const uint32_t *cb = bcodes;// all same codes
            float * n_gt_q = & n_gt [qcodes[q] * nc * nc];

            printf("init gt for q=%d/%d    \r", q, nq); fflush(stdout);

            std::vector<int> rankv (nb);
            int * ranks = rankv.data();

            // elements in each code bin, ordered by rank within each bin
            std::vector<std::vector<int> > tab (nc);

            { // build rank table
                IndirectSort s = {gtd};
                for (int j = 0; j < nb; j++) ranks[j] = j;
                std::sort (ranks, ranks + nb, s);
            }

            for (int rank = 0; rank < nb; rank++) {
                int i = ranks [rank];
                tab [cb[i]].push_back (rank);
            }


            // this is very expensive. Any suggestion for improvement
            // welcome.
            for (int i = 0; i < nc; i++) {
                std::vector<int> & di = tab[i];
                for (int j = 0; j < nc; j++) {
                    std::vector<int> & dj = tab[j];
                    n_gt_q [i * nc + j] += accum_gt_weight_diff (di, dj);

                }
            }

        }

    }

};


/*****************************************
 * PolysemousTraining
 ******************************************/



PolysemousTraining::PolysemousTraining ()
{
    optimization_type = OT_ReproduceDistances_affine;
    ntrain_permutation = 0;
    dis_weight_factor = log(2);
}



void PolysemousTraining::optimize_reproduce_distances (
       ProductQuantizer &pq) const
{

    int dsub = pq.dsub;

    int n = pq.ksub;
    int nbits = pq.nbits;

#pragma omp parallel for
    for (int m = 0; m < pq.M; m++) {
        std::vector<double> dis_table;

        // printf ("Optimizing quantizer %d\n", m);

        float * centroids = pq.get_centroids (m, 0);

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                dis_table.push_back (fvec_L2sqr (centroids + i * dsub,
                                                 centroids + j * dsub,
                                                 dsub));
            }
        }

        std::vector<int> perm (n);
        ReproduceWithHammingObjective obj (
               nbits, dis_table,
               dis_weight_factor);


        SimulatedAnnealingOptimizer optim (&obj, *this);

        if (log_pattern.size()) {
            char fname[256];
            snprintf (fname, 256, log_pattern.c_str(), m);
            printf ("opening log file %s\n", fname);
            optim.logfile = fopen (fname, "w");
            FAISS_THROW_IF_NOT_MSG (optim.logfile, "could not open logfile");
        }
        double final_cost = optim.run_optimization (perm.data());

        if (verbose > 0) {
            printf ("SimulatedAnnealingOptimizer for m=%d: %g -> %g\n",
                    m, optim.init_cost, final_cost);
        }

        if (log_pattern.size()) fclose (optim.logfile);

        std::vector<float> centroids_copy;
        for (int i = 0; i < dsub * n; i++)
            centroids_copy.push_back (centroids[i]);

        for (int i = 0; i < n; i++)
            memcpy (centroids + perm[i] * dsub,
                    centroids_copy.data() + i * dsub,
                    dsub * sizeof(centroids[0]));

    }

}


void PolysemousTraining::optimize_ranking (
      ProductQuantizer &pq, size_t n, const float *x) const
{

    int dsub = pq.dsub;

    int nbits = pq.nbits;

    std::vector<uint8_t> all_codes (pq.code_size * n);

    pq.compute_codes (x, all_codes.data(), n);

    FAISS_THROW_IF_NOT (pq.nbits == 8);

    if (n == 0)
        pq.compute_sdc_table ();

#pragma omp parallel for
    for (int m = 0; m < pq.M; m++) {
        size_t nq, nb;
        std::vector <uint32_t> codes; // query codes, then db codes
        std::vector <float> gt_distances; // nq * nb matrix of distances

        if (n > 0) {
            std::vector<float> xtrain (n * dsub);
            for (int i = 0; i < n; i++)
                memcpy (xtrain.data() + i * dsub,
                        x + i * pq.d + m * dsub,
                        sizeof(float) * dsub);

            codes.resize (n);
            for (int i = 0; i < n; i++)
                codes [i] = all_codes [i * pq.code_size + m];

            nq = n / 4; nb = n - nq;
            const float *xq = xtrain.data();
            const float *xb = xq + nq * dsub;

            gt_distances.resize (nq * nb);

            pairwise_L2sqr (dsub,
                            nq, xq,
                            nb, xb,
                            gt_distances.data());
        } else {
            nq = nb = pq.ksub;
            codes.resize (2 * nq);
            for (int i = 0; i < nq; i++)
                codes[i] = codes [i + nq] = i;

            gt_distances.resize (nq * nb);

            memcpy (gt_distances.data (),
                    pq.sdc_table.data () + m * nq * nb,
                    sizeof (float) * nq * nb);
        }

        double t0 = getmillisecs ();

        PermutationObjective *obj = new RankingScore2 (
                  nbits, nq, nb,
                  codes.data(), codes.data() + nq,
                  gt_distances.data ());
        ScopeDeleter1<PermutationObjective> del (obj);

        if (verbose > 0) {
            printf("   m=%d, nq=%ld, nb=%ld, intialize RankingScore "
                   "in %.3f ms\n",
                   m, nq, nb, getmillisecs () - t0);
        }

        SimulatedAnnealingOptimizer optim (obj, *this);

        if (log_pattern.size()) {
            char fname[256];
            snprintf (fname, 256, log_pattern.c_str(), m);
            printf ("opening log file %s\n", fname);
            optim.logfile = fopen (fname, "w");
            FAISS_THROW_IF_NOT_FMT (optim.logfile,
                                    "could not open logfile %s", fname);
        }

        std::vector<int> perm (pq.ksub);

        double final_cost = optim.run_optimization (perm.data());
        printf ("SimulatedAnnealingOptimizer for m=%d: %g -> %g\n",
                m, optim.init_cost, final_cost);

        if (log_pattern.size()) fclose (optim.logfile);

        float * centroids = pq.get_centroids (m, 0);

        std::vector<float> centroids_copy;
        for (int i = 0; i < dsub * pq.ksub; i++)
            centroids_copy.push_back (centroids[i]);

        for (int i = 0; i < pq.ksub; i++)
            memcpy (centroids + perm[i] * dsub,
                    centroids_copy.data() + i * dsub,
                    dsub * sizeof(centroids[0]));

    }

}



void PolysemousTraining::optimize_pq_for_hamming (ProductQuantizer &pq,
                                                size_t n, const float *x) const
{
    if (optimization_type == OT_None) {

    } else if (optimization_type == OT_ReproduceDistances_affine) {
        optimize_reproduce_distances (pq);
    } else {
        optimize_ranking (pq, n, x);
    }

    pq.compute_sdc_table ();

}


} // namespace faiss
