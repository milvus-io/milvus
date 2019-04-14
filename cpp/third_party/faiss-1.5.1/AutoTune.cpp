/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD+Patents license found in the
 * LICENSE file in the root directory of this source tree.
 */

// -*- c++ -*-

/*
 * implementation of Hyper-parameter auto-tuning
 */

#include "AutoTune.h"

#include <cmath>
#include <stdarg.h>     /* va_list, va_start, va_arg, va_end */


#include "FaissAssert.h"
#include "utils.h"

#include "IndexFlat.h"
#include "VectorTransform.h"
#include "IndexLSH.h"
#include "IndexPQ.h"
#include "IndexIVF.h"
#include "IndexIVFPQ.h"
#include "IndexIVFFlat.h"
#include "MetaIndexes.h"
#include "IndexScalarQuantizer.h"
#include "IndexHNSW.h"
#include "IndexBinaryFlat.h"
#include "IndexBinaryHNSW.h"
#include "IndexBinaryIVF.h"

namespace faiss {


AutoTuneCriterion::AutoTuneCriterion (idx_t nq, idx_t nnn):
    nq (nq), nnn (nnn), gt_nnn (0)
{}


void AutoTuneCriterion::set_groundtruth (
     int gt_nnn, const float *gt_D_in, const idx_t *gt_I_in)
{
    this->gt_nnn = gt_nnn;
    if (gt_D_in) { // allow null for this, as it is often not used
        gt_D.resize (nq * gt_nnn);
        memcpy (gt_D.data(), gt_D_in, sizeof (gt_D[0]) * nq * gt_nnn);
    }
    gt_I.resize (nq * gt_nnn);
    memcpy (gt_I.data(), gt_I_in, sizeof (gt_I[0]) * nq * gt_nnn);
}



OneRecallAtRCriterion::OneRecallAtRCriterion (idx_t nq, idx_t R):
    AutoTuneCriterion(nq, R), R(R)
{}

double OneRecallAtRCriterion::evaluate(const float* /*D*/, const idx_t* I)
    const {
  FAISS_THROW_IF_NOT_MSG(
      (gt_I.size() == gt_nnn * nq && gt_nnn >= 1 && nnn >= R),
      "ground truth not initialized");
  idx_t n_ok = 0;
  for (idx_t q = 0; q < nq; q++) {
    idx_t gt_nn = gt_I[q * gt_nnn];
    const idx_t* I_line = I + q * nnn;
    for (int i = 0; i < R; i++) {
      if (I_line[i] == gt_nn) {
        n_ok++;
        break;
      }
    }
  }
  return n_ok / double(nq);
}


IntersectionCriterion::IntersectionCriterion (idx_t nq, idx_t R):
    AutoTuneCriterion(nq, R), R(R)
{}

double IntersectionCriterion::evaluate(const float* /*D*/, const idx_t* I)
    const {
    FAISS_THROW_IF_NOT_MSG(
      (gt_I.size() == gt_nnn * nq && gt_nnn >= R && nnn >= R),
      "ground truth not initialized");
    long n_ok = 0;
#pragma omp parallel for reduction(+: n_ok)
    for (idx_t q = 0; q < nq; q++) {
        n_ok += ranklist_intersection_size (
             R, &gt_I [q * gt_nnn],
             R, I + q * nnn);
    }
    return n_ok / double (nq * R);
}

/***************************************************************
 * OperatingPoints
 ***************************************************************/

OperatingPoints::OperatingPoints ()
{
    clear();
}

void OperatingPoints::clear ()
{
    all_pts.clear();
    optimal_pts.clear();
    /// default point: doing nothing gives 0 performance and takes 0 time
    OperatingPoint op = {0, 0, "", -1};
    optimal_pts.push_back(op);
}

/// add a performance measure
bool OperatingPoints::add (double perf, double t, const std::string & key,
                           size_t cno)
{
    OperatingPoint op = {perf, t, key, long(cno)};
    all_pts.push_back (op);
    if (perf == 0) {
        return false;  // no method for 0 accuracy is faster than doing nothing
    }
    std::vector<OperatingPoint> & a = optimal_pts;
    if (perf > a.back().perf) {
        // keep unconditionally
        a.push_back (op);
    } else if (perf == a.back().perf) {
        if (t < a.back ().t) {
            a.back() = op;
        } else {
            return false;
        }
    } else {
        int i;
        // stricto sensu this should be a bissection
        for (i = 0; i < a.size(); i++) {
            if (a[i].perf >= perf) break;
        }
        assert (i < a.size());
        if (t < a[i].t) {
            if (a[i].perf == perf) {
                a[i] = op;
            } else {
                a.insert (a.begin() + i, op);
            }
        } else {
            return false;
        }
    }
    { // remove non-optimal points from array
        int i = a.size() - 1;
        while (i > 0) {
            if (a[i].t < a[i - 1].t)
                a.erase (a.begin() + (i - 1));
            i--;
        }
    }
    return true;
}


int OperatingPoints::merge_with (const OperatingPoints &other,
                                 const std::string & prefix)
{
    int n_add = 0;
    for (int i = 0; i < other.all_pts.size(); i++) {
        const OperatingPoint & op = other.all_pts[i];
        if (add (op.perf, op.t, prefix + op.key, op.cno))
            n_add++;
    }
    return n_add;
}



/// get time required to obtain a given performance measure
double OperatingPoints::t_for_perf (double perf) const
{
    const std::vector<OperatingPoint> & a = optimal_pts;
    if (perf > a.back().perf) return 1e50;
    int i0 = -1, i1 = a.size() - 1;
    while (i0 + 1 < i1) {
        int imed = (i0 + i1 + 1) / 2;
        if (a[imed].perf < perf) i0 = imed;
        else                     i1 = imed;
    }
    return a[i1].t;
}


void OperatingPoints::all_to_gnuplot (const char *fname) const
{
    FILE *f = fopen(fname, "w");
    if (!f) {
        fprintf (stderr, "cannot open %s", fname);
        perror("");
        abort();
    }
    for (int i = 0; i < all_pts.size(); i++) {
        const OperatingPoint & op = all_pts[i];
        fprintf (f, "%g %g %s\n", op.perf, op.t, op.key.c_str());
    }
    fclose(f);
}

void OperatingPoints::optimal_to_gnuplot (const char *fname) const
{
    FILE *f = fopen(fname, "w");
    if (!f) {
        fprintf (stderr, "cannot open %s", fname);
        perror("");
        abort();
    }
    double prev_perf = 0.0;
    for (int i = 0; i < optimal_pts.size(); i++) {
        const OperatingPoint & op = optimal_pts[i];
        fprintf (f, "%g %g\n", prev_perf, op.t);
        fprintf (f, "%g %g %s\n", op.perf, op.t, op.key.c_str());
        prev_perf = op.perf;
    }
    fclose(f);
}

void OperatingPoints::display (bool only_optimal) const
{
    const std::vector<OperatingPoint> &pts =
        only_optimal ? optimal_pts : all_pts;
    printf("Tested %ld operating points, %ld ones are optimal:\n",
           all_pts.size(), optimal_pts.size());

    for (int i = 0; i < pts.size(); i++) {
        const OperatingPoint & op = pts[i];
        const char *star = "";
        if (!only_optimal) {
            for (int j = 0; j < optimal_pts.size(); j++) {
                if (op.cno == optimal_pts[j].cno) {
                    star = "*";
                    break;
                }
            }
        }
        printf ("cno=%ld key=%s perf=%.4f t=%.3f %s\n",
                op.cno, op.key.c_str(), op.perf, op.t, star);
    }

}

/***************************************************************
 * ParameterSpace
 ***************************************************************/

ParameterSpace::ParameterSpace ():
    verbose (1), n_experiments (500),
    batchsize (1<<30), thread_over_batches (false),
    min_test_duration (0)
{
}

/* not keeping this constructor as inheritors will call the parent
   initialize()
 */

#if 0
ParameterSpace::ParameterSpace (Index *index):
    verbose (1), n_experiments (500),
    batchsize (1<<30), thread_over_batches (false)

{
    initialize(index);
}
#endif

size_t ParameterSpace::n_combinations () const
{
    size_t n = 1;
    for (int i = 0; i < parameter_ranges.size(); i++)
        n *= parameter_ranges[i].values.size();
    return n;
}

/// get string representation of the combination
std::string ParameterSpace::combination_name (size_t cno) const {
    char buf[1000], *wp = buf;
    *wp = 0;
    for (int i = 0; i < parameter_ranges.size(); i++) {
        const ParameterRange & pr = parameter_ranges[i];
        size_t j = cno % pr.values.size();
        cno /= pr.values.size();
        wp += snprintf (
              wp, buf + 1000 - wp, "%s%s=%g", i == 0 ? "" : ",",
              pr.name.c_str(), pr.values[j]);
    }
    return std::string (buf);
}


bool ParameterSpace::combination_ge (size_t c1, size_t c2) const
{
    for (int i = 0; i < parameter_ranges.size(); i++) {
        int nval = parameter_ranges[i].values.size();
        size_t j1 = c1 % nval;
        size_t j2 = c2 % nval;
        if (!(j1 >= j2)) return false;
        c1 /= nval;
        c2 /= nval;
    }
    return true;
}



#define DC(classname) \
    const classname *ix = dynamic_cast<const classname *>(index)

static void init_pq_ParameterRange (const ProductQuantizer & pq,
                                    ParameterRange & pr)
{
    if (pq.code_size % 4 == 0) {
        // Polysemous not supported for code sizes that are not a
        // multiple of 4
        for (int i = 2; i <= pq.code_size * 8 / 2; i+= 2)
            pr.values.push_back(i);
    }
    pr.values.push_back (pq.code_size * 8);
}

ParameterRange &ParameterSpace::add_range(const char * name)
{
    for (auto & pr : parameter_ranges) {
        if (pr.name == name) {
            return pr;
        }
    }
    parameter_ranges.push_back (ParameterRange ());
    parameter_ranges.back ().name = name;
    return parameter_ranges.back ();
}


/// initialize with reasonable parameters for the index
void ParameterSpace::initialize (const Index * index)
{
    if (DC (IndexPreTransform)) {
        index = ix->index;
    }
    if (DC (IndexRefineFlat)) {
        ParameterRange & pr = add_range("k_factor_rf");
        for (int i = 0; i <= 6; i++) {
            pr.values.push_back (1 << i);
        }
        index = ix->base_index;
    }
    if (DC (IndexPreTransform)) {
        index = ix->index;
    }

    if (DC (IndexIVF)) {
        {
            ParameterRange & pr = add_range("nprobe");
            for (int i = 0; i < 13; i++) {
                size_t nprobe = 1 << i;
                if (nprobe >= ix->nlist) break;
                pr.values.push_back (nprobe);
            }
        }
        if (dynamic_cast<const IndexHNSW*>(ix->quantizer)) {
            ParameterRange & pr = add_range("efSearch");
            for (int i = 2; i <= 9; i++) {
                pr.values.push_back (1 << i);
            }
        }
    }
    if (DC (IndexPQ)) {
        ParameterRange & pr = add_range("ht");
        init_pq_ParameterRange (ix->pq, pr);
    }
    if (DC (IndexIVFPQ)) {
        ParameterRange & pr = add_range("ht");
        init_pq_ParameterRange (ix->pq, pr);
    }

    if (DC (IndexIVF)) {
        const MultiIndexQuantizer *miq =
            dynamic_cast<const MultiIndexQuantizer *> (ix->quantizer);
        if (miq) {
            ParameterRange & pr_max_codes = add_range("max_codes");
            for (int i = 8; i < 20; i++) {
                pr_max_codes.values.push_back (1 << i);
            }
            pr_max_codes.values.push_back (
                std::numeric_limits<double>::infinity()
            );
        }
    }
    if (DC (IndexIVFPQR)) {
        ParameterRange & pr = add_range("k_factor");
        for (int i = 0; i <= 6; i++) {
            pr.values.push_back (1 << i);
        }
    }
    if (dynamic_cast<const IndexHNSW*>(index)) {
        ParameterRange & pr = add_range("efSearch");
        for (int i = 2; i <= 9; i++) {
            pr.values.push_back (1 << i);
        }
    }
}

#undef DC

// non-const version
#define DC(classname) classname *ix = dynamic_cast<classname *>(index)


/// set a combination of parameters on an index
void ParameterSpace::set_index_parameters (Index *index, size_t cno) const
{

    for (int i = 0; i < parameter_ranges.size(); i++) {
        const ParameterRange & pr = parameter_ranges[i];
        size_t j = cno % pr.values.size();
        cno /= pr.values.size();
        double val = pr.values [j];
        set_index_parameter (index, pr.name, val);
    }
}

/// set a combination of parameters on an index
void ParameterSpace::set_index_parameters (
     Index *index, const char *description_in) const
{
    char description[strlen(description_in) + 1];
    char *ptr;
    memcpy (description, description_in, strlen(description_in) + 1);

    for (char *tok = strtok_r (description, " ,", &ptr);
         tok;
         tok = strtok_r (nullptr, " ,", &ptr)) {
        char name[100];
        double val;
        int ret = sscanf (tok, "%100[^=]=%lf", name, &val);
        FAISS_THROW_IF_NOT_FMT (
           ret == 2, "could not interpret parameters %s", tok);
        set_index_parameter (index, name, val);
    }

}

void ParameterSpace::set_index_parameter (
        Index * index, const std::string & name, double val) const
{
    if (verbose > 1)
        printf("    set %s=%g\n", name.c_str(), val);

    if (name == "verbose") {
        index->verbose = int(val);
        // and fall through to also enable it on sub-indexes
    }
    if (DC (IndexPreTransform)) {
        set_index_parameter (ix->index, name, val);
        return;
    }
    if (DC (IndexShards)) {
        // call on all sub-indexes
        for (auto & shard_index : ix->shard_indexes) {
            set_index_parameter (shard_index, name, val);
        }
        return;
    }
    if (DC (IndexRefineFlat)) {
        if (name == "k_factor_rf") {
            ix->k_factor = int(val);
            return;
        }
        // otherwise it is for the sub-index
        set_index_parameter (&ix->refine_index, name, val);
        return;
    }

    if (name == "verbose") {
        index->verbose = int(val);
        return; // last verbose that we could find
    }

    if (name == "nprobe") {
        if (DC (IndexIDMap)) {
            set_index_parameter (ix->index, name, val);
            return;
        } else if (DC (IndexIVF)) {
            ix->nprobe = int(val);
            return;
        }
    }

    if (name == "ht") {
        if (DC (IndexPQ)) {
            if (val >= ix->pq.code_size * 8) {
                ix->search_type = IndexPQ::ST_PQ;
            } else {
                ix->search_type = IndexPQ::ST_polysemous;
                ix->polysemous_ht = int(val);
            }
            return;
        } else if (DC (IndexIVFPQ)) {
            if (val >= ix->pq.code_size * 8) {
                ix->polysemous_ht = 0;
            } else {
                ix->polysemous_ht = int(val);
            }
            return;
        }
    }

    if (name == "k_factor") {
        if (DC (IndexIVFPQR)) {
            ix->k_factor = val;
            return;
        }
    }
    if (name == "max_codes") {
        if (DC (IndexIVF)) {
            ix->max_codes = std::isfinite(val) ? size_t(val) : 0;
            return;
        }
    }

    if (name == "efSearch") {
        if (DC (IndexHNSW)) {
            ix->hnsw.efSearch = int(val);
            return;
        }
        if (DC (IndexIVF)) {
            if (IndexHNSW *cq =
                dynamic_cast<IndexHNSW *>(ix->quantizer)) {
                cq->hnsw.efSearch = int(val);
                return;
            }
        }
    }

    FAISS_THROW_FMT ("ParameterSpace::set_index_parameter:"
                     "could not set parameter %s",
                     name.c_str());
}

void ParameterSpace::display () const
{
    printf ("ParameterSpace, %ld parameters, %ld combinations:\n",
            parameter_ranges.size (), n_combinations ());
    for (int i = 0; i < parameter_ranges.size(); i++) {
        const ParameterRange & pr = parameter_ranges[i];
        printf ("   %s: ", pr.name.c_str ());
        char sep = '[';
        for (int j = 0; j < pr.values.size(); j++) {
            printf ("%c %g", sep, pr.values [j]);
            sep = ',';
        }
        printf ("]\n");
    }
}



void ParameterSpace::update_bounds (size_t cno, const OperatingPoint & op,
                                    double *upper_bound_perf,
                                    double *lower_bound_t) const
{
    if (combination_ge (cno, op.cno)) {
        if (op.t > *lower_bound_t) *lower_bound_t = op.t;
    }
    if (combination_ge (op.cno, cno)) {
        if (op.perf < *upper_bound_perf) *upper_bound_perf = op.perf;
    }
}



void ParameterSpace::explore (Index *index,
                              size_t nq, const float *xq,
                              const AutoTuneCriterion & crit,
                              OperatingPoints * ops) const
{
    FAISS_THROW_IF_NOT_MSG (nq == crit.nq,
                      "criterion does not have the same nb of queries");

    size_t n_comb = n_combinations ();

    if (n_experiments == 0) {

        for (size_t cno = 0; cno < n_comb; cno++) {
            set_index_parameters (index, cno);
            std::vector<Index::idx_t> I(nq * crit.nnn);
            std::vector<float> D(nq * crit.nnn);

            double t0 = getmillisecs ();
            index->search (nq, xq, crit.nnn, D.data(), I.data());
            double t_search = (getmillisecs() - t0) / 1e3;

            double perf = crit.evaluate (D.data(), I.data());

            bool keep = ops->add (perf, t_search, combination_name (cno), cno);

            if (verbose)
                printf("  %ld/%ld: %s perf=%.3f t=%.3f s %s\n", cno, n_comb,
                       combination_name (cno).c_str(), perf, t_search,
                       keep ? "*" : "");
        }
        return;
    }

    int n_exp = n_experiments;

    if (n_exp > n_comb) n_exp = n_comb;
    FAISS_THROW_IF_NOT (n_comb == 1 || n_exp > 2);
    std::vector<int> perm (n_comb);
    // make sure the slowest and fastest experiment are run
    perm[0] = 0;
    if (n_comb > 1) {
        perm[1] = n_comb - 1;
        rand_perm (&perm[2], n_comb - 2, 1234);
        for (int i = 2; i < perm.size(); i++) perm[i] ++;
    }

    for (size_t xp = 0; xp < n_exp; xp++) {
        size_t cno = perm[xp];

        if (verbose)
            printf("  %ld/%d: cno=%ld %s ", xp, n_exp, cno,
                   combination_name (cno).c_str());

        {
            double lower_bound_t = 0.0;
            double upper_bound_perf = 1.0;
            for (int i = 0; i < ops->all_pts.size(); i++) {
                update_bounds (cno, ops->all_pts[i],
                               &upper_bound_perf, &lower_bound_t);
            }
            double best_t = ops->t_for_perf (upper_bound_perf);
            if (verbose)
                printf ("bounds [perf<=%.3f t>=%.3f] %s",
                        upper_bound_perf, lower_bound_t,
                        best_t <= lower_bound_t ? "skip\n" : "");
            if (best_t <= lower_bound_t) continue;
        }

        set_index_parameters (index, cno);
        std::vector<Index::idx_t> I(nq * crit.nnn);
        std::vector<float> D(nq * crit.nnn);

        double t0 = getmillisecs ();

        int nrun = 0;
        double t_search;

        do {

            if (thread_over_batches) {
#pragma omp parallel for
                for (size_t q0 = 0; q0 < nq; q0 += batchsize) {
                    size_t q1 = q0 + batchsize;
                    if (q1 > nq) q1 = nq;
                    index->search (q1 - q0, xq + q0 * index->d,
                                   crit.nnn,
                                   D.data() + q0 * crit.nnn,
                                   I.data() + q0 * crit.nnn);
                }
            } else {
                for (size_t q0 = 0; q0 < nq; q0 += batchsize) {
                    size_t q1 = q0 + batchsize;
                    if (q1 > nq) q1 = nq;
                    index->search (q1 - q0, xq + q0 * index->d,
                                   crit.nnn,
                                   D.data() + q0 * crit.nnn,
                                   I.data() + q0 * crit.nnn);
                }
            }
            nrun ++;
            t_search = (getmillisecs() - t0) / 1e3;

        } while (t_search < min_test_duration);

        t_search /= nrun;

        double perf = crit.evaluate (D.data(), I.data());

        bool keep = ops->add (perf, t_search, combination_name (cno), cno);

        if (verbose)
            printf(" perf %.3f t %.3f (%d runs) %s\n",
                   perf, t_search, nrun,
                   keep ? "*" : "");
    }
}

/***************************************************************
 * index_factory
 ***************************************************************/

namespace {

struct VTChain {
    std::vector<VectorTransform *> chain;
    ~VTChain () {
        for (int i = 0; i < chain.size(); i++) {
            delete chain[i];
        }
    }
};


/// what kind of training does this coarse quantizer require?
char get_trains_alone(const Index *coarse_quantizer) {
    return
        dynamic_cast<const MultiIndexQuantizer*>(coarse_quantizer) ? 1 :
        dynamic_cast<const IndexHNSWFlat*>(coarse_quantizer) ? 2 :
        0;
}


}

Index *index_factory (int d, const char *description_in, MetricType metric)
{
    VTChain vts;
    Index *coarse_quantizer = nullptr;
    Index *index = nullptr;
    bool add_idmap = false;
    bool make_IndexRefineFlat = false;

    ScopeDeleter1<Index> del_coarse_quantizer, del_index;

    char description[strlen(description_in) + 1];
    char *ptr;
    memcpy (description, description_in, strlen(description_in) + 1);

    int ncentroids = -1;

    for (char *tok = strtok_r (description, " ,", &ptr);
         tok;
         tok = strtok_r (nullptr, " ,", &ptr)) {
        int d_out, opq_M, nbit, M, M2, pq_m, ncent;
        std::string stok(tok);

        // to avoid mem leaks with exceptions:
        // do all tests before any instanciation

        VectorTransform *vt_1 = nullptr;
        Index *coarse_quantizer_1 = nullptr;
        Index *index_1 = nullptr;

        // VectorTransforms
        if (sscanf (tok, "PCA%d", &d_out) == 1) {
            vt_1 = new PCAMatrix (d, d_out);
            d = d_out;
        } else if (sscanf (tok, "PCAR%d", &d_out) == 1) {
            vt_1 = new PCAMatrix (d, d_out, 0, true);
            d = d_out;
        } else if (sscanf (tok, "RR%d", &d_out) == 1) {
            vt_1 = new RandomRotationMatrix (d, d_out);
            d = d_out;
        } else if (sscanf (tok, "PCAW%d", &d_out) == 1) {
            vt_1 = new PCAMatrix (d, d_out, -0.5, false);
            d = d_out;
        } else if (sscanf (tok, "PCAWR%d", &d_out) == 1) {
            vt_1 = new PCAMatrix (d, d_out, -0.5, true);
            d = d_out;
        } else if (sscanf (tok, "OPQ%d_%d", &opq_M, &d_out) == 2) {
            vt_1 = new OPQMatrix (d, opq_M, d_out);
            d = d_out;
        } else if (sscanf (tok, "OPQ%d", &opq_M) == 1) {
            vt_1 = new OPQMatrix (d, opq_M);
        } else if (stok == "L2norm") {
            vt_1 = new NormalizationTransform (d, 2.0);

        // coarse quantizers
        } else if (!coarse_quantizer &&
                   sscanf (tok, "IVF%d_HNSW%d", &ncentroids, &M) == 2) {
            FAISS_THROW_IF_NOT (metric == METRIC_L2);
            coarse_quantizer_1 = new IndexHNSWFlat (d, M);

        } else if (!coarse_quantizer &&
                   sscanf (tok, "IVF%d", &ncentroids) == 1) {
            if (metric == METRIC_L2) {
                coarse_quantizer_1 = new IndexFlatL2 (d);
            } else { // if (metric == METRIC_IP)
                coarse_quantizer_1 = new IndexFlatIP (d);
            }
        } else if (!coarse_quantizer && sscanf (tok, "IMI2x%d", &nbit) == 1) {
            FAISS_THROW_IF_NOT_MSG (metric == METRIC_L2,
                             "MultiIndex not implemented for inner prod search");
            coarse_quantizer_1 = new MultiIndexQuantizer (d, 2, nbit);
            ncentroids = 1 << (2 * nbit);
        } else if (stok == "IDMap") {
            add_idmap = true;

            // IVFs
        } else if (!index && (stok == "Flat" || stok == "FlatDedup")) {
            if (coarse_quantizer) {
                // if there was an IVF in front, then it is an IVFFlat
                IndexIVF *index_ivf = stok == "Flat" ?
                    new IndexIVFFlat (
                          coarse_quantizer, d, ncentroids, metric) :
                    new IndexIVFFlatDedup (
                          coarse_quantizer, d, ncentroids, metric);
                index_ivf->quantizer_trains_alone =
                    get_trains_alone (coarse_quantizer);
                index_ivf->cp.spherical = metric == METRIC_INNER_PRODUCT;
                del_coarse_quantizer.release ();
                index_ivf->own_fields = true;
                index_1 = index_ivf;
            } else {
                FAISS_THROW_IF_NOT_MSG (stok != "FlatDedup",
                                        "dedup supported only for IVFFlat");
                index_1 = new IndexFlat (d, metric);
            }
        } else if (!index && (stok == "SQ8" || stok == "SQ4" ||
                              stok == "SQfp16")) {
            ScalarQuantizer::QuantizerType qt =
                stok == "SQ8" ? ScalarQuantizer::QT_8bit :
                stok == "SQ4" ? ScalarQuantizer::QT_4bit :
                stok == "SQfp16" ? ScalarQuantizer::QT_fp16 :
                ScalarQuantizer::QT_4bit;
            if (coarse_quantizer) {
                IndexIVFScalarQuantizer *index_ivf =
                    new IndexIVFScalarQuantizer (
                      coarse_quantizer, d, ncentroids, qt, metric);
                index_ivf->quantizer_trains_alone =
                    get_trains_alone (coarse_quantizer);
                del_coarse_quantizer.release ();
                index_ivf->own_fields = true;
                index_1 = index_ivf;
            } else {
                index_1 = new IndexScalarQuantizer (d, qt, metric);
            }
        } else if (!index && sscanf (tok, "PQ%d+%d", &M, &M2) == 2) {
            FAISS_THROW_IF_NOT_MSG(coarse_quantizer,
                             "PQ with + works only with an IVF");
            FAISS_THROW_IF_NOT_MSG(metric == METRIC_L2,
                             "IVFPQR not implemented for inner product search");
            IndexIVFPQR *index_ivf = new IndexIVFPQR (
                  coarse_quantizer, d, ncentroids, M, 8, M2, 8);
            index_ivf->quantizer_trains_alone =
                    get_trains_alone (coarse_quantizer);
            del_coarse_quantizer.release ();
            index_ivf->own_fields = true;
            index_1 = index_ivf;
        } else if (!index && (sscanf (tok, "PQ%d", &M) == 1 ||
                              sscanf (tok, "PQ%dnp", &M) == 1)) {
            bool do_polysemous_training = stok.find("np") == std::string::npos;
            if (coarse_quantizer) {
                IndexIVFPQ *index_ivf = new IndexIVFPQ (
                    coarse_quantizer, d, ncentroids, M, 8);
                index_ivf->quantizer_trains_alone =
                    get_trains_alone (coarse_quantizer);
                index_ivf->metric_type = metric;
                index_ivf->cp.spherical = metric == METRIC_INNER_PRODUCT;
                del_coarse_quantizer.release ();
                index_ivf->own_fields = true;
                index_ivf->do_polysemous_training = do_polysemous_training;
                index_1 = index_ivf;
            } else {
                IndexPQ *index_pq = new IndexPQ (d, M, 8, metric);
                index_pq->do_polysemous_training = do_polysemous_training;
                index_1 = index_pq;
            }
        } else if (!index &&
                   sscanf (tok, "HNSW%d_%d+PQ%d", &M, &ncent, &pq_m) == 3) {
            Index * quant = new IndexFlatL2 (d);
            IndexHNSW2Level * hidx2l = new IndexHNSW2Level (quant, ncent, pq_m, M);
            Index2Layer * idx2l = dynamic_cast<Index2Layer*>(hidx2l->storage);
            idx2l->q1.own_fields = true;
            index_1 = hidx2l;
        } else if (!index &&
                   sscanf (tok, "HNSW%d_2x%d+PQ%d", &M, &nbit, &pq_m) == 3) {
            Index * quant = new MultiIndexQuantizer (d, 2, nbit);
            IndexHNSW2Level * hidx2l =
                new IndexHNSW2Level (quant, 1 << (2 * nbit), pq_m, M);
            Index2Layer * idx2l = dynamic_cast<Index2Layer*>(hidx2l->storage);
            idx2l->q1.own_fields = true;
            idx2l->q1.quantizer_trains_alone = 1;
            index_1 = hidx2l;
        } else if (!index &&
                   sscanf (tok, "HNSW%d_PQ%d", &M, &pq_m) == 2) {
            index_1 = new IndexHNSWPQ (d, pq_m, M);
        } else if (!index &&
                   sscanf (tok, "HNSW%d", &M) == 1) {
            index_1 = new IndexHNSWFlat (d, M);
        } else if (!index &&
                   sscanf (tok, "HNSW%d_SQ%d", &M, &pq_m) == 2 &&
                   pq_m == 8) {
            index_1 = new IndexHNSWSQ (d, ScalarQuantizer::QT_8bit, M);
        } else if (stok == "RFlat") {
            make_IndexRefineFlat = true;
        } else {
            FAISS_THROW_FMT( "could not parse token \"%s\" in %s\n",
                             tok, description_in);
        }

        if (index_1 && add_idmap) {
            IndexIDMap *idmap = new IndexIDMap(index_1);
            del_index.set (idmap);
            idmap->own_fields = true;
            index_1 = idmap;
            add_idmap = false;
        }

        if (vt_1)  {
            vts.chain.push_back (vt_1);
        }

        if (coarse_quantizer_1) {
            coarse_quantizer = coarse_quantizer_1;
            del_coarse_quantizer.set (coarse_quantizer);
        }

        if (index_1) {
            index = index_1;
            del_index.set (index);
        }
    }

    FAISS_THROW_IF_NOT_FMT(index, "descrption %s did not generate an index",
                    description_in);

    // nothing can go wrong now
    del_index.release ();
    del_coarse_quantizer.release ();

    if (add_idmap) {
        fprintf(stderr, "index_factory: WARNING: "
                "IDMap option not used\n");
    }

    if (vts.chain.size() > 0) {
        IndexPreTransform *index_pt = new IndexPreTransform (index);
        index_pt->own_fields = true;
        // add from back
        while (vts.chain.size() > 0) {
            index_pt->prepend_transform (vts.chain.back ());
            vts.chain.pop_back ();
        }
        index = index_pt;
    }

    if (make_IndexRefineFlat) {
        IndexRefineFlat *index_rf = new IndexRefineFlat (index);
        index_rf->own_fields = true;
        index = index_rf;
    }

    return index;
}

IndexBinary *index_binary_factory(int d, const char *description)
{
    IndexBinary *index = nullptr;

    int ncentroids = -1;
    int M;

    if (sscanf(description, "BIVF%d_HNSW%d", &ncentroids, &M) == 2) {
        IndexBinaryIVF *index_ivf = new IndexBinaryIVF(
            new IndexBinaryHNSW(d, M), d, ncentroids
        );
        index_ivf->own_fields = true;
        index = index_ivf;

    } else if (sscanf(description, "BIVF%d", &ncentroids) == 1) {
        IndexBinaryIVF *index_ivf = new IndexBinaryIVF(
            new IndexBinaryFlat(d), d, ncentroids
        );
        index_ivf->own_fields = true;
        index = index_ivf;

    } else if (sscanf(description, "BHNSW%d", &M) == 1) {
        IndexBinaryHNSW *index_hnsw = new IndexBinaryHNSW(d, M);
        index = index_hnsw;

    } else if (std::string(description) == "BFlat") {
        index = new IndexBinaryFlat(d);

    } else {
        FAISS_THROW_IF_NOT_FMT(index, "descrption %s did not generate an index",
                               description);
    }

    return index;
}

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
