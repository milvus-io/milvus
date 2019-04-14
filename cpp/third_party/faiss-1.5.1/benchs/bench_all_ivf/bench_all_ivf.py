# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#!/usr/bin/env python2

import os
import sys
import time
import numpy as np
import faiss
import argparse
import datasets
from datasets import sanitize

######################################################
# Command-line parsing
######################################################


parser = argparse.ArgumentParser()

def aa(*args, **kwargs):
    group.add_argument(*args, **kwargs)


group = parser.add_argument_group('dataset options')

aa('--db', default='deep1M', help='dataset')
aa('--compute_gt', default=False, action='store_true',
    help='compute and store the groundtruth')

group = parser.add_argument_group('index consturction')

aa('--indexkey', default='HNSW32', help='index_factory type')
aa('--efConstruction', default=200, type=int,
   help='HNSW construction factor')
aa('--M0', default=-1, type=int, help='size of base level')
aa('--maxtrain', default=256 * 256, type=int,
   help='maximum number of training points (0 to set automatically)')
aa('--indexfile', default='', help='file to read or write index from')
aa('--add_bs', default=-1, type=int,
   help='add elements index by batches of this size')
aa('--no_precomputed_tables', action='store_true', default=False,
   help='disable precomputed tables (uses less memory)')
aa('--clustering_niter', default=-1, type=int,
   help='number of clustering iterations (-1 = leave default)')
aa('--train_on_gpu', default=False, action='store_true',
   help='do training on GPU')
aa('--get_centroids_from', default='',
   help='get the centroids from this index (to speed up training)')

group = parser.add_argument_group('searching')

aa('--k', default=100, type=int, help='nb of nearest neighbors')
aa('--searchthreads', default=-1, type=int,
   help='nb of threads to use at search time')
aa('--searchparams', nargs='+', default=['autotune'],
   help="search parameters to use (can be autotune or a list of params)")
aa('--n_autotune', default=500, type=int,
   help="max nb of autotune experiments")
aa('--autotune_max', default=[], nargs='*',
   help='set max value for autotune variables format "var:val" (exclusive)')
aa('--autotune_range', default=[], nargs='*',
   help='set complete autotune range, format "var:val1,val2,..."')
aa('--min_test_duration', default=0, type=float,
   help='run test at least for so long to avoid jitter')

args = parser.parse_args()

print "args:", args

os.system('echo -n "nb processors "; '
          'cat /proc/cpuinfo | grep ^processor | wc -l; '
          'cat /proc/cpuinfo | grep ^"model name" | tail -1')

######################################################
# Load dataset
######################################################

xt, xb, xq, gt = datasets.load_data(
    dataset=args.db, compute_gt=args.compute_gt)


print "dataset sizes: train %s base %s query %s GT %s" % (
    xt.shape, xb.shape, xq.shape, gt.shape)

nq, d = xq.shape
nb, d = xb.shape


######################################################
# Make index
######################################################

if args.indexfile and os.path.exists(args.indexfile):

    print "reading", args.indexfile
    index = faiss.read_index(args.indexfile)

    if isinstance(index, faiss.IndexPreTransform):
        index_ivf = faiss.downcast_index(index.index)
    else:
        index_ivf = index
        assert isinstance(index_ivf, faiss.IndexIVF)
        vec_transform = lambda x: x
    assert isinstance(index_ivf, faiss.IndexIVF)

else:

    print "build index, key=", args.indexkey

    index = faiss.index_factory(d, args.indexkey)

    if isinstance(index, faiss.IndexPreTransform):
        index_ivf = faiss.downcast_index(index.index)
        vec_transform = index.chain.at(0).apply_py
    else:
        index_ivf = index
        vec_transform = lambda x:x
    assert isinstance(index_ivf, faiss.IndexIVF)
    index_ivf.verbose = True
    index_ivf.quantizer.verbose = True
    index_ivf.cp.verbose = True

    maxtrain = args.maxtrain
    if maxtrain == 0:
        if 'IMI' in args.indexkey:
            maxtrain = int(256 * 2 ** (np.log2(index_ivf.nlist) / 2))
        else:
            maxtrain = 50 * index_ivf.nlist
        print "setting maxtrain to %d" % maxtrain
        args.maxtrain = maxtrain

    xt2 = sanitize(xt[:args.maxtrain])
    assert np.all(np.isfinite(xt2))

    print "train, size", xt2.shape

    if args.get_centroids_from == '':

        if args.clustering_niter >= 0:
            print ("setting nb of clustering iterations to %d" %
                   args.clustering_niter)
            index_ivf.cp.niter = args.clustering_niter

        if args.train_on_gpu:
            print "add a training index on GPU"
            train_index = faiss.index_cpu_to_all_gpus(faiss.IndexFlatL2(d))
            index_ivf.clustering_index = train_index

    else:
        print "Getting centroids from", args.get_centroids_from
        src_index = faiss.read_index(args.get_centroids_from)
        src_quant = faiss.downcast_index(src_index.quantizer)
        centroids = faiss.vector_to_array(src_quant.xb)
        centroids = centroids.reshape(-1, d)
        print "  centroid table shape", centroids.shape

        if isinstance(index, faiss.IndexPreTransform):
            print "  training vector transform"
            assert index.chain.size() == 1
            vt = index.chain.at(0)
            vt.train(xt2)
            print "  transform centroids"
            centroids = vt.apply_py(centroids)

        print "  add centroids to quantizer"
        index_ivf.quantizer.add(centroids)
        del src_index

    t0 = time.time()
    index.train(xt2)
    print "  train in %.3f s" % (time.time() - t0)

    print "adding"
    t0 = time.time()
    if args.add_bs == -1:
        index.add(sanitize(xb))
    else:
        for i0 in range(0, nb, args.add_bs):
            i1 = min(nb, i0 + args.add_bs)
            print "  adding %d:%d / %d" % (i0, i1, nb)
            index.add(sanitize(xb[i0:i1]))

    print "  add in %.3f s" % (time.time() - t0)
    if args.indexfile:
        print "storing", args.indexfile
        faiss.write_index(index, args.indexfile)

if args.no_precomputed_tables:
    if isinstance(index_ivf, faiss.IndexIVFPQ):
        print "disabling precomputed table"
        index_ivf.use_precomputed_table = -1
        index_ivf.precomputed_table.clear()

if args.indexfile:
    print "index size on disk: ", os.stat(args.indexfile).st_size

print "current RSS:", faiss.get_mem_usage_kb() * 1024

precomputed_table_size = 0
if hasattr(index_ivf, 'precomputed_table'):
    precomputed_table_size = index_ivf.precomputed_table.size() * 4

print "precomputed tables size:", precomputed_table_size


#############################################################
# Index is ready
#############################################################

xq = sanitize(xq)

if args.searchthreads != -1:
    print "Setting nb of threads to", args.searchthreads
    faiss.omp_set_num_threads(args.searchthreads)


ps = faiss.ParameterSpace()
ps.initialize(index)


parametersets = args.searchparams

header = '%-40s     R@1   R@10  R@100  time(ms/q)   nb distances #runs' % "parameters"


def eval_setting(index, xq, gt, min_time):
    nq = xq.shape[0]
    ivf_stats = faiss.cvar.indexIVF_stats
    ivf_stats.reset()
    nrun = 0
    t0 = time.time()
    while True:
        D, I = index.search(xq, 100)
        nrun += 1
        t1 = time.time()
        if t1 - t0 > min_time:
            break
    ms_per_query = ((t1 - t0) * 1000.0 / nq / nrun)
    for rank in 1, 10, 100:
        n_ok = (I[:, :rank] == gt[:, :1]).sum()
        print "%.4f" % (n_ok / float(nq)),
    print "   %8.3f  " % ms_per_query,
    print "%12d   " % (ivf_stats.ndis / nrun),
    print nrun


if parametersets == ['autotune']:

    ps.n_experiments = args.n_autotune
    ps.min_test_duration = args.min_test_duration

    for kv in args.autotune_max:
        k, vmax = kv.split(':')
        vmax = float(vmax)
        print "limiting %s to %g" % (k, vmax)
        pr = ps.add_range(k)
        values = faiss.vector_to_array(pr.values)
        values = np.array([v for v in values if v < vmax])
        faiss.copy_array_to_vector(values, pr.values)

    for kv in args.autotune_range:
        k, vals = kv.split(':')
        vals = np.fromstring(vals, sep=',')
        print "setting %s to %s" % (k, vals)
        pr = ps.add_range(k)
        faiss.copy_array_to_vector(vals, pr.values)

    # setup the Criterion object: optimize for 1-R@1
    crit = faiss.OneRecallAtRCriterion(nq, 1)

    # by default, the criterion will request only 1 NN
    crit.nnn = 100
    crit.set_groundtruth(None, gt.astype('int64'))

    # then we let Faiss find the optimal parameters by itself
    print "exploring operating points"
    ps.display()

    t0 = time.time()
    op = ps.explore(index, xq, crit)
    print "Done in %.3f s, available OPs:" % (time.time() - t0)

    op.display()

    print header
    opv = op.optimal_pts
    for i in range(opv.size()):
        opt = opv.at(i)

        ps.set_index_parameters(index, opt.key)

        print "%-40s " % opt.key,
        sys.stdout.flush()

        eval_setting(index, xq, gt, args.min_test_duration)

else:
    print header
    for param in parametersets:
        print "%-40s " % param,
        sys.stdout.flush()
        ps.set_index_parameters(index, param)

        eval_setting(index, xq, gt, args.min_test_duration)
