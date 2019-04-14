# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD+Patents license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

import numpy as np
import time
import os
import sys
import faiss
import re

from multiprocessing.dummy import Pool as ThreadPool


####################################################################
# Parse command line
####################################################################


def usage():
    print >>sys.stderr, """

Usage: bench_gpu_1bn.py dataset indextype [options]

dataset: set of vectors to operate on.
   Supported: SIFT1M, SIFT2M, ..., SIFT1000M or Deep1B

indextype: any index type supported by index_factory that runs on GPU.

    General options

-ngpu ngpu         nb of GPUs to use (default = all)
-tempmem N         use N bytes of temporary GPU memory
-nocache           do not read or write intermediate files
-float16           use 16-bit floats on the GPU side

    Add options

-abs N             split adds in blocks of no more than N vectors
-max_add N         copy sharded dataset to CPU each max_add additions
                   (to avoid memory overflows with geometric reallocations)
-altadd            Alternative add function, where the index is not stored
                   on GPU during add. Slightly faster for big datasets on
                   slow GPUs

    Search options

-R R:              nb of replicas of the same dataset (the dataset
                   will be copied across ngpu/R, default R=1)
-noptables         do not use precomputed tables in IVFPQ.
-qbs N             split queries in blocks of no more than N vectors
-nnn N             search N neighbors for each query
-nprobe 4,16,64    try this number of probes
-knngraph          instead of the standard setup for the dataset,
                   compute a k-nn graph with nnn neighbors per element
-oI xx%d.npy       output the search result indices to this numpy file,
                   %d will be replaced with the nprobe
-oD xx%d.npy       output the search result distances to this file

"""
    sys.exit(1)


# default values

dbname = None
index_key = None

ngpu = faiss.get_num_gpus()

replicas = 1  # nb of replicas of sharded dataset
add_batch_size = 32768
query_batch_size = 16384
nprobes = [1 << l for l in range(9)]
knngraph = False
use_precomputed_tables = True
tempmem = -1  # if -1, use system default
max_add = -1
use_float16 = False
use_cache = True
nnn = 10
altadd = False
I_fname = None
D_fname = None

args = sys.argv[1:]

while args:
    a = args.pop(0)
    if a == '-h': usage()
    elif a == '-ngpu':      ngpu = int(args.pop(0))
    elif a == '-R':         replicas = int(args.pop(0))
    elif a == '-noptables': use_precomputed_tables = False
    elif a == '-abs':       add_batch_size = int(args.pop(0))
    elif a == '-qbs':       query_batch_size = int(args.pop(0))
    elif a == '-nnn':       nnn = int(args.pop(0))
    elif a == '-tempmem':   tempmem = int(args.pop(0))
    elif a == '-nocache':   use_cache = False
    elif a == '-knngraph':  knngraph = True
    elif a == '-altadd':    altadd = True
    elif a == '-float16':   use_float16 = True
    elif a == '-nprobe':    nprobes = [int(x) for x in args.pop(0).split(',')]
    elif a == '-max_add':   max_add = int(args.pop(0))
    elif not dbname:        dbname = a
    elif not index_key:     index_key = a
    else:
        print >> sys.stderr, "argument %s unknown" % a
        sys.exit(1)

cacheroot = '/tmp/bench_gpu_1bn'

if not os.path.isdir(cacheroot):
    print "%s does not exist, creating it" % cacheroot
    os.mkdir(cacheroot)

#################################################################
# Small Utility Functions
#################################################################


def ivecs_read(fname):
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def fvecs_read(fname):
    return ivecs_read(fname).view('float32')

# we mem-map the biggest files to avoid having them in memory all at
# once

def mmap_fvecs(fname):
    x = np.memmap(fname, dtype='int32', mode='r')
    d = x[0]
    return x.view('float32').reshape(-1, d + 1)[:, 1:]

def mmap_bvecs(fname):
    x = np.memmap(fname, dtype='uint8', mode='r')
    d = x[:4].view('int32')[0]
    return x.reshape(-1, d + 4)[:, 4:]


def rate_limited_imap(f, l):
    """A threaded imap that does not produce elements faster than they
    are consumed"""
    pool = ThreadPool(1)
    res = None
    for i in l:
        res_next = pool.apply_async(f, (i, ))
        if res:
            yield res.get()
        res = res_next
    yield res.get()


class IdentPreproc:
    """a pre-processor is either a faiss.VectorTransform or an IndentPreproc"""

    def __init__(self, d):
        self.d_in = self.d_out = d

    def apply_py(self, x):
        return x


def sanitize(x):
    """ convert array to a c-contiguous float array """
    return np.ascontiguousarray(x.astype('float32'))


def dataset_iterator(x, preproc, bs):
    """ iterate over the lines of x in blocks of size bs"""

    nb = x.shape[0]
    block_ranges = [(i0, min(nb, i0 + bs))
                    for i0 in range(0, nb, bs)]

    def prepare_block((i0, i1)):
        xb = sanitize(x[i0:i1])
        return i0, preproc.apply_py(xb)

    return rate_limited_imap(prepare_block, block_ranges)


def eval_intersection_measure(gt_I, I):
    """ measure intersection measure (used for knngraph)"""
    inter = 0
    rank = I.shape[1]
    assert gt_I.shape[1] >= rank
    for q in range(nq_gt):
        inter += faiss.ranklist_intersection_size(
            rank, faiss.swig_ptr(gt_I[q, :]),
            rank, faiss.swig_ptr(I[q, :].astype('int64')))
    return inter / float(rank * nq_gt)


#################################################################
# Prepare dataset
#################################################################

print "Preparing dataset", dbname

if dbname.startswith('SIFT'):
    # SIFT1M to SIFT1000M
    dbsize = int(dbname[4:-1])
    xb = mmap_bvecs('bigann/bigann_base.bvecs')
    xq = mmap_bvecs('bigann/bigann_query.bvecs')
    xt = mmap_bvecs('bigann/bigann_learn.bvecs')

    # trim xb to correct size
    xb = xb[:dbsize * 1000 * 1000]

    gt_I = ivecs_read('bigann/gnd/idx_%dM.ivecs' % dbsize)

elif dbname == 'Deep1B':
    xb = mmap_fvecs('deep1b/base.fvecs')
    xq = mmap_fvecs('deep1b/deep1B_queries.fvecs')
    xt = mmap_fvecs('deep1b/learn.fvecs')
    # deep1B's train is is outrageously big
    xt = xt[:10 * 1000 * 1000]
    gt_I = ivecs_read('deep1b/deep1B_groundtruth.ivecs')

else:
    print >> sys.stderr, 'unknown dataset', dbname
    sys.exit(1)


if knngraph:
    # convert to knn-graph dataset
    xq = xb
    xt = xb
    # we compute the ground-truth on this number of queries for validation
    nq_gt = 10000
    gt_sl = 100

    # ground truth will be computed below
    gt_I = None


print "sizes: B %s Q %s T %s gt %s" % (
    xb.shape, xq.shape, xt.shape,
    gt_I.shape if gt_I is not None else None)



#################################################################
# Parse index_key and set cache files
#
# The index_key is a valid factory key that would work, but we
# decompose the training to do it faster
#################################################################


pat = re.compile('(OPQ[0-9]+(_[0-9]+)?,|PCAR[0-9]+,)?' +
                 '(IVF[0-9]+),' +
                 '(PQ[0-9]+|Flat)')

matchobject = pat.match(index_key)

assert matchobject, 'could not parse ' + index_key

mog = matchobject.groups()

preproc_str = mog[0]
ivf_str = mog[2]
pqflat_str = mog[3]

ncent = int(ivf_str[3:])

prefix = ''

if knngraph:
    gt_cachefile = '%s/BK_gt_%s.npy' % (cacheroot, dbname)
    prefix = 'BK_'
    # files must be kept distinct because the training set is not the
    # same for the knngraph

if preproc_str:
    preproc_cachefile = '%s/%spreproc_%s_%s.vectrans' % (
        cacheroot, prefix, dbname, preproc_str[:-1])
else:
    preproc_cachefile = None
    preproc_str = ''

cent_cachefile = '%s/%scent_%s_%s%s.npy' % (
    cacheroot, prefix, dbname, preproc_str, ivf_str)

index_cachefile = '%s/%s%s_%s%s,%s.index' % (
    cacheroot, prefix, dbname, preproc_str, ivf_str, pqflat_str)


if not use_cache:
    preproc_cachefile = None
    cent_cachefile = None
    index_cachefile = None

print "cachefiles:"
print preproc_cachefile
print cent_cachefile
print index_cachefile


#################################################################
# Wake up GPUs
#################################################################

print "preparing resources for %d GPUs" % ngpu

gpu_resources = []

for i in range(ngpu):
    res = faiss.StandardGpuResources()
    if tempmem >= 0:
        res.setTempMemory(tempmem)
    gpu_resources.append(res)


def make_vres_vdev(i0=0, i1=-1):
    " return vectors of device ids and resources useful for gpu_multiple"
    vres = faiss.GpuResourcesVector()
    vdev = faiss.IntVector()
    if i1 == -1:
        i1 = ngpu
    for i in range(i0, i1):
        vdev.push_back(i)
        vres.push_back(gpu_resources[i])
    return vres, vdev


#################################################################
# Prepare ground truth (for the knngraph)
#################################################################


def compute_GT():
    print "compute GT"
    t0 = time.time()

    gt_I = np.zeros((nq_gt, gt_sl), dtype='int64')
    gt_D = np.zeros((nq_gt, gt_sl), dtype='float32')
    heaps = faiss.float_maxheap_array_t()
    heaps.k = gt_sl
    heaps.nh = nq_gt
    heaps.val = faiss.swig_ptr(gt_D)
    heaps.ids = faiss.swig_ptr(gt_I)
    heaps.heapify()
    bs = 10 ** 5

    n, d = xb.shape
    xqs = sanitize(xq[:nq_gt])

    db_gt = faiss.IndexFlatL2(d)
    vres, vdev = make_vres_vdev()
    db_gt_gpu = faiss.index_cpu_to_gpu_multiple(
        vres, vdev, db_gt)

    # compute ground-truth by blocks of bs, and add to heaps
    for i0, xsl in dataset_iterator(xb, IdentPreproc(d), bs):
        db_gt_gpu.add(xsl)
        D, I = db_gt_gpu.search(xqs, gt_sl)
        I += i0
        heaps.addn_with_ids(
            gt_sl, faiss.swig_ptr(D), faiss.swig_ptr(I), gt_sl)
        db_gt_gpu.reset()
        print "\r   %d/%d, %.3f s" % (i0, n, time.time() - t0),
    print
    heaps.reorder()

    print "GT time: %.3f s" % (time.time() - t0)
    return gt_I


if knngraph:

    if gt_cachefile and os.path.exists(gt_cachefile):
        print "load GT", gt_cachefile
        gt_I = np.load(gt_cachefile)
    else:
        gt_I = compute_GT()
        if gt_cachefile:
            print "store GT", gt_cachefile
            np.save(gt_cachefile, gt_I)

#################################################################
# Prepare the vector transformation object (pure CPU)
#################################################################


def train_preprocessor():
    print "train preproc", preproc_str
    d = xt.shape[1]
    t0 = time.time()
    if preproc_str.startswith('OPQ'):
        fi = preproc_str[3:-1].split('_')
        m = int(fi[0])
        dout = int(fi[1]) if len(fi) == 2 else d
        preproc = faiss.OPQMatrix(d, m, dout)
    elif preproc_str.startswith('PCAR'):
        dout = int(preproc_str[4:-1])
        preproc = faiss.PCAMatrix(d, dout, 0, True)
    else:
        assert False
    preproc.train(sanitize(xt[:1000000]))
    print "preproc train done in %.3f s" % (time.time() - t0)
    return preproc


def get_preprocessor():
    if preproc_str:
        if not preproc_cachefile or not os.path.exists(preproc_cachefile):
            preproc = train_preprocessor()
            if preproc_cachefile:
                print "store", preproc_cachefile
                faiss.write_VectorTransform(preproc, preproc_cachefile)
        else:
            print "load", preproc_cachefile
            preproc = faiss.read_VectorTransform(preproc_cachefile)
    else:
        d = xb.shape[1]
        preproc = IdentPreproc(d)
    return preproc


#################################################################
# Prepare the coarse quantizer
#################################################################


def train_coarse_quantizer(x, k, preproc):
    d = preproc.d_out
    clus = faiss.Clustering(d, k)
    clus.verbose = True
    # clus.niter = 2
    clus.max_points_per_centroid = 10000000

    print "apply preproc on shape", x.shape, 'k=', k
    t0 = time.time()
    x = preproc.apply_py(sanitize(x))
    print "   preproc %.3f s output shape %s" % (
        time.time() - t0, x.shape)

    vres, vdev = make_vres_vdev()
    index = faiss.index_cpu_to_gpu_multiple(
        vres, vdev, faiss.IndexFlatL2(d))

    clus.train(x, index)
    centroids = faiss.vector_float_to_array(clus.centroids)

    return centroids.reshape(k, d)


def prepare_coarse_quantizer(preproc):

    if cent_cachefile and os.path.exists(cent_cachefile):
        print "load centroids", cent_cachefile
        centroids = np.load(cent_cachefile)
    else:
        nt = max(1000000, 256 * ncent)
        print "train coarse quantizer..."
        t0 = time.time()
        centroids = train_coarse_quantizer(xt[:nt], ncent, preproc)
        print "Coarse train time: %.3f s" % (time.time() - t0)
        if cent_cachefile:
            print "store centroids", cent_cachefile
            np.save(cent_cachefile, centroids)

    coarse_quantizer = faiss.IndexFlatL2(preproc.d_out)
    coarse_quantizer.add(centroids)

    return coarse_quantizer


#################################################################
# Make index and add elements to it
#################################################################


def prepare_trained_index(preproc):

    coarse_quantizer = prepare_coarse_quantizer(preproc)
    d = preproc.d_out
    if pqflat_str == 'Flat':
        print "making an IVFFlat index"
        idx_model = faiss.IndexIVFFlat(coarse_quantizer, d, ncent,
                                       faiss.METRIC_L2)
    else:
        m = int(pqflat_str[2:])
        assert m < 56 or use_float16, "PQ%d will work only with -float16" % m
        print "making an IVFPQ index, m = ", m
        idx_model = faiss.IndexIVFPQ(coarse_quantizer, d, ncent, m, 8)

    coarse_quantizer.this.disown()
    idx_model.own_fields = True

    # finish training on CPU
    t0 = time.time()
    print "Training vector codes"
    x = preproc.apply_py(sanitize(xt[:1000000]))
    idx_model.train(x)
    print "  done %.3f s" % (time.time() - t0)

    return idx_model


def compute_populated_index(preproc):
    """Add elements to a sharded index. Return the index and if available
    a sharded gpu_index that contains the same data. """

    indexall = prepare_trained_index(preproc)

    co = faiss.GpuMultipleClonerOptions()
    co.useFloat16 = use_float16
    co.useFloat16CoarseQuantizer = False
    co.usePrecomputed = use_precomputed_tables
    co.indicesOptions = faiss.INDICES_CPU
    co.verbose = True
    co.reserveVecs = max_add if max_add > 0 else xb.shape[0]
    co.shard = True
    assert co.shard_type in (0, 1, 2)
    vres, vdev = make_vres_vdev()
    gpu_index = faiss.index_cpu_to_gpu_multiple(
        vres, vdev, indexall, co)

    print "add..."
    t0 = time.time()
    nb = xb.shape[0]
    for i0, xs in dataset_iterator(xb, preproc, add_batch_size):
        i1 = i0 + xs.shape[0]
        gpu_index.add_with_ids(xs, np.arange(i0, i1))
        if max_add > 0 and gpu_index.ntotal > max_add:
            print "Flush indexes to CPU"
            for i in range(ngpu):
                index_src_gpu = faiss.downcast_index(gpu_index.at(i))
                index_src = faiss.index_gpu_to_cpu(index_src_gpu)
                print "  index %d size %d" % (i, index_src.ntotal)
                index_src.copy_subset_to(indexall, 0, 0, nb)
                index_src_gpu.reset()
                index_src_gpu.reserveMemory(max_add)
            gpu_index.sync_with_shard_indexes()

        print '\r%d/%d (%.3f s)  ' % (
            i0, nb, time.time() - t0),
        sys.stdout.flush()
    print "Add time: %.3f s" % (time.time() - t0)

    print "Aggregate indexes to CPU"
    t0 = time.time()

    if hasattr(gpu_index, 'at'):
        # it is a sharded index
        for i in range(ngpu):
            index_src = faiss.index_gpu_to_cpu(gpu_index.at(i))
            print "  index %d size %d" % (i, index_src.ntotal)
            index_src.copy_subset_to(indexall, 0, 0, nb)
    else:
        # simple index
        index_src = faiss.index_gpu_to_cpu(gpu_index)
        index_src.copy_subset_to(indexall, 0, 0, nb)

    print "  done in %.3f s" % (time.time() - t0)

    if max_add > 0:
        # it does not contain all the vectors
        gpu_index = None

    return gpu_index, indexall

def compute_populated_index_2(preproc):

    indexall = prepare_trained_index(preproc)

    # set up a 3-stage pipeline that does:
    # - stage 1: load + preproc
    # - stage 2: assign on GPU
    # - stage 3: add to index

    stage1 = dataset_iterator(xb, preproc, add_batch_size)

    vres, vdev = make_vres_vdev()
    coarse_quantizer_gpu = faiss.index_cpu_to_gpu_multiple(
        vres, vdev, indexall.quantizer)

    def quantize((i0, xs)):
        _, assign = coarse_quantizer_gpu.search(xs, 1)
        return i0, xs, assign.ravel()

    stage2 = rate_limited_imap(quantize, stage1)

    print "add..."
    t0 = time.time()
    nb = xb.shape[0]

    for i0, xs, assign in stage2:
        i1 = i0 + xs.shape[0]
        if indexall.__class__ == faiss.IndexIVFPQ:
            indexall.add_core_o(i1 - i0, faiss.swig_ptr(xs),
                                None, None, faiss.swig_ptr(assign))
        elif indexall.__class__ == faiss.IndexIVFFlat:
            indexall.add_core(i1 - i0, faiss.swig_ptr(xs), None,
                              faiss.swig_ptr(assign))
        else:
            assert False

        print '\r%d/%d (%.3f s)  ' % (
            i0, nb, time.time() - t0),
        sys.stdout.flush()
    print "Add time: %.3f s" % (time.time() - t0)

    return None, indexall



def get_populated_index(preproc):

    if not index_cachefile or not os.path.exists(index_cachefile):
        if not altadd:
            gpu_index, indexall = compute_populated_index(preproc)
        else:
            gpu_index, indexall = compute_populated_index_2(preproc)
        if index_cachefile:
            print "store", index_cachefile
            faiss.write_index(indexall, index_cachefile)
    else:
        print "load", index_cachefile
        indexall = faiss.read_index(index_cachefile)
        gpu_index = None

    co = faiss.GpuMultipleClonerOptions()
    co.useFloat16 = use_float16
    co.useFloat16CoarseQuantizer = False
    co.usePrecomputed = use_precomputed_tables
    co.indicesOptions = 0
    co.verbose = True
    co.shard = True    # the replicas will be made "manually"
    t0 = time.time()
    print "CPU index contains %d vectors, move to GPU" % indexall.ntotal
    if replicas == 1:

        if not gpu_index:
            print "copying loaded index to GPUs"
            vres, vdev = make_vres_vdev()
            index = faiss.index_cpu_to_gpu_multiple(
                vres, vdev, indexall, co)
        else:
            index = gpu_index

    else:
        del gpu_index # We override the GPU index

        print "Copy CPU index to %d sharded GPU indexes" % replicas

        index = faiss.IndexReplicas()

        for i in range(replicas):
            gpu0 = ngpu * i / replicas
            gpu1 = ngpu * (i + 1) / replicas
            vres, vdev = make_vres_vdev(gpu0, gpu1)

            print "   dispatch to GPUs %d:%d" % (gpu0, gpu1)

            index1 = faiss.index_cpu_to_gpu_multiple(
                vres, vdev, indexall, co)
            index1.this.disown()
            index.addIndex(index1)
        index.own_fields = True
    del indexall
    print "move to GPU done in %.3f s" % (time.time() - t0)
    return index



#################################################################
# Perform search
#################################################################


def eval_dataset(index, preproc):

    ps = faiss.GpuParameterSpace()
    ps.initialize(index)

    nq_gt = gt_I.shape[0]
    print "search..."
    sl = query_batch_size
    nq = xq.shape[0]
    for nprobe in nprobes:
        ps.set_index_parameter(index, 'nprobe', nprobe)
        t0 = time.time()

        if sl == 0:
            D, I = index.search(preproc.apply_py(sanitize(xq)), nnn)
        else:
            I = np.empty((nq, nnn), dtype='int32')
            D = np.empty((nq, nnn), dtype='float32')

            inter_res = ''

            for i0, xs in dataset_iterator(xq, preproc, sl):
                print '\r%d/%d (%.3f s%s)   ' % (
                    i0, nq, time.time() - t0, inter_res),
                sys.stdout.flush()

                i1 = i0 + xs.shape[0]
                Di, Ii = index.search(xs, nnn)

                I[i0:i1] = Ii
                D[i0:i1] = Di

                if knngraph and not inter_res and i1 >= nq_gt:
                    ires = eval_intersection_measure(
                        gt_I[:, :nnn], I[:nq_gt])
                    inter_res = ', %.4f' % ires

        t1 = time.time()
        if knngraph:
            ires = eval_intersection_measure(gt_I[:, :nnn], I[:nq_gt])
            print "  probe=%-3d: %.3f s rank-%d intersection results: %.4f" % (
                nprobe, t1 - t0, nnn, ires)
        else:
            print "  probe=%-3d: %.3f s" % (nprobe, t1 - t0),
            gtc = gt_I[:, :1]
            nq = xq.shape[0]
            for rank in 1, 10, 100:
                if rank > nnn: continue
                nok = (I[:, :rank] == gtc).sum()
                print "1-R@%d: %.4f" % (rank, nok / float(nq)),
            print
        if I_fname:
            I_fname_i = I_fname % I
            print "storing", I_fname_i
            np.save(I, I_fname_i)
        if D_fname:
            D_fname_i = I_fname % I
            print "storing", D_fname_i
            np.save(D, D_fname_i)


#################################################################
# Driver
#################################################################


preproc = get_preprocessor()

index = get_populated_index(preproc)

eval_dataset(index, preproc)

# make sure index is deleted before the resources
del index
