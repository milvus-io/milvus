# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#!/usr/bin/env python2

import sys
import numpy as np
import faiss


#################################################################
# Small I/O functions
#################################################################


def ivecs_read(fname):
    a = np.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()


def fvecs_read(fname):
    return ivecs_read(fname).view('float32')


#################################################################
#  Main program
#################################################################

stage = int(sys.argv[1])

tmpdir = '/tmp/'

if stage == 0:
    # train the index
    xt = fvecs_read("sift1M/sift_learn.fvecs")
    index = faiss.index_factory(xt.shape[1], "IVF4096,Flat")
    print("training index")
    index.train(xt)
    print("write " + tmpdir + "trained.index")
    faiss.write_index(index, tmpdir + "trained.index")


if 1 <= stage <= 4:
    # add 1/4 of the database to 4 independent indexes
    bno = stage - 1
    xb = fvecs_read("sift1M/sift_base.fvecs")
    i0, i1 = int(bno * xb.shape[0] / 4), int((bno + 1) * xb.shape[0] / 4)
    index = faiss.read_index(tmpdir + "trained.index")
    print("adding vectors %d:%d" % (i0, i1))
    index.add_with_ids(xb[i0:i1], np.arange(i0, i1))
    print("write " + tmpdir + "block_%d.index" % bno)
    faiss.write_index(index, tmpdir + "block_%d.index" % bno)


if stage == 5:
    # merge the images into an on-disk index
    # first load the inverted lists
    ivfs = []
    for bno in range(4):
        # the IO_FLAG_MMAP is to avoid actually loading the data thus
        # the total size of the inverted lists can exceed the
        # available RAM
        print("read " + tmpdir + "block_%d.index" % bno)
        index = faiss.read_index(tmpdir + "block_%d.index" % bno,
                                 faiss.IO_FLAG_MMAP)
        ivfs.append(index.invlists)

        # avoid that the invlists get deallocated with the index
        index.own_invlists = False

    # construct the output index
    index = faiss.read_index(tmpdir + "trained.index")

    # prepare the output inverted lists. They will be written
    # to merged_index.ivfdata
    invlists = faiss.OnDiskInvertedLists(
        index.nlist, index.code_size,
        tmpdir + "merged_index.ivfdata")

    # merge all the inverted lists
    ivf_vector = faiss.InvertedListsPtrVector()
    for ivf in ivfs:
        ivf_vector.push_back(ivf)

    print("merge %d inverted lists " % ivf_vector.size())
    ntotal = invlists.merge_from(ivf_vector.data(), ivf_vector.size())

    # now replace the inverted lists in the output index
    index.ntotal = ntotal
    index.replace_invlists(invlists)

    print("write " + tmpdir + "populated.index")
    faiss.write_index(index, tmpdir + "populated.index")


if stage == 6:
    # perform a search from disk
    print("read " + tmpdir + "populated.index")
    index = faiss.read_index(tmpdir + "populated.index")
    index.nprobe = 16

    # load query vectors and ground-truth
    xq = fvecs_read("sift1M/sift_query.fvecs")
    gt = ivecs_read("sift1M/sift_groundtruth.ivecs")

    D, I = index.search(xq, 5)

    recall_at_1 = (I[:, :1] == gt[:, :1]).sum() / float(xq.shape[0])
    print("recall@1: %.3f" % recall_at_1)
