# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import os
import faiss
import argparse
from multiprocessing.dummy import Pool as ThreadPool

if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--inputs', nargs='*', required=True,
                        help='input indexes to merge')
    parser.add_argument('--l0', type=int, default=0)
    parser.add_argument('--l1', type=int, default=-1)

    parser.add_argument('--nt', default=-1,
                        help='nb threads')

    parser.add_argument('--output', required=True,
                        help='output index filename')
    parser.add_argument('--outputIL',
                        help='output invfile filename')

    args = parser.parse_args()

    if args.nt != -1:
        print('set nb of threads to', args.nt)


    ils = faiss.InvertedListsPtrVector()
    ils_dont_dealloc = []

    pool = ThreadPool(20)

    def load_index(fname):
        print("loading", fname)
        try:
            index = faiss.read_index(fname, faiss.IO_FLAG_MMAP | faiss.IO_FLAG_READ_ONLY)
        except RuntimeError as e:
            print('could not load %s: %s' % (fname, e))
            return fname, None

        print("  %d entries" % index.ntotal)
        return fname, index

    index0 = None

    for _, index in pool.imap(load_index, args.inputs):
        if index is None:
            continue
        index_ivf = faiss.extract_index_ivf(index)
        il = faiss.downcast_InvertedLists(index_ivf.invlists)
        index_ivf.invlists = None
        il.this.own()
        ils_dont_dealloc.append(il)
        if (args.l0, args.l1) != (0, -1):
            print('restricting to lists %d:%d' % (args.l0, args.l1))
            # il = faiss.SliceInvertedLists(il, args.l0, args.l1)

            il.crop_invlists(args.l0, args.l1)
            ils_dont_dealloc.append(il)
        ils.push_back(il)

        if index0 is None:
            index0 = index

    print("loaded %d invlists" % ils.size())

    if not args.outputIL:
        args.outputIL = args.output + '_invlists'

    il0 = ils.at(0)

    il = faiss.OnDiskInvertedLists(
        il0.nlist, il0.code_size,
        args.outputIL)

    print("perform merge")

    ntotal = il.merge_from(ils.data(), ils.size(), True)

    print("swap into index0")

    index0_ivf = faiss.extract_index_ivf(index0)
    index0_ivf.nlist = il0.nlist
    index0_ivf.ntotal = index0.ntotal = ntotal
    index0_ivf.invlists = il
    index0_ivf.own_invlists = False

    print("write", args.output)

    faiss.write_index(index0, args.output)
