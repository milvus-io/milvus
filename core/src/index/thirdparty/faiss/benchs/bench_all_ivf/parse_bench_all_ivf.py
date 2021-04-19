# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

#! /usr/bin/env python2

import os
import numpy as np
from matplotlib import pyplot

import re

from argparse import Namespace


# the directory used in run_on_cluster.bash
basedir = '/mnt/vol/gfsai-east/ai-group/users/matthijs/bench_all_ivf/'
logdir = basedir + 'logs/'


# which plot to output
db = 'bigann1B'
code_size = 8



def unitsize(indexkey):
    """ size of one vector in the index """
    mo = re.match('.*,PQ(\\d+)', indexkey)
    if mo:
        return int(mo.group(1))
    if indexkey.endswith('SQ8'):
        bits_per_d = 8
    elif indexkey.endswith('SQ4'):
        bits_per_d = 4
    elif indexkey.endswith('SQfp16'):
        bits_per_d = 16
    else:
        assert False
    mo = re.match('PCAR(\\d+),.*', indexkey)
    if mo:
        return bits_per_d * int(mo.group(1)) / 8
    mo = re.match('OPQ\\d+_(\\d+),.*', indexkey)
    if mo:
        return bits_per_d * int(mo.group(1)) / 8
    mo = re.match('RR(\\d+),.*', indexkey)
    if mo:
        return bits_per_d * int(mo.group(1)) / 8
    assert False


def dbsize_from_name(dbname):
    sufs = {
        '1B': 10**9,
        '100M': 10**8,
        '10M': 10**7,
        '1M': 10**6,
    }
    for s in sufs:
        if dbname.endswith(s):
            return sufs[s]
    else:
        assert False


def keep_latest_stdout(fnames):
    fnames = [fname for fname in fnames if fname.endswith('.stdout')]
    fnames.sort()
    n = len(fnames)
    fnames2 = []
    for i, fname in enumerate(fnames):
        if i + 1 < n and fnames[i + 1][:-8] == fname[:-8]:
            continue
        fnames2.append(fname)
    return fnames2


def parse_result_file(fname):
    # print fname
    st = 0
    res = []
    keys = []
    stats = {}
    stats['run_version'] = fname[-8]
    for l in open(fname):
        if st == 0:
            if l.startswith('CHRONOS_JOB_INSTANCE_ID'):
                stats['CHRONOS_JOB_INSTANCE_ID'] = l.split()[-1]
            if l.startswith('index size on disk:'):
                stats['index_size'] = int(l.split()[-1])
            if l.startswith('current RSS:'):
                stats['RSS'] = int(l.split()[-1])
            if l.startswith('precomputed tables size:'):
                stats['tables_size'] = int(l.split()[-1])
            if l.startswith('Setting nb of threads to'):
                stats['n_threads'] = int(l.split()[-1])
            if l.startswith('  add in'):
                stats['add_time'] = float(l.split()[-2])
            if l.startswith('args:'):
                args = eval(l[l.find(' '):])
                indexkey = args.indexkey
            elif 'R@1   R@10  R@100' in l:
                st = 1
            elif 'index size on disk:' in l:
                index_size = int(l.split()[-1])
        elif st == 1:
            st = 2
        elif st == 2:
            fi = l.split()
            keys.append(fi[0])
            res.append([float(x) for x in fi[1:]])
    return indexkey, np.array(res), keys, stats

# run parsing
allres = {}
allstats = {}
nts = []
missing = []
versions = {}

fnames = keep_latest_stdout(os.listdir(logdir))
# print fnames
# filenames are in the form <key>.x.stdout
# where x is a version number (from a to z)
# keep only latest version of each name

for fname in fnames:
    if not ('db' + db in fname and fname.endswith('.stdout')):
        continue
    indexkey, res, _, stats = parse_result_file(logdir + fname)
    if res.size == 0:
        missing.append(fname)
        errorline = open(
            logdir + fname.replace('.stdout', '.stderr')).readlines()
        if len(errorline) > 0:
            errorline = errorline[-1]
        else:
            errorline = 'NO STDERR'
        print fname, stats['CHRONOS_JOB_INSTANCE_ID'], errorline

    else:
        if indexkey in allres:
            if allstats[indexkey]['run_version'] > stats['run_version']:
                # don't use this run
                continue
        n_threads = stats.get('n_threads', 1)
        nts.append(n_threads)
        allres[indexkey] = res
        allstats[indexkey] = stats

assert len(set(nts)) == 1
n_threads = nts[0]


def plot_tradeoffs(allres, code_size, recall_rank):
    dbsize = dbsize_from_name(db)
    recall_idx = int(np.log10(recall_rank))

    bigtab = []
    names = []

    for k,v in sorted(allres.items()):
        if v.ndim != 2: continue
        us = unitsize(k)
        if us != code_size: continue
        perf = v[:, recall_idx]
        times = v[:, 3]
        bigtab.append(
            np.vstack((
                np.ones(times.size, dtype=int) * len(names),
                perf, times
            ))
        )
        names.append(k)

    bigtab = np.hstack(bigtab)

    perm = np.argsort(bigtab[1, :])
    bigtab = bigtab[:, perm]

    times = np.minimum.accumulate(bigtab[2, ::-1])[::-1]
    selection = np.where(bigtab[2, :] == times)

    selected_methods = [names[i] for i in
                        np.unique(bigtab[0, selection].astype(int))]
    not_selected = list(set(names) - set(selected_methods))

    print "methods without an optimal OP: ", not_selected

    nq = 10000
    pyplot.title('database ' + db + ' code_size=%d' % code_size)

    # grayed out lines

    for k in not_selected:
        v = allres[k]
        if v.ndim != 2: continue
        us = unitsize(k)
        if us != code_size: continue

        linestyle = (':' if 'PQ' in k else
                     '-.' if 'SQ4' in k else
                     '--' if 'SQ8' in k else '-')

        pyplot.semilogy(v[:, recall_idx], v[:, 3], label=None,
                        linestyle=linestyle,
                        marker='o' if 'HNSW' in k else '+',
                        color='#cccccc', linewidth=0.2)

    # important methods
    for k in selected_methods:
        v = allres[k]
        if v.ndim != 2: continue
        us = unitsize(k)
        if us != code_size: continue

        stats = allstats[k]
        tot_size = stats['index_size'] + stats['tables_size']
        id_size = 8 # 64 bit

        addt = ''
        if 'add_time' in stats:
            add_time = stats['add_time']
            if add_time > 7200:
                add_min = add_time / 60
                addt = ', %dh%02d' % (add_min / 60, add_min % 60)
            else:
                add_sec = int(add_time)
                addt = ', %dm%02d' % (add_sec / 60, add_sec % 60)


        label = k + ' (size+%.1f%%%s)' % (
            tot_size / float((code_size + id_size) * dbsize) * 100 - 100,
            addt)

        linestyle = (':' if 'PQ' in k else
                     '-.' if 'SQ4' in k else
                     '--' if 'SQ8' in k else '-')

        pyplot.semilogy(v[:, recall_idx], v[:, 3], label=label,
                        linestyle=linestyle,
                        marker='o' if 'HNSW' in k else '+')

    if len(not_selected) == 0:
        om = ''
    else:
        om = '\nomitted:'
        nc = len(om)
        for m in not_selected:
            if nc > 80:
                om += '\n'
                nc = 0
            om += ' ' + m
            nc += len(m) + 1

    pyplot.xlabel('1-recall at %d %s' % (recall_rank, om) )
    pyplot.ylabel('search time per query (ms, %d threads)' % n_threads)
    pyplot.legend()
    pyplot.grid()
    pyplot.savefig('figs/tradeoffs_%s_cs%d_r%d.png' % (
        db, code_size, recall_rank))
    return selected_methods, not_selected


pyplot.gcf().set_size_inches(15, 10)

plot_tradeoffs(allres, code_size=code_size, recall_rank=1)
