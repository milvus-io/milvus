# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

# @nolint

# This script launches the experiments on a cluster
# It assumes two shell functions are defined:
#
#    run_on_1machine: runs a command on one (full) machine on a cluster
#
#    run_on_8gpu: runs a command on one machine with 8 GPUs
#
# the two functions are called as:
#
#    run_on_1machine <name> <command>
#
# the stdout of the command should be stored in $logdir/<name>.stdout

function run_on_1machine () {
    # To be implemented
}

function run_on_8gpu () {
    # To be implemented
}


# prepare output directories
# set to some directory where all indexes, can be written.
basedir=XXXXX

logdir=$basedir/logs
indexdir=$basedir/indexes

mkdir -p $lars $logdir $indexdir


############################### 1M experiments

for db in sift1M deep1M bigann1M; do

    for coarse in IMI2x9 IMI2x10 IVF1024_HNSW32 IVF4096_HNSW32 IVF16384_HNSW32
    do

        for indexkey in \
            OPQ8_64,$coarse,PQ8 \
            PCAR16,$coarse,SQ4 \
            OPQ16_64,$coarse,PQ16 \
            PCAR32,$coarse,SQ4 \
            PCAR16,$coarse,SQ8 \
            OPQ32_128,$coarse,PQ32 \
            PCAR64,$coarse,SQ4 \
            PCAR32,$coarse,SQ8 \
            PCAR16,$coarse,SQfp16 \
            PCAR64,$coarse,SQ8 \
            PCAR32,$coarse,SQfp16 \
            PCAR128,$coarse,SQ4
        do
            key=autotune.db$db.${indexkey//,/_}
            run_on_1machine $key \
                 python -u bench_all_ivf.py \
                    --db $db \
                    --indexkey $indexkey \
                    --maxtrain 0  \
                    --indexfile $indexdir/$key.faissindex

        done
    done
done



############################### 10M experiments


for db in deep10M bigann10M; do

    for coarse in \
        IMI2x10 IMI2x11 IMI2x12 IMI2x13 IVF4096_HNSW32 \
        IVF16384_HNSW32 IVF65536_HNSW32 IVF262144_HNSW32
    do

        for indexkey in \
            OPQ8_64,$coarse,PQ8 \
            PCAR16,$coarse,SQ4 \
            OPQ16_64,$coarse,PQ16 \
            PCAR32,$coarse,SQ4 \
            PCAR16,$coarse,SQ8 \
            OPQ32_128,$coarse,PQ32 \
            PCAR64,$coarse,SQ4 \
            PCAR32,$coarse,SQ8 \
            PCAR16,$coarse,SQfp16 \
            PCAR64,$coarse,SQ8 \
            PCAR32,$coarse,SQfp16 \
            PCAR128,$coarse,SQ4 \
            OPQ64_128,$coarse,PQ64
        do
            key=autotune.db$db.${indexkey//,/_}
            run_on_1machine $key \
                 python -u bench_all_ivf.py \
                    --db $db \
                    --indexkey $indexkey \
                    --maxtrain 0  \
                    --indexfile $indexdir/$key.faissindex \
                    --searchthreads 16 \
                    --min_test_duration 3 \

        done
    done
done


############################### 100M experiments

for db in deep100M bigann100M; do

    for coarse in IMI2x11 IMI2x12 IVF65536_HNSW32 IVF262144_HNSW32
    do

        for indexkey in \
            OPQ8_64,$coarse,PQ8 \
            OPQ16_64,$coarse,PQ16 \
            PCAR32,$coarse,SQ4 \
            OPQ32_128,$coarse,PQ32 \
            PCAR64,$coarse,SQ4 \
            PCAR32,$coarse,SQ8 \
            PCAR64,$coarse,SQ8 \
            PCAR32,$coarse,SQfp16 \
            PCAR128,$coarse,SQ4 \
            OPQ64_128,$coarse,PQ64
        do
            key=autotune.db$db.${indexkey//,/_}
            run_on_1machine $key \
                 python -u bench_all_ivf.py \
                    --db $db \
                    --indexkey $indexkey \
                    --maxtrain 0  \
                    --indexfile $indexdir/$key.faissindex \
                    --searchthreads 16 \
                    --min_test_duration 3 \
                    --add_bs 1000000

        done
    done
done


############################### 1B experiments

for db in deep1B bigann1B; do

    for coarse in  IMI2x12 IMI2x13 IVF262144_HNSW32
    do

        for indexkey in \
            OPQ8_64,$coarse,PQ8 \
            OPQ16_64,$coarse,PQ16 \
            PCAR32,$coarse,SQ4 \
            OPQ32_128,$coarse,PQ32 \
            PCAR64,$coarse,SQ4 \
            PCAR32,$coarse,SQ8 \
            PCAR64,$coarse,SQ8 \
            PCAR32,$coarse,SQfp16 \
            PCAR128,$coarse,SQ4 \
            PQ64_128,$coarse,PQ64 \
            RR128,$coarse,SQ4
        do
            key=autotune.db$db.${indexkey//,/_}
            run_on_1machine $key \
                 python -u bench_all_ivf.py \
                    --db $db \
                    --indexkey $indexkey \
                    --maxtrain 0  \
                    --indexfile $indexdir/$key.faissindex \
                    --searchthreads 16 \
                    --min_test_duration 3 \
                    --add_bs 1000000

        done
    done

done

############################################
# precompute centroids on GPU for large vocabularies


for db in deep1M bigann1M; do

    for ncent in 1048576 4194304; do

        key=clustering.db$db.IVF$ncent
        run_on_8gpu $key \
            python -u bench_all_ivf.py \
                --db $db \
                --indexkey IVF$ncent,SQ8 \
                --maxtrain 100000000  \
                --indexfile $indexdir/$key.faissindex \
                --searchthreads 16 \
                --min_test_duration 3 \
                --add_bs 1000000 \
                --train_on_gpu

    done
done


#################################
# Run actual experiment

for db in deep1B bigann1B; do

    for ncent in 1048576 4194304; do
        coarse=IVF${ncent}_HNSW32
        centroidsname=clustering.db${db/1B/1M}.IVF${ncent}.faissindex

        for indexkey in \
            OPQ8_64,$coarse,PQ8 \
            OPQ16_64,$coarse,PQ16 \
            PCAR32,$coarse,SQ4 \
            OPQ32_128,$coarse,PQ32 \
            PCAR64,$coarse,SQ4 \
            PCAR32,$coarse,SQ8 \
            PCAR64,$coarse,SQ8 \
            PCAR32,$coarse,SQfp16 \
            OPQ64_128,$coarse,PQ64 \
            RR128,$coarse,SQ4 \
            OPQ64_128,$coarse,PQ64 \
            RR128,$coarse,SQ4
        do
            key=autotune.db$db.${indexkey//,/_}

            run_on_1machine $key.c $key \
                 python -u bench_all_ivf.py \
                    --db $db \
                    --indexkey $indexkey \
                    --maxtrain 256000  \
                    --indexfile $indexdir/$key.faissindex \
                    --get_centroids_from $indexdir/$centroidsname \
                    --searchthreads 16 \
                    --min_test_duration 3 \
                    --add_bs 1000000

        done
    done

done
