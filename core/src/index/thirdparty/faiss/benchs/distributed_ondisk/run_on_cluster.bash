#! /bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

set -e

todo=$1
# other options can be transmitted
shift

# the training data of the Deep1B dataset
deep1bdir=/datasets01_101/simsearch/041218/deep1b
traindata=$deep1bdir/learn.fvecs

# this is for small tests
nvec=1000000
k=4000

# for the real run
# nvec=50000000
# k=1000000

# working directory for the real run
workdir=/checkpoint/matthijs/ondisk_distributed
mkdir -p $workdir/{vslices,hslices}

if [ -z "$todo" ]; then
    echo "nothing to do"
    exit 1
elif [ $todo == test_kmeans_0 ]; then
    # non distributed baseline
    python distributed_kmeans.py \
           --indata $traindata --i1 $nvec \
           --k $k

elif [ $todo == test_kmeans_1 ]; then
    # using all the machine's GPUs
    python distributed_kmeans.py \
           --indata $traindata --i1 $nvec \
           --k $k --gpu -1

elif [ $todo == test_kmeans_2 ]; then
    # distrbuted run, with one local server per GPU
    ngpu=$( echo /dev/nvidia? | wc -w )
    baseport=12012

    # kill background porcesses on output of this script
    trap 'kill -HUP 0' 0

    hostports=''

    for((gpu=0;gpu<ngpu;gpu++)); do
        # range of vectors to assign to each sever
        i0=$((nvec * gpu / ngpu))
        i1=$((nvec * (gpu + 1) / ngpu))
        port=$(( baseport + gpu ))

        echo "start server $gpu for range $i0:$i1"

        python distributed_kmeans.py \
               --indata $traindata \
               --i0 $i0 --i1 $i1 \
               --server --gpu $gpu \
               --port $port --ipv4 &

        hostports="$hostports localhost:$port"
    done

    # lame way of making sure all servers are running
    sleep 5s

    python distributed_kmeans.py \
           --client --servers "$hostports" \
           --k $k --ipv4

elif [ $todo == slurm_distributed_kmeans ]; then

    nserv=5

    srun -n$nserv \
         --time=48:00:00 \
         --cpus-per-task=40 --gres=gpu:4 --mem=100G \
         --partition=priority --comment='priority is the only one that works'  \
         -l bash $( realpath $0 ) slurm_within_kmeans_server

elif [ $todo == slurm_within_kmeans_server ]; then

   nserv=$SLURM_NPROCS
   [ ! -z "$nserv" ] || (echo "should be run by slurm"; exit 1)
   rank=$SLURM_PROCID

   baseport=12012

   i0=$((nvec * rank / nserv))
   i1=$((nvec * (rank + 1) / nserv))
   port=$(( baseport + rank ))

   echo "host $(hostname) start server $rank for range $i0:$i1 port $port"

   if [ $rank != 0 ]; then

       python -u distributed_kmeans.py \
              --indata $traindata \
              --i0 $i0 --i1 $i1 \
              --server --gpu -1 \
              --port $port --ipv4
   else
       # master process

       # kill background processes on output of this script
       trap 'kill -HUP 0' 0

       python -u distributed_kmeans.py \
              --indata $traindata \
              --i0 $i0 --i1 $i1 \
              --server --gpu -1 \
              --port $port --ipv4 &

       # Slurm has a somewhat convoluted way of specifying the nodes
       # assigned to each task. This is to parse the SLURM_TASKS_PER_NODE variable
       function parse_tasks_per_node () {
           local blocks=$1
           for block in ${blocks//,/ }; do
               if [ ${block/x/} != $block ]; then
                   tpn="${block%(*}"
                   repeat=${block#*x}
                   repeat=${repeat%?}
                   for((i=0;i<repeat;i++)); do
                       echo $tpn
                   done
               else
                   echo $block
               fi
            done
       }

       hostports=""
       port=$baseport
       echo VARS $SLURM_TASKS_PER_NODE $SLURM_JOB_NODELIST
       tasks_per_node=( $( parse_tasks_per_node $SLURM_TASKS_PER_NODE ) )
       nodes=( $( scontrol show hostnames $SLURM_JOB_NODELIST ) )
       n=${#nodes[*]}
       for((i=0;i<n;i++)); do
           hostname=${nodes[i]}
           for((j=0;j<tasks_per_node[i];j++)); do
               hostports="$hostports $hostname:$port"
               ((port++))
           done
       done

       echo HOSTPORTS $hostports

       sleep 20s

       # run client
       python distributed_kmeans.py \
           --client --servers "$hostports" \
           --k $k --ipv4 "$@"

       echo "Done, kill the job"
       scancel $SLURM_JOBID

   fi

elif [ $todo == deep1b_clustering ]; then
    # also set nvec=500M and k=10M in the top of the file
    nserv=20

    srun -n$nserv \
         --time=48:00:00 \
         --cpus-per-task=40 --gres=gpu:4 --mem=100G \
         --partition=priority --comment='priority is the only one that works'  \
         -l bash $( realpath $0 ) slurm_within_kmeans_server \
         --out $workdir/1M_centroids.npy

elif [ $todo == make_index_vslices ]; then

    # vslice: slice per database shards

    nvec=1000000000
    nslice=200

    for((i=0;i<nslice;i++)); do
        i0=$((nvec * i / nslice))
        i1=$((nvec * (i + 1) / nslice))

        # make the script to be run by sbatch
        cat > $workdir/vslices/slice$i.bash <<EOF
#!/bin/bash

srun python -u make_index_vslice.py \
                 --inputindex $workdir/trained.faissindex \
                 --input $deep1bdir/base.fvecs \
                 --nt 40 \
                 --i0 $i0 --i1 $i1 \
                 -o $workdir/vslices/slice$i.faissindex

EOF
        # specify resources for script and run it
        sbatch -n1 \
             --time=48:00:00 \
             --cpus-per-task=40 --gres=gpu:0 --mem=200G \
             --output=$workdir/vslices/slice$i.log \
             --job-name=vslice$i.c \
             $workdir/vslices/slice$i.bash
        echo "logs in $workdir/vslices/slice$i.log"

    done

elif [ $todo == make_index_hslices ]; then

    # hslice: slice per inverted lists

    nlist=1000000
    nslice=50

    for((i=0;i<nslice;i++)); do
        i0=$((nlist * i / nslice))
        i1=$((nlist * (i + 1) / nslice))

        # make the script to be run by sbatch
        cat > $workdir/hslices/slice$i.bash <<EOF
#!/bin/bash

srun python -u merge_to_ondisk.py \
                 --input $workdir/vslices/slice{0..199}.faissindex \
                 --nt 20 \
                 --l0 $i0 --l1 $i1 \
                 --output $workdir/hslices/slice$i.faissindex \
                 --outputIL $workdir/hslices/slice$i.invlists


EOF
        # specify resources for script and run it
        sbatch -n1 \
             --time=48:00:00 \
             --cpus-per-task=20 --gres=gpu:0 --mem=200G \
             --output=$workdir/hslices/slice$i.log \
             --job-name=hslice$i.a \
             --constraint=pascal \
             $workdir/hslices/slice$i.bash
        echo "logs in $workdir/hslices/slice$i.log"

    done

elif [ $todo == run_search_servers ]; then

    nserv=3

    srun -n$nserv \
         --time=48:00:00 \
         --cpus-per-task=64 --gres=gpu:0 --mem=100G \
         --constraint=pascal \
         --partition=priority --comment='priority is the only one that works'  \
         -l python -u search_server.py --port 12012


else
    echo "unknown todo $todo"
    exit 1
fi
