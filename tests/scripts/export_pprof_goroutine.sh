#!/bin/bash

# Exit immediately for non zero status
set -e

ns_name=$1
instance_name=$2
log_dir=${3:-"k8s_logs"}


# export goroutine of all milvus pods
array=($(kubectl get pod -n ${ns_name} -l "app.kubernetes.io/instance=${instance_name}, app.kubernetes.io/name=milvus"| awk 'NR == 1 {next} {print $1}'))


echo ${array[@]}
if [ ! -d $log_dir/goroutine ];
then
    mkdir -p $log_dir/goroutine
fi
echo "export goroutine start"
for pod in ${array[*]}
do
echo "export goroutine for pod $pod "
ip=($(kubectl get pod ${pod} -n ${ns_name} -o jsonpath='{.status.podIP}'))
echo $ip
curl "http://${ip}:9091/debug/pprof/goroutine?debug=10" -o ./$log_dir/goroutine/$pod.log || echo "export goroutine for pod $pod failed"
done
echo "export goroutine done"
