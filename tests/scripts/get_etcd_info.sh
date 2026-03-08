#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.
instance_name=$1
# Define the etcdctl command with endpoints option
etcdctl_cmd="etcdctl endpoint status -w table --endpoints"

# Get the ip of all the etcd pods with the specified labels
etcd_pods=$(kubectl get pods -l app.kubernetes.io/name=etcd,app.kubernetes.io/instance=${instance_name} -o jsonpath='{.items[*].status.podIP}')

# Check if the etcd pods are running
if [ -z "$etcd_pods" ]; then
    echo "No etcd pods found"
    exit 1
fi

# Loop through the list of etcd pods and get their status
endpoints=""
for pod in $etcd_pods
do
    endpoints+="$pod:2379,"
done

$etcdctl_cmd ${endpoints::-1}