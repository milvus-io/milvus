#!/bin/bash


namespace='default'
log_path='./milvus-log'


#-n namespace: The namespace that Milvus is installed in.
#-i milvus_instance: The name of milvus instance.
#-p log_path: Log storage path.

while getopts "n:i:p:" opt_name
do
    case $opt_name in
        n) namespace=$OPTARG
           ;;
        i) instance_name=$OPTARG 
           ;;
	p) log_path=$OPTARG
           ;;
	*) echo "Unkonwen parameters"
	   ;;
    esac
done

if [ ! $instance_name ];
then
	echo "Missing argument instance_name, please add it. For example:'./export-milvus-log.sh -i milvus-instance-name'"
	exit 1
fi

pods=$(kubectl get pod -n $namespace -l app.kubernetes.io/instance=$instance_name,app.kubernetes.io/name=milvus --output=jsonpath={.items..metadata.name})

if [ ${#pods} == 0 ];
then
	echo "There is no Milvus instance $instance_name in the namespace $namespace"
	exit 1
fi

if [ ! -d $log_path ];
then
	mkdir -p $log_path
fi

echo "The log files will be stored $(readlink -f $log_path)"

for pod in $pods;
do
	# Check if the pod has been restarted
	if [ $(kubectl get pod $pod -n $namespace --output=jsonpath={.status.containerStatuses[0].restartCount}) == 0 ];
	then
		echo "Export log of $pod"
		kubectl logs $pod -n $namespace > $log_path/$pod.log
	else
		echo "Export log of $pod"
		kubectl logs $pod -n $namespace -p > $log_path/$pod-pre.log
		kubectl logs $pod -n $namespace > $log_path/$pod.log
	fi
done

tar zcf $log_path.tar.gz $log_path

echo "The compressed logs are stored in $(readlink -f $log_path.tar.gz)"
