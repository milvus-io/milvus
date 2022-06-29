#!/bin/bash


namespace='default'
log_path='./milvus-log'

etcd="false"
minio="false"
pulsar="false"
kafka="false"

#-n namespace: The namespace that Milvus is installed in.
#-i milvus_instance: The name of milvus instance.
#-p log_path: Log storage path.
#-e export etcd logs
#-m export minio logs
#-u export pulsar logs
#-k export kafka logs
while getopts "n:i:p:emuk" opt_name
do
    case $opt_name in
    	n) namespace=$OPTARG;;
    	i) instance_name=$OPTARG;;
    	p) log_path=$OPTARG;;
    	e) etcd="true";;
    	m) minio="true";;
    	u) pulsar="true";;
    	k) kafka="true";;
    	*) echo "Unkonwen parameters";;
    esac
done

if [ ! $instance_name ];
then
	echo "Missing argument instance_name, please add it. For example:'./export-milvus-log.sh -i milvus-instance-name'"
	exit 1
fi

if [ ! -d $log_path ];
then
	mkdir -p $log_path
fi

echo "The log files will be stored $(readlink -f $log_path)"


function export_log(){
	# export pod logs
	for pod in $1;
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
}

# export milvus component log
pods=$(kubectl get pod -n $namespace -l app.kubernetes.io/instance=$instance_name,app.kubernetes.io/name=milvus --output=jsonpath={.items..metadata.name})
if [ ${#pods} == 0 ];
then
	echo "There is no Milvus instance $instance_name in the namespace $namespace"
else
	export_log "${pods[*]}"
fi

# export etcd component log
if $etcd;
then
	etcd_pods=$(kubectl get pod -n $namespace -l app.kubernetes.io/instance=$instance_name,app.kubernetes.io/name=etcd --output=jsonpath={.items..metadata.name})
	if [ ${#etcd_pods} == 0 ];
	then
		echo "There is no etcd component for Milvus instance $instance_name in the namespace $namespace"
	else
		export_log "${etcd_pods[*]}"
	fi
fi

# export minio component log
if $minio;
then
	minio_pods=$(kubectl get pod -n $namespace -l release=$instance_name,app=minio --output=jsonpath={.items..metadata.name})
	if [ ${#minio_pods} == 0 ];
	then
		echo "There is no minio component for Milvus instance $instance_name in the namespace $namespace"
	else
		export_log "${minio_pods[*]}"
	fi
fi

# export pulsar component log
if $pulsar;
then
	pulsar_pods=$(kubectl get pod -n $namespace -l cluster=$instance_name-pulsar,app=pulsar --output=jsonpath={.items..metadata.name})
	if [ ${#pulsar_pods} == 0 ];
	then
		echo "There is no pulsar component for Milvus instance $instance_name in the namespace $namespace"
	else
		export_log "${pulsar_pods[*]}"
	fi
fi

# export kafka component log
if $kafka;
then
	kafka_pods=$(kubectl get pod -n $namespace -l app.kubernetes.io/instance=$instance_name,app.kubernetes.io/component=kafka --output=jsonpath={.items..metadata.name})
	if [ ${#kafka_pods} == 0 ];
	then
		echo "There is no kafka component for Milvus instance $instance_name in the namespace $namespace"
	else
		export_log "${kafka_pods[*]}"
	fi
fi

tar zcf $log_path.tar.gz $log_path

echo "The compressed logs are stored in $(readlink -f $log_path.tar.gz)"
