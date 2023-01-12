#!/bin/bash


namespace='default'
log_path='./milvus-log'

etcd="false"
minio="false"
pulsar="false"
kafka="false"
since_args=""
operator="false"
#-n namespace: The namespace that Milvus is installed in.
#-i milvus_instance: The name of milvus instance.
#-d log_path: Log storage path.
#-e export etcd logs
#-m export minio logs
#-p export pulsar logs
#-k export kafka logs
#-s 24h: export logs since 24h
while getopts "n:i:d:s:empko" opt_name
do
    case $opt_name in
    	n) namespace=$OPTARG;;
    	i) instance_name=$OPTARG;;
    	d) log_path=$OPTARG;;
    	e) etcd="true";;
    	m) minio="true";;
    	p) pulsar="true";;
    	k) kafka="true";;
        o) operator="true";;
        s) since=$OPTARG;;
    	*) echo "Unkonwen parameters";;
    esac
done

if [ ! $instance_name ];
then
	echo "Missing argument instance_name, please add it. For example:'./export-milvus-log.sh -i milvus-instance-name'"
	exit 1
fi

if [ ! -d $log_path/pod_log ] || [ ! -d $log_dir/pod_log_previous ] || [ ! -d $log_dir/pod_describe ];
then
    mkdir -p $log_path/pod_log
    mkdir -p $log_path/pod_log_previous
    mkdir -p $log_path/pod_describe
fi

if [ $since ];
then
  since_args="--since=$since"
fi

echo "The log files will be stored in $(readlink -f $log_path)"


function export_log(){
	# export pod logs
	for pod in $1;
	do
		# Check if the pod has been restarted
		if [ $(kubectl get pod $pod -n $namespace --output=jsonpath={.status.containerStatuses[0].restartCount}) == 0 ];
		then
			echo "Export log of $pod"
			kubectl logs $pod -n $namespace ${since_args}> $log_path/pod_log/$pod.log
		else
			echo "Export log of $pod"
			kubectl logs $pod -n $namespace -p ${since_args}> $log_path/pod_log_previous/$pod.log
			kubectl logs $pod -n $namespace ${since_args}> $log_path/pod_log/$pod.log
		fi
		echo "Export describe of $pod"
		kubectl describe pod $pod -n $namespace > $log_path/pod_describe/$pod.log
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
	if $operator
	then
		etcd_pods=$(kubectl get pod -n $namespace -l app.kubernetes.io/instance=$instance_name-etcd,app.kubernetes.io/name=etcd --output=jsonpath={.items..metadata.name})
	else
		etcd_pods=$(kubectl get pod -n $namespace -l app.kubernetes.io/instance=$instance_name,app.kubernetes.io/name=etcd --output=jsonpath={.items..metadata.name})
	fi
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
	if $operator
	then
		minio_pods=$(kubectl get pod -n $namespace -l release=${instance_name}-minio,app=minio --output=jsonpath={.items..metadata.name})
	else
		minio_pods=$(kubectl get pod -n $namespace -l release=$instance_name,app=minio --output=jsonpath={.items..metadata.name})
	fi
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
	if $operator
	then
		pulsar_pods=$(kubectl get pod -n $namespace -l cluster=${instance_name}-pulsar,app=pulsar --output=jsonpath={.items..metadata.name})
	else
		if [[ $instance_name =~ "pulsar" ]]
		then
			pulsar_pods=$(kubectl get pod -n $namespace -l cluster=$instance_name,app=pulsar --output=jsonpath={.items..metadata.name})
		else
			pulsar_pods=$(kubectl get pod -n $namespace -l cluster=${instance_name}-pulsar,app=pulsar --output=jsonpath={.items..metadata.name})
		fi
	fi
	
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
	if $operator
	then
		kafka_pods=$(kubectl get pod -n $namespace -l app.kubernetes.io/instance=${instance_name}-kafka,app.kubernetes.io/component=kafka --output=jsonpath={.items..metadata.name})
	else
		kafka_pods=$(kubectl get pod -n $namespace -l app.kubernetes.io/instance=${instance_name},app.kubernetes.io/component=kafka --output=jsonpath={.items..metadata.name})
	fi
	
	if [ ${#kafka_pods} == 0 ];
	then
		echo "There is no kafka component for Milvus instance $instance_name in the namespace $namespace"
	else
		export_log "${kafka_pods[*]}"
	fi
fi

tar zcf $log_path.tar.gz $log_path

echo "The compressed logs are stored in $(readlink -f $log_path.tar.gz)"
