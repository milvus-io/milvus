array=($(kubectl get pod|grep chaos-milvus|awk '{print $1}')) # change chaos-milvus to your own pod name prefix
echo ${array[@]}
log_dir="k8s_logs"
mkdir $log_dir
echo "export logs start"
for pod in ${array[*]}
do
echo "export logs for pod $pod "
kubectl logs $pod > ./$log_dir/$pod.log
done
echo "export logs done"
