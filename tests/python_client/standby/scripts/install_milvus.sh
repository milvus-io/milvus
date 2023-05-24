
release=${1:-"milvs-chaos"}
milvus_mode=${2:-"cluster"}
ns=${3:-"chaos-testing"}
bash uninstall_milvus.sh ${release} ${ns}|| true

helm repo add milvus https://milvus-io.github.io/milvus-helm/
helm repo update
if [[ ${milvus_mode} == "cluster" ]];
then
    helm install --wait --timeout 360s ${release} milvus/milvus -f ../cluster-values.yaml --set metrics.serviceMonitor.enabled=true -n=${ns}
fi

if [[ ${milvus_mode} == "standalone" ]];
then
    helm install --wait --timeout 360s ${release} milvus/milvus -f ../standalone-values.yaml --set metrics.serviceMonitor.enabled=true -n=${ns}
fi
