set -e
set -x


echo "check os env"
platform='unknown'
unamestr=$(uname)
if [[ "$unamestr" == 'Linux' ]]; then
   platform='Linux'
elif [[ "$unamestr" == 'Darwin' ]]; then
   platform='Mac'
fi
echo "platform: $platform"

# define chaos testing object
release=${1:-"milvus-chaos"}
ns=${2:-"chaos-testing"}

# switch namespace
kubectl config set-context --current --namespace=${ns}
pod="standalone"
chaos_type="pod_kill"
chaos_task="data-consist-test" # chaos-test or data-consist-test 
release="milvus-chaos"
ns="chaos-testing"

# install milvus cluster for chaos testing
pushd ./scripts
echo "uninstall milvus if exist"
bash uninstall_milvus.sh ${release} ${ns}|| true
echo "install milvus"
if [ ${pod} != "standalone" ];
then
    echo "insatll cluster"
    bash install_milvus.sh ${release} ${ns}
fi

if [ ${pod} == "standalone" ];
then
    echo "install standalone"
    helm install --wait --timeout 360s ${release} milvus/milvus --set service.type=NodePort -f ../standalone-values.yaml -n=${ns}
fi
# if chaos_type is pod_failure, update replicas
if [ "$chaos_type" == "pod_failure" ];
then
    declare -A pod_map=(["querynode"]="queryNode" ["indexnode"]="indexNode" ["datanode"]="dataNode" ["proxy"]="proxy")
    helm upgrade ${release} milvus/milvus --set ${pod_map[${pod}]}.replicas=2 --reuse-values
fi

popd

# replace chaos object as defined
if [ "$platform" == "Mac" ];
then
    sed -i "" "s/TESTS_CONFIG_LOCATION =.*/TESTS_CONFIG_LOCATION = \'chaos_objects\/${chaos_type}\/'/g" constants.py
    sed -i "" "s/ALL_CHAOS_YAMLS =.*/ALL_CHAOS_YAMLS = \'chaos_${pod}_${chaos_type}.yaml\'/g" constants.py
else
    sed -i "s/TESTS_CONFIG_LOCATION =.*/TESTS_CONFIG_LOCATION = \'chaos_objects\/${chaos_type}\/'/g" constants.py
    sed -i "s/ALL_CHAOS_YAMLS =.*/ALL_CHAOS_YAMLS = \'chaos_${pod}_${chaos_type}.yaml\'/g" constants.py
fi

# run chaos testing
echo "start running testcase ${pod}"
host=$(kubectl get svc/milvus-chaos -o jsonpath="{.spec.clusterIP}")
python scripts/hello_milvus.py --host "$host"
# chaos test
if [ "$chaos_task" == "chaos-test" ];
then
    pytest -s -v test_chaos.py --host "$host" || echo "chaos test fail"
fi
# data consist test
if [ "$chaos_task" == "data-consist-test" ];
then
    pytest -s -v test_chaos_data_consist.py --host "$host" || echo "chaos test fail"
fi
sleep 30s
echo "start running e2e test"
python scripts/hello_milvus.py --host "$host" || echo "e2e test fail"

# save logs
bash ../../scripts/export_log_k8s.sh ${ns} ${release} k8s_log/${pod}
