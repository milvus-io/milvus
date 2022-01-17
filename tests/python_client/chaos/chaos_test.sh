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

ns="chaos-testing"

# switch namespace
kubectl config set-context --current --namespace=${ns}

# set parameters
pod=${1:-"querynode"}
chaos_type=${2:-"pod_kill"} #pod_kill or pod_failure
chaos_task=${3:-"chaos-test"} # chaos-test or data-consist-test 
node_num=${4:-1} # cluster_1_node or cluster_n_nodes

cur_time=$(date +%H-%M-%S)
release="test"-${pod}-${chaos_type/_/-}-${cur_time} # replace pod_kill to pod-kill

# install milvus cluster for chaos testing
pushd ./scripts
echo "uninstall milvus if exist"
bash uninstall_milvus.sh ${release} ${ns}|| true

declare -A pod_map=(["querynode"]="queryNode" ["indexnode"]="indexNode" ["datanode"]="dataNode" ["proxy"]="proxy")
echo "install milvus"
if [ ${pod} != "standalone" ];
then
    echo "insatll cluster"
    helm install --wait --timeout 360s ${release} milvus/milvus --set image.all.tag=${image_tag:-"master-latest"} --set ${pod_map[${pod}]}.replicas=$node_num -f ../cluster-values.yaml -n=${ns}
fi

if [ ${pod} == "standalone" ];
then
    echo "install standalone"
    helm install --wait --timeout 360s ${release} milvus/milvus --set image.all.tag=${image_tag:-"master-latest"} -f ../standalone-values.yaml -n=${ns}
fi

# wait all pod ready
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance=${release} -n ${ns} --timeout=360s
kubectl wait --for=condition=Ready pod -l release=${release} -n ${ns} --timeout=360s

popd

# replace chaos object as defined
if [ "$platform" == "Mac" ];
then
    sed -i "" "s/TESTS_CONFIG_LOCATION =.*/TESTS_CONFIG_LOCATION = \'chaos_objects\/${chaos_type}\/'/g" constants.py
    sed -i "" "s/ALL_CHAOS_YAMLS =.*/ALL_CHAOS_YAMLS = \'chaos_${pod}_${chaos_type}.yaml\'/g" constants.py
    sed -i "" "s/RELEASE_NAME =.*/RELEASE_NAME = \'${release}\'/g" constants.py
else
    sed -i "s/TESTS_CONFIG_LOCATION =.*/TESTS_CONFIG_LOCATION = \'chaos_objects\/${chaos_type}\/'/g" constants.py
    sed -i "s/ALL_CHAOS_YAMLS =.*/ALL_CHAOS_YAMLS = \'chaos_${pod}_${chaos_type}.yaml\'/g" constants.py
    sed -i "s/RELEASE_NAME =.*/RELEASE_NAME = \'${release}\'/g" constants.py
fi

# run chaos testing
echo "start running testcase ${pod}"
if [[ $release =~ "milvus" ]]
then
    host=$(kubectl get svc/${release} -o jsonpath="{.spec.clusterIP}")
else
    host=$(kubectl get svc/${release}-milvus -o jsonpath="{.spec.clusterIP}")
fi
pytest -s -v ../testcases/test_e2e.py --host "$host" --log-cli-level=INFO --capture=no
python scripts/hello_milvus.py --host "$host"

# chaos test
if [ "$chaos_task" == "chaos-test" ];
then
    pytest -s -v test_chaos.py --host "$host" --log-cli-level=INFO --capture=no || echo "chaos test fail"
fi
# data consist test
if [ "$chaos_task" == "data-consist-test" ];
then
    pytest -s -v test_chaos_data_consist.py --host "$host" --log-cli-level=INFO --capture=no || echo "chaos test fail"
fi
sleep 30
echo "start running e2e test"
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance=${release} -n ${ns} --timeout=360s
kubectl wait --for=condition=Ready pod -l release=${release} -n ${ns} --timeout=360s

pytest -s -v ../testcases/test_e2e.py --host "$host" --log-cli-level=INFO --capture=no || echo "e2e test fail"
python scripts/hello_milvus.py --host "$host" || echo "e2e test fail"

# save logs
cur_time=$(date +%Y-%m-%d-%H-%M-%S)
bash ../../scripts/export_log_k8s.sh ${ns} ${release} k8s_log/${pod}-${chaos_type}-${chaos_task}-${cur_time}
