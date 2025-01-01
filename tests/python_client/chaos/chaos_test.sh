#!/bin/bash

# Set PS4 prompt to display line number, function name and timestamp
export PS4='+(${BASH_SOURCE}:${LINENO}):${FUNCNAME[0]:+${FUNCNAME[0]}(): }'

set -e  # Exit immediately if a command exits with a non-zero status
set -x  # Print commands and their arguments as they are executed

# Store the initial directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Function to log important steps
declare -A colors=(
    ["INFO"]=$'\033[32m'    # Green
    ["BOLD"]=$'\033[1m'     # Bold
    ["TIME"]=$'\033[36m'    # Cyan
    ["RESET"]=$'\033[0m'    # Reset
)

log_step() {
    # Check if stdout is a terminal
    if [ -t 1 ]; then
        local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
        echo -e "${colors[INFO]}${colors[BOLD]}===> STEP [${colors[TIME]}${timestamp}${colors[INFO]}]: $1${colors[RESET]}"
    else
        # If not terminal (e.g., redirected to file), don't use colors
        local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
        echo "===> STEP [${timestamp}]: $1"
    fi
}

# Cleanup function to ensure resources are properly released
cleanup() {
    local exit_code=$?
    log_step "Performing cleanup (exit code: $exit_code)"
    
    # Make sure we're in the correct directory for cleanup
    cd "${SCRIPT_DIR}" || true
    
    # Export logs
    cur_time=$(date +%Y-%m-%d-%H-%M-%S)
    if [ -f "../../scripts/export_log_k8s.sh" ]; then
        bash ../../scripts/export_log_k8s.sh ${ns} ${release} k8s_log/${target_component}-${chaos_type}-${cur_time} || true
    else
        echo "Warning: export_log_k8s.sh not found in expected location"
    fi
    
    # Uninstall Milvus
    if [ -f "./scripts/uninstall_milvus.sh" ]; then
        bash ./scripts/uninstall_milvus.sh ${release} ${ns} || true
    else
        echo "Warning: uninstall_milvus.sh not found in expected location"
    fi
    
    exit $exit_code
}

# Set up trap to catch exits
trap cleanup EXIT

# Initialize basic variables
ns="chaos-testing"
cur_time=$(date +%H-%M-%S)
target_component=${1:-"standalone"}
chaos_type=${2:-"pod_kill"}
node_num=${3:-1}

log_step "Initializing with parameters: target_component=${target_component}, chaos_type=${chaos_type}, node_num=${node_num}"

# Generate release name
release_name="test"-${target_component}-${chaos_type/_/-}-${cur_time}
release=${RELEASE_NAME:-"${release_name}"}

# Normalize chaos type format
chaos_type=${chaos_type/-/_}
log_step "Configured chaos_type: ${chaos_type}"

# Change to scripts directory
pushd ./scripts || exit 1
log_step "Uninstalling existing Milvus instance if any"
bash uninstall_milvus.sh ${release} ${ns} || true

# Map component names
declare -A target_component_map=(["querynode"]="queryNode" ["indexnode"]="indexNode" ["datanode"]="dataNode" ["proxy"]="proxy")
log_step "Installing Milvus"

# Install cluster configuration if not standalone
if [[ ${target_component} != *"standalone"* ]]; then
    log_step "Installing cluster configuration"
    helm repo add milvus https://zilliztech.github.io/milvus-helm/
    helm repo update milvus
    helm install --wait --debug --timeout 360s ${release} milvus/milvus \
         --set ${target_component_map[${target_component}]}.replicas=$node_num \
         -f ../cluster-values.yaml -n=${ns}
fi

# Install standalone configuration
if [[ ${target_component} == *"standalone"* ]]; then
    log_step "Installing standalone configuration"
    helm install --wait --debug --timeout 360s ${release} milvus/milvus \
         -f ../standalone-values.yaml -n=${ns}
fi

# Wait for all pods to be ready
log_step "Waiting for pods to be ready"
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance=${release} -n ${ns} --timeout=360s
kubectl wait --for=condition=Ready pod -l release=${release} -n ${ns} --timeout=360s
kubectl get pod -o wide -l app.kubernetes.io/instance=${release} -n ${ns}
popd || exit 1

# Configure service and get LoadBalancer IP
log_step "Starting chaos testing"
kubectl patch svc ${release}-milvus -p='{"spec":{"type":"LoadBalancer"}}' -n ${ns}
loadbalancer_ip=$(kubectl get svc ${release}-milvus -n ${ns} -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
host=${loadbalancer_ip}

# Run initial e2e tests
log_step "Running initial e2e tests"
pytest -s -v ../testcases/test_e2e.py --host "$host" --log-cli-level=INFO --capture=no
python3 scripts/hello_milvus.py --host "$host"

# Run parallel chaos and request tests
log_step "Starting parallel chaos and request tests"
pytest test_chaos_apply.py --milvus_ns ${ns} --chaos_type ${chaos_type} \
      --target_component ${target_component} --host "$host" \
      --log-cli-level=INFO --capture=no &
pytest testcases/test_single_request_operation.py --host "$host" \
      --request_duration 15m --log-cli-level=INFO --capture=no &
wait

# Wait for system recovery after chaos tests
log_step "Waiting for pods to be ready after chaos tests"
kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance=${release} -n ${ns} --timeout=360s
kubectl wait --for=condition=Ready pod -l release=${release} -n ${ns} --timeout=360s

# Run final verification tests
log_step "Running final e2e tests"
pytest -s -v ../testcases/test_e2e.py --host "$host" --log-cli-level=INFO --capture=no || echo "e2e test fail"
python3 scripts/hello_milvus.py --host "$host" || echo "e2e test fail"