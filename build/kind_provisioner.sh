#!/bin/bash

# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# WARNING: DO NOT EDIT, THIS FILE IS PROBABLY A COPY
#
# The original version of this file is located in the https://github.com/istio/common-files repo.
# If you're looking at this file in a different repo and want to make a change, please go to the
# common-files repo, make the change there and check it in. Then come back to this repo and run
# "make update-common".

# Copyright Istio Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
set -x

# The purpose of this file is to unify prow/lib.sh in both istio and istio.io
# repos to avoid code duplication.

####################################################################
#################   COMMON SECTION   ###############################
####################################################################

# DEFAULT_KIND_IMAGE is used to set the Kubernetes version for KinD unless overridden in params to setup_kind_cluster(s)
DEFAULT_KIND_IMAGE="kindest/node:v1.20.2"

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT="$( cd -P "$( dirname "$SOURCE" )/.." && pwd )"

UNAME="$(uname -s)"
case "${UNAME}" in
    Linux*)     MACHINE=Linux;;
    Darwin*)    MACHINE=Mac;;
    CYGWIN*)    MACHINE=Cygwin;;
    MINGW*)     MACHINE=MinGw;;
    *)          MACHINE="UNKNOWN:${UNAME}"
esac

# load_cluster_topology function reads cluster configuration topology file and
# sets up environment variables used by other functions. So this should be called
# before anything else.
#
# Note: Cluster configuration topology file specifies basic configuration of each
# KinD cluster like its name, pod and service subnets and network_id. If two cluster
# have the same network_id then they belong to the same network and their pods can
# talk to each other directly.
#
# [{ "cluster_name": "cluster1","pod_subnet": "10.10.0.0/16","svc_subnet": "10.255.10.0/24","network_id": "0" },
#  { "cluster_name": "cluster2","pod_subnet": "10.20.0.0/16","svc_subnet": "10.255.20.0/24","network_id": "0" },
#  { "cluster_name": "cluster3","pod_subnet": "10.30.0.0/16","svc_subnet": "10.255.30.0/24","network_id": "1" }]
function load_cluster_topology() {
  CLUSTER_TOPOLOGY_CONFIG_FILE="${1}"

  if [[ ! -f "${CLUSTER_TOPOLOGY_CONFIG_FILE}" ]]; then
    echo 'cluster topology configuration file is not specified'
    exit 1
  fi

  export CLUSTER_NAMES
  export CLUSTER_POD_SUBNETS
  export CLUSTER_SVC_SUBNETS
  export CLUSTER_NETWORK_ID

  KUBE_CLUSTERS=$(jq '.[] | select(.kind == "Kubernetes" or .kind == null)' "${CLUSTER_TOPOLOGY_CONFIG_FILE}")

  while read -r value; do
    CLUSTER_NAMES+=("$value")
  done < <(echo "${KUBE_CLUSTERS}" | jq -r '.cluster_name // .clusterName')

  while read -r value; do
    CLUSTER_POD_SUBNETS+=("$value")
  done < <(echo "${KUBE_CLUSTERS}" | jq -r '.pod_subnet // .podSubnet')

  while read -r value; do
    CLUSTER_SVC_SUBNETS+=("$value")
  done < <(echo "${KUBE_CLUSTERS}" | jq -r '.svc_subnet // .svcSubnet')

  while read -r value; do
    CLUSTER_NETWORK_ID+=("$value")
  done < <(echo "${KUBE_CLUSTERS}" | jq -r '.network_id // .network')

  export NUM_CLUSTERS
  NUM_CLUSTERS=$(echo "${KUBE_CLUSTERS}" | jq -s 'length')

  echo "${CLUSTER_NAMES[@]}"
  echo "${CLUSTER_POD_SUBNETS[@]}"
  echo "${CLUSTER_SVC_SUBNETS[@]}"
  echo "${CLUSTER_NETWORK_ID[@]}"
  echo "${NUM_CLUSTERS}"
}

#####################################################################
###################   SINGLE-CLUSTER SECTION   ######################
#####################################################################

# cleanup_kind_cluster takes a single parameter NAME
# and deletes the KinD cluster with that name
function cleanup_kind_cluster() {
  echo "Test exited with exit code $?."
  NAME="${1}"
  if [[ -z "${SKIP_EXPORT_LOGS:-}" ]]; then
    kind export logs --name "${NAME}" "${ARTIFACTS}/kind" -v9 || true
  fi
  if [[ -z "${SKIP_CLEANUP:-}" ]]; then
    echo "Cleaning up kind cluster"
    kind delete cluster --name "${NAME}" -v9 || true
    docker network rm kind > /dev/null 2>&1 || true
  fi
}

# check_default_cluster_yaml checks the presence of default cluster YAML
# It returns 1 if it is not present
function check_default_cluster_yaml() {
  if [[ -z "${DEFAULT_CLUSTER_YAML}" ]]; then
    echo 'DEFAULT_CLUSTER_YAML file must be specified. Exiting...'
    return 1
  fi
}

# setup_kind_cluster creates new KinD cluster with given name, image and configuration
# 1. NAME: Name of the Kind cluster (optional)
# 2. IMAGE: Node image used by KinD (optional)
# 3. CONFIG: KinD cluster configuration YAML file. If not specified then DEFAULT_CLUSTER_YAML is used
# 4. NOMETALBINSTALL: Dont install matllb if set true.
# 5. CRON_LOGGER_INSTALL: Install Cron Logger if set true.
# This function returns 0 when everything goes well, or 1 otherwise
# If Kind cluster was already created then it would be cleaned up in case of errors
function setup_kind_cluster() {
  NAME="${1:-kind}"
  IMAGE="${2:-"${DEFAULT_KIND_IMAGE}"}"
  CONFIG="${3:-}"
  NOMETALBINSTALL="${4:-false}"
  CRON_LOGGER_INSTALL="${5:-true}"

  check_default_cluster_yaml

  # Delete any previous KinD cluster
  echo "Deleting previous KinD cluster with name=${NAME}"
  if ! (kind delete cluster --name="${NAME}" -v9) > /dev/null; then
    echo "No existing kind cluster with name ${NAME}. Continue..."
  else
    docker network rm kind > /dev/null 2>&1 || true
  fi

  # explicitly disable shellcheck since we actually want $NAME to expand now
  # shellcheck disable=SC2064
  trap "cleanup_kind_cluster ${NAME}" EXIT

    # If config not explicitly set, then use defaults
  if [[ -z "${CONFIG}" ]]; then
    # Kubernetes 1.15+
    CONFIG=${DEFAULT_CLUSTER_YAML}
    # Configure the cluster IP Family only for default configs
    if [ "${IP_FAMILY}" = "ipv6" ]; then
      grep 'ipFamily: ipv6' "${CONFIG}" || \
      cat <<EOF >> "${CONFIG}"
networking:
  ipFamily: ipv6
EOF
    fi
  fi

  # Create KinD cluster
  if ! (kind create cluster --name="${NAME}" --config "${CONFIG}" -v9 --retain --image "${IMAGE}" --wait=60s); then
    echo "Could not setup KinD environment. Something wrong with KinD setup. Exporting logs."
    exit 1
  fi

  # If metrics server configuration directory is specified then deploy in
  # the cluster just created
  if [[ -n ${METRICS_SERVER_CONFIG_DIR} ]]; then
    kubectl apply -f "${METRICS_SERVER_CONFIG_DIR}"
  fi

  # Install Metallb if not set to install explicitly
  if [[ "${NOMETALBINSTALL}" != "true" ]]; then
    install_metallb ""
  fi

  # Install Cron logger if set to install explicitly'
  if [[ "${CRON_LOGGER_INSTALL}" == "true" ]]; then
    install_cron_logger ""
  fi
}

###############################################################################
####################    MULTICLUSTER SECTION    ###############################
###############################################################################

# Cleans up the clusters created by setup_kind_clusters
# It expects CLUSTER_NAMES to be present which means that
# load_cluster_topology must be called before invoking it
function cleanup_kind_clusters() {
  echo "Test exited with exit code $?."
  for c in "${CLUSTER_NAMES[@]}"; do
    cleanup_kind_cluster "${c}"
  done
}

# setup_kind_clusters sets up a given number of kind clusters with given topology
# as specified in cluster topology configuration file.
# 1. IMAGE = docker image used as node by KinD
# 2. IP_FAMILY = either ipv4 or ipv6
#
# NOTE: Please call load_cluster_topology before calling this method as it expects
# cluster topology information to be loaded in advance
function setup_kind_clusters() {
  IMAGE="${1:-"${DEFAULT_KIND_IMAGE}"}"
  KUBECONFIG_DIR="${ARTIFACTS:-$(mktemp -d)}/kubeconfig"
  IP_FAMILY="${2:-ipv4}"

  check_default_cluster_yaml

  # Trap replaces any previous trap's, so we need to explicitly cleanup both clusters here
  trap cleanup_kind_clusters EXIT

  function deploy_kind() {
    IDX="${1}"
    CLUSTER_NAME="${CLUSTER_NAMES[$IDX]}"
    CLUSTER_POD_SUBNET="${CLUSTER_POD_SUBNETS[$IDX]}"
    CLUSTER_SVC_SUBNET="${CLUSTER_SVC_SUBNETS[$IDX]}"
    CLUSTER_YAML="${ARTIFACTS}/config-${CLUSTER_NAME}.yaml"
    if [ ! -f "${CLUSTER_YAML}" ]; then
      cp "${DEFAULT_CLUSTER_YAML}" "${CLUSTER_YAML}"
      cat <<EOF >> "${CLUSTER_YAML}"
networking:
  podSubnet: ${CLUSTER_POD_SUBNET}
  serviceSubnet: ${CLUSTER_SVC_SUBNET}
EOF
    fi

    CLUSTER_KUBECONFIG="${KUBECONFIG_DIR}/${CLUSTER_NAME}"

    # Create the clusters.
    KUBECONFIG="${CLUSTER_KUBECONFIG}" setup_kind_cluster "${CLUSTER_NAME}" "${IMAGE}" "${CLUSTER_YAML}" "true" "true"

    # Kind currently supports getting a kubeconfig for internal or external usage. To simplify our tests,
    # its much simpler if we have a single kubeconfig that can be used internally and externally.
    # To do this, we can replace the server with the IP address of the docker container
    # https://github.com/kubernetes-sigs/kind/issues/1558 tracks this upstream
    CONTAINER_IP=$(docker inspect "${CLUSTER_NAME}-control-plane" --format "{{ .NetworkSettings.Networks.kind.IPAddress }}")
    kind get kubeconfig --name "${CLUSTER_NAME}" --internal | \
      sed "s/${CLUSTER_NAME}-control-plane/${CONTAINER_IP}/g" > "${CLUSTER_KUBECONFIG}"

    # Enable core dumps
    docker exec "${CLUSTER_NAME}"-control-plane bash -c "sysctl -w kernel.core_pattern=/var/lib/istio/data/core.proxy && ulimit -c unlimited"
  }

  # Now deploy the specified number of KinD clusters and
  # wait till they are provisioned successfully.
  declare -a DEPLOY_KIND_JOBS
  for i in "${!CLUSTER_NAMES[@]}"; do
    deploy_kind "${i}" & DEPLOY_KIND_JOBS+=("${!}")
  done

  for pid in "${DEPLOY_KIND_JOBS[@]}"; do
    wait "${pid}" || exit 1
  done

  # Install MetalLB for LoadBalancer support. Must be done synchronously since METALLB_IPS is shared.
  # and keep track of the list of Kubeconfig files that will be exported later
  export KUBECONFIGS
  for CLUSTER_NAME in "${CLUSTER_NAMES[@]}"; do
    KUBECONFIG_FILE="${KUBECONFIG_DIR}/${CLUSTER_NAME}"
    if [[ ${NUM_CLUSTERS} -gt 1 ]]; then
      install_metallb "${KUBECONFIG_FILE}"
      # Install Cron logger if set to install explicitly'
      if [[ -n ${CRON_LOGGER_INSTALL} ]]; then
        install_cron_logger "${KUBECONFIG_FILE}"
      fi
    fi
    KUBECONFIGS+=("${KUBECONFIG_FILE}")
  done

  ITER_END=$((NUM_CLUSTERS-1))
  for i in $(seq 0 "$ITER_END"); do
    for j in $(seq 0 "$ITER_END"); do
      if [[ "${j}" -gt "${i}" ]]; then
        NETWORK_ID_I="${CLUSTER_NETWORK_ID[i]}"
        NETWORK_ID_J="${CLUSTER_NETWORK_ID[j]}"
        if [[ "$NETWORK_ID_I" == "$NETWORK_ID_J" ]]; then
          POD_TO_POD_AND_SERVICE_CONNECTIVITY=1
        else
          POD_TO_POD_AND_SERVICE_CONNECTIVITY=0
        fi
        connect_kind_clusters \
          "${CLUSTER_NAMES[i]}" "${KUBECONFIGS[i]}" \
          "${CLUSTER_NAMES[j]}" "${KUBECONFIGS[j]}" \
          "${POD_TO_POD_AND_SERVICE_CONNECTIVITY}"
      fi
    done
  done
}

function connect_kind_clusters() {
  C1="${1}"
  C1_KUBECONFIG="${2}"
  C2="${3}"
  C2_KUBECONFIG="${4}"
  POD_TO_POD_AND_SERVICE_CONNECTIVITY="${5}"

  C1_NODE="${C1}-control-plane"
  C2_NODE="${C2}-control-plane"
  C1_DOCKER_IP=$(docker inspect -f "{{ .NetworkSettings.Networks.kind.IPAddress }}" "${C1_NODE}")
  C2_DOCKER_IP=$(docker inspect -f "{{ .NetworkSettings.Networks.kind.IPAddress }}" "${C2_NODE}")
  if [ "${POD_TO_POD_AND_SERVICE_CONNECTIVITY}" -eq 1 ]; then
    # Set up routing rules for inter-cluster direct pod to pod & service communication
    C1_POD_CIDR=$(KUBECONFIG="${C1_KUBECONFIG}" kubectl get node -ojsonpath='{.items[0].spec.podCIDR}')
    C2_POD_CIDR=$(KUBECONFIG="${C2_KUBECONFIG}" kubectl get node -ojsonpath='{.items[0].spec.podCIDR}')
    C1_SVC_CIDR=$(KUBECONFIG="${C1_KUBECONFIG}" kubectl cluster-info dump | sed -n 's/^.*--service-cluster-ip-range=\([^"]*\).*$/\1/p' | head -n 1)
    C2_SVC_CIDR=$(KUBECONFIG="${C2_KUBECONFIG}" kubectl cluster-info dump | sed -n 's/^.*--service-cluster-ip-range=\([^"]*\).*$/\1/p' | head -n 1)
    docker exec "${C1_NODE}" ip route add "${C2_POD_CIDR}" via "${C2_DOCKER_IP}"
    docker exec "${C1_NODE}" ip route add "${C2_SVC_CIDR}" via "${C2_DOCKER_IP}"
    docker exec "${C2_NODE}" ip route add "${C1_POD_CIDR}" via "${C1_DOCKER_IP}"
    docker exec "${C2_NODE}" ip route add "${C1_SVC_CIDR}" via "${C1_DOCKER_IP}"
  fi

  # Set up routing rules for inter-cluster pod to MetalLB LoadBalancer communication
  connect_metallb "$C1_NODE" "$C2_KUBECONFIG" "$C2_DOCKER_IP"
  connect_metallb "$C2_NODE" "$C1_KUBECONFIG" "$C1_DOCKER_IP"
}

function install_kind() {
    KIND_DIR=$1
    KIND_VERSION=$2
    echo 'Installing kind...'

    mkdir -p "${KIND_DIR}"

    if [[ "${MACHINE}" == "Linux" ]]; then
      curl -sSLo "${KIND_DIR}/kind" "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-amd64"
    elif [[ "${MACHINE}" == "Mac" ]]; then
      curl -sSLo "${KIND_DIR}/kind" "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-darwin-amd64"
    else
      echo "Error Download kind ..."
      exit 1
    fi
    chmod +x "${KIND_DIR}/kind"
}

function install_kubectl() {
    KUBECTL_DIR=$1
    KUBECTL_VERSION=$2
    echo 'Installing kubectl...'

    mkdir -p "${KUBECTL_DIR}"

    if [[ "${MACHINE}" == "Linux" ]]; then
      curl -sSLo "${KUBECTL_DIR}/kubectl" "https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
    elif [[ "${MACHINE}" == "Mac" ]]; then
      curl -sSLo "${KUBECTL_DIR}/kubectl" "https://storage.googleapis.com/kubernetes-release/release/${KUBECTL_VERSION}/bin/darwin/amd64/kubectl"
    else
      echo "Error Download kubectl ..."
      exit 1
    fi
    chmod +x "${KUBECTL_DIR}/kubectl"
}

function install_helm() {
    HELM_DIR=$1
    HELM_VERSION=$2
    echo 'Installing helm...'

    mkdir -p "${HELM_DIR}"

    OS_NAME="unknown"
    if [[ "${MACHINE}" == "Linux" ]]; then
      OS_NAME="linux"
    elif [[ "${MACHINE}" == "Mac" ]]; then
      OS_NAME="darwin"
    else
      echo "Error Download helm ..."
      exit 1
    fi
    curl -sSLo "${HELM_DIR}/helm.tar.gz" "https://get.helm.sh/helm-${HELM_VERSION}-${OS_NAME}-amd64.tar.gz"
    tar zxvf "${HELM_DIR}/helm.tar.gz" -C "${HELM_DIR}"
    mv "${HELM_DIR}/${OS_NAME}-amd64/helm" "${HELM_DIR}"
    chmod +x "${HELM_DIR}/helm"
}

function install_metallb() {
  KUBECONFIG="${1}"
  kubectl apply --kubeconfig="$KUBECONFIG" -f "${ROOT}/build/config/metallb.yaml"
  kubectl create --kubeconfig="$KUBECONFIG" secret generic -n metallb-system memberlist --from-literal=secretkey="$(openssl rand -base64 128)"

  if [ -z "${METALLB_IPS[*]-}" ]; then
    # Take IPs from the end of the docker kind network subnet to use for MetalLB IPs
    DOCKER_KIND_SUBNET="$(docker inspect kind | jq '.[0].IPAM.Config[0].Subnet' -r)"
    METALLB_IPS=()
    while read -r ip; do
      METALLB_IPS+=("$ip")
    done < <(cidr_to_ips "$DOCKER_KIND_SUBNET" | tail -n 100)
  fi

  # Give this cluster of those IPs
  RANGE="${METALLB_IPS[0]}-${METALLB_IPS[9]}"
  METALLB_IPS=("${METALLB_IPS[@]:10}")

  echo 'apiVersion: v1
kind: ConfigMap
metadata:
  namespace: metallb-system
  name: config
data:
  config: |
    address-pools:
    - name: default
      protocol: layer2
      addresses:
      - '"$RANGE" | kubectl apply --kubeconfig="$KUBECONFIG" -f -
}

function install_cron_logger() {
  KUBECONFIG="${1}"
  kubectl apply --kubeconfig="$KUBECONFIG" -f "${ROOT}/build/config/logging/"
}

function connect_metallb() {
  REMOTE_NODE=$1
  METALLB_KUBECONFIG=$2
  METALLB_DOCKER_IP=$3

  IP_REGEX='(([0-9]{1,3}\.?){4})'
  LB_CONFIG="$(kubectl --kubeconfig="${METALLB_KUBECONFIG}" -n metallb-system get cm config -o jsonpath="{.data.config}")"
  if [[ "$LB_CONFIG" =~ $IP_REGEX-$IP_REGEX ]]; then
    while read -r lb_cidr; do
      docker exec "${REMOTE_NODE}" ip route add "${lb_cidr}" via "${METALLB_DOCKER_IP}"
    done < <(ips_to_cidrs "${BASH_REMATCH[1]}" "${BASH_REMATCH[3]}")
  fi
}

function cidr_to_ips() {
    CIDR="$1"
    python3 - <<EOF
from ipaddress import IPv4Network; [print(str(ip)) for ip in IPv4Network('$CIDR').hosts()]
EOF
}

function ips_to_cidrs() {
  IP_RANGE_START="$1"
  IP_RANGE_END="$2"
  python3 - <<EOF
from ipaddress import summarize_address_range, IPv4Address
[ print(n.compressed) for n in summarize_address_range(IPv4Address(u'$IP_RANGE_START'), IPv4Address(u'$IP_RANGE_END')) ]
EOF
}
