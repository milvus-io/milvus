#!/bin/bash

# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT="$( cd -P "$( dirname "$SOURCE" )/../.." && pwd )"

# Exit immediately for non zero status
set -e
# Check unset variables
set -u
# Print commands
set -x

# shellcheck source=build/lib.sh
source "${ROOT}/build/lib.sh"
setup_and_export_git_sha

# shellcheck source=build/kind_provisioner.sh
source "${ROOT}/build/kind_provisioner.sh"

TOPOLOGY=SINGLE_CLUSTER
NODE_IMAGE="kindest/node:v1.20.2"
KIND_CONFIG=""
INSTALL_EXTRA_ARG=""
TEST_EXTRA_ARG=""
CLUSTER_TOPOLOGY_CONFIG_FILE="${ROOT}/build/config/topology/multicluster.json"

while (( "$#" )); do
  case "$1" in
    # Node images can be found at https://github.com/kubernetes-sigs/kind/releases
    # For example, kindest/node:v1.14.0
    --node-image)
      NODE_IMAGE=$2
      shift 2
    ;;
    # Config for enabling different Kubernetes features in KinD (see build/config/topology/trustworthy-jwt.yaml).
    --kind-config)
    KIND_CONFIG=$2
    shift 2
    ;;
    --build-command)
    BUILD_COMMAND=$2
    shift 2
    ;;
    --install-extra-arg)
    INSTALL_EXTRA_ARG=$2
    shift 2
    ;;
    --test-extra-arg)
    TEST_EXTRA_ARG=$2
    shift 2
    ;;
    --test-timeout)
    TEST_TIMEOUT=$2
    shift 2
    ;;
    --skip-setup)
      SKIP_SETUP=true
      shift
    ;;
    --skip-install)
      SKIP_INSTALL=true
      shift
    ;;
    --skip-cleanup)
      SKIP_CLEANUP=true
      shift
    ;;
    --skip-build)
      SKIP_BUILD=true
      shift
    ;;
    --skip-build-image)
      SKIP_BUILD_IMAGE=true
      shift
    ;;
    --skip-test)
      SKIP_TEST=true
      shift
    ;;
    --skip-export-logs)
      SKIP_EXPORT_LOGS=true
      shift
    ;;
    --manual)
      MANUAL=true
      shift
    ;;
    --topology)
      case $2 in
        SINGLE_CLUSTER | MULTICLUSTER_SINGLE_NETWORK | MULTICLUSTER )
          TOPOLOGY=$2
          echo "Running with topology ${TOPOLOGY}"
          ;;
        *)
          echo "Error: Unsupported topology ${TOPOLOGY}" >&2
          exit 1
          ;;
      esac
      shift 2
    ;;
    --topology-config)
      CLUSTER_TOPOLOGY_CONFIG_FILE="${ROOT}/${2}"
      shift 2
    ;;
    -h|--help)
      { set +x; } 2>/dev/null
      HELP="
Usage:
  $0 [flags] [Arguments]

    --node-image                Kubernetes in Docker (KinD) Node image
                                The image is a Docker image for running nested containers, systemd, and Kubernetes components.
                                Node images can be found at https://github.com/kubernetes-sigs/kind/releases.
                                Default: \"kindest/node:v1.20.2\"

    --kind-config               Config for enabling different Kubernetes features in KinD

    --build-command             Specified build milvus command

    --install-extra-arg         Install Milvus Helm Chart extra configuration. (see https://github.com/zilliztech/milvus-helm-charts/blob/main/charts/milvus-ha/values.yaml)
                                To override values in a chart, use either the '--values' flag and pass in a file or use the '--set' flag and pass configuration from the command line, to force a string value use '--set-string'.
                                Refer: https://helm.sh/docs/helm/helm_install/#helm-install

    --test-extra-arg            Run e2e test extra configuration
                                For example, \"--tag=smoke\"

    --test-timeout              To specify timeout period of e2e test. Timeout time is specified in seconds.

    --topology                  KinD cluster topology of deployments
                                Provides three classes: \"SINGLE_CLUSTER\", \"MULTICLUSTER_SINGLE_NETWORK\", \"MULTICLUSTER\"
                                Default: \"SINGLE_CLUSTER\"

    --topology-config           KinD cluster topology configuration file

    --skip-setup                Skip setup KinD cluster

    --skip-install              Skip install Milvus Helm Chart

    --skip-cleanup              Skip cleanup KinD cluster

    --skip-build                Skip build Milvus binary

    --skip-build-image          Skip build Milvus image

    --skip-test                 Skip e2e test

    --skip-export-logs          Skip kind export logs

    --manual                    Manual Mode

    -h or --help                Print help information


Use \"$0  --help\" for more information about a given command.
"
      echo -e "${HELP}" ; exit 0
    ;;
    -*)
      echo "Error: Unsupported flag $1" >&2
      exit 1
      ;;
    *) # preserve positional arguments
      PARAMS+=("$1")
      shift
      ;;
  esac
done

export BUILD_COMMAND="${BUILD_COMMAND:-make install}"

export MANUAL="${MANUAL:-}"

# Default IP family of the cluster is IPv4
export IP_FAMILY="${IP_FAMILY:-ipv4}"

# KinD will not have a LoadBalancer, so we need to disable it
export TEST_ENV=kind
# LoadBalancer in Kind is supported using metallb if not ipv6.
if [ "${IP_FAMILY}" != "ipv6" ]; then
  export TEST_ENV=kind-metallb
fi

# See https://kind.sigs.k8s.io/docs/user/quick-start/#loading-an-image-into-your-cluster
export PULL_POLICY=IfNotPresent

# We run a local-registry in a docker container that KinD nodes pull from
# These values must match what are in config/trustworthy-jwt.yaml
export KIND_REGISTRY_NAME="kind-registry"
export KIND_REGISTRY_PORT="5000"
export KIND_REGISTRY="localhost:${KIND_REGISTRY_PORT}"

export ARTIFACTS="${ARTIFACTS:-$(mktemp -d)}"
export SINGLE_CLUSTER_NAME="${SINGLE_CLUSTER_NAME:-kind}"

export HUB="${HUB:-milvusdb}"
export TAG="${TAG:-latest}"

export CI="true"

if [[ ! -d "${ARTIFACTS}" ]];then
  mkdir -p "${ARTIFACTS}"
fi

if [[ ! -x "$(command -v kind)" ]]; then
  KIND_DIR="${KIND_DIR:-"${HOME}/tool_cache/kind"}"
  KIND_VERSION="v0.11.1"

  export PATH="${KIND_DIR}:${PATH}"
  if [[ ! -x "$(command -v kind)" ]]; then
    install_kind "${KIND_DIR}" "${KIND_VERSION}"
  fi
fi

if [[ ! -x "$(command -v kubectl)" ]]; then
  KUBECTL_DIR="${KUBECTL_DIR:-"${HOME}/tool_cache/kubectl"}"
  KUBECTL_VERSION="v1.20.2"

  export PATH="${KUBECTL_DIR}:${PATH}"
  if [[ ! -x "$(command -v kubectl)" ]]; then
    install_kubectl "${KUBECTL_DIR}" "${KUBECTL_VERSION}"
  fi
fi

if [[ ! -x "$(command -v helm)" ]]; then
  HELM_DIR="${HELM_DIR:-"${HOME}/tool_cache/helm"}"
  HELM_VERSION="v3.5.4"

  export PATH="${HELM_DIR}:${PATH}"
  if [[ ! -x "$(command -v helm)" ]]; then
    install_helm "${HELM_DIR}" "${HELM_VERSION}"
  fi
fi

if [[ -z "${SKIP_SETUP:-}" ]]; then
  export DEFAULT_CLUSTER_YAML="${ROOT}/build/config/topology/trustworthy-jwt.yaml"
  export METRICS_SERVER_CONFIG_DIR="${ROOT}/build/config/metrics"

  if [[ "${TOPOLOGY}" == "SINGLE_CLUSTER" ]]; then
    trace "setup kind cluster" setup_kind_cluster "${SINGLE_CLUSTER_NAME}" "${NODE_IMAGE}" "${KIND_CONFIG}"
  else
    trace "load cluster topology" load_cluster_topology "${CLUSTER_TOPOLOGY_CONFIG_FILE}"
    trace "setup kind clusters" setup_kind_clusters "${NODE_IMAGE}" "${IP_FAMILY}"

    TOPOLOGY_JSON=$(cat "${CLUSTER_TOPOLOGY_CONFIG_FILE}")
    for i in $(seq 0 $((${#CLUSTER_NAMES[@]} - 1))); do
      CLUSTER="${CLUSTER_NAMES[i]}"
      KCONFIG="${KUBECONFIGS[i]}"
      TOPOLOGY_JSON=$(set_topology_value "${TOPOLOGY_JSON}" "${CLUSTER}" "meta.kubeconfig" "${KCONFIG}")
    done
    RUNTIME_TOPOLOGY_CONFIG_FILE="${ARTIFACTS}/topology-config.json"
    echo "${TOPOLOGY_JSON}" > "${RUNTIME_TOPOLOGY_CONFIG_FILE}"

    export INTEGRATION_TEST_TOPOLOGY_FILE
    INTEGRATION_TEST_TOPOLOGY_FILE="${RUNTIME_TOPOLOGY_CONFIG_FILE}"

    export INTEGRATION_TEST_KUBECONFIG
    INTEGRATION_TEST_KUBECONFIG=NONE
  fi
fi

if [[ -z "${SKIP_BUILD:-}" ]]; then
  trace "setup kind registry" setup_kind_registry
  pushd "${ROOT}"
    trace "build milvus" "${ROOT}/build/builder.sh" /bin/bash -c "${BUILD_COMMAND}"
  popd
fi

if [[ -z "${SKIP_BUILD_IMAGE:-}" ]]; then
  # If we're not intending to pull from an actual remote registry, use the local kind registry
  running="$(docker inspect -f '{{.State.Running}}' "${KIND_REGISTRY_NAME}" 2>/dev/null || true)"
  if [[ "${running}" == 'true' ]]; then
    HUB="${KIND_REGISTRY}"
    export HUB
  fi
  export MILVUS_IMAGE_REPO="${HUB}/milvus"
  export MILVUS_IMAGE_TAG="${TAG}"

  pushd "${ROOT}"
    trace "build milvus image" "${ROOT}/build/build_image.sh"
    trace "push milvus image" docker push "${MILVUS_IMAGE_REPO}:${MILVUS_IMAGE_TAG}"
  popd
fi

if [[ -z "${SKIP_INSTALL:-}" ]]; then
  trace "install milvus helm chart" "${ROOT}/tests/scripts/install_milvus.sh" "${INSTALL_EXTRA_ARG}"
fi

if [[ -z "${SKIP_TEST:-}" ]]; then
  trace "prepare e2e test" "${ROOT}/tests/scripts/prepare_e2e.sh"
  if [[ -n "${TEST_TIMEOUT:-}" ]]; then
    trace "e2e test" "timeout" "-v" "${TEST_TIMEOUT}" "${ROOT}/tests/scripts/e2e.sh" "${TEST_EXTRA_ARG}"
  else
    trace "e2e test" "${ROOT}/tests/scripts/e2e.sh" "${TEST_EXTRA_ARG}"
  fi
fi

# Check if the user is running the clusters in manual mode.
if [[ -n "${MANUAL:-}" ]]; then
  echo "Running cluster(s) in manual mode. Press any key to shutdown and exit..."
  read -rsn1
  exit 0
fi
