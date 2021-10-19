#!/bin/bash

# Copyright 2018 Istio Authors
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

# Output a message, with a timestamp matching istio log format
function log() {
  echo -e "$(date -u '+%Y-%m-%dT%H:%M:%S.%NZ')\t$*"
}

# Trace runs the provided command and records additional timing information
# NOTE: to avoid spamming the logs, we disable xtrace and re-enable it before executing the function
# and after completion. If xtrace was never set, this will result in xtrace being enabled.
# Ideally we would restore the old xtrace setting, but I don't think its possible to do that without also log-spamming
# If we need to call it from a context without xtrace we can just make a new function.
function trace() {
  { set +x; } 2>/dev/null
  log "Running '${1}'"
  start="$(date -u +%s)"
  { set -x; } 2>/dev/null

  "${@:2}"

  { set +x; } 2>/dev/null
  end="$(date -u +%s)"
  elapsed=$((end - start))
  log "Command '${1}' complete in ${elapsed}s"
  # Write to YAML file as well for easy reading by tooling
  echo "'${1}': $elapsed" >> "${ARTIFACTS}/trace.yaml"
  { set -x; } 2>/dev/null
}

function setup_and_export_git_sha() {
  if [[ -n "${CI:-}" ]]; then

    if [ -z "${PULL_PULL_SHA:-}" ]; then
      if [ -z "${PULL_BASE_SHA:-}" ]; then
        GIT_SHA="$(git rev-parse --verify HEAD)"
        export GIT_SHA
      else
        export GIT_SHA="${PULL_BASE_SHA}"
      fi
    else
      export GIT_SHA="${PULL_PULL_SHA}"
    fi
  else
    # Use the current commit.
    GIT_SHA="$(git rev-parse --verify HEAD)"
    export GIT_SHA
  fi
  GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
  export GIT_BRANCH
}

# Creates a local registry for kind nodes to pull images from. Expects that the "kind" network already exists.
function setup_kind_registry() {
  # create a registry container if it not running already
  running="$(docker inspect -f '{{.State.Running}}' "${KIND_REGISTRY_NAME}" 2>/dev/null || true)"
  if [[ "${running}" != 'true' ]]; then
      docker run \
        -d --restart=always -p "${KIND_REGISTRY_PORT}:5000" --name "${KIND_REGISTRY_NAME}" \
        registry:2

    # Allow kind nodes to reach the registry
    docker network connect "kind" "${KIND_REGISTRY_NAME}"
  fi

  # https://docs.tilt.dev/choosing_clusters.html#discovering-the-registry
  for cluster in $(kind get clusters); do
    # TODO get context/config from existing variables
    kind export kubeconfig --name="${cluster}"
    for node in $(kind get nodes --name="${cluster}"); do
      kubectl annotate node "${node}" "kind.x-k8s.io/registry=localhost:${KIND_REGISTRY_PORT}" --overwrite;
    done
  done
}

# The setup_cluster_reg is used to set up a cluster registry for multicluster testing
function setup_cluster_reg () {
    MAIN_CONFIG=""
    for context in "${CLUSTERREG_DIR}"/*; do
        if [[ -z "${MAIN_CONFIG}" ]]; then
            MAIN_CONFIG="${context}"
        fi
        export KUBECONFIG="${context}"
        kubectl delete ns istio-system-multi --ignore-not-found
        kubectl delete clusterrolebinding istio-multi-test --ignore-not-found
        kubectl create ns istio-system-multi
        kubectl create sa istio-multi-test -n istio-system-multi
        kubectl create clusterrolebinding istio-multi-test --clusterrole=cluster-admin --serviceaccount=istio-system-multi:istio-multi-test
        CLUSTER_NAME=$(kubectl config view --minify=true -o "jsonpath={.clusters[].name}")
        gen_kubeconf_from_sa istio-multi-test "${context}"
    done
    export KUBECONFIG="${MAIN_CONFIG}"
}

function gen_kubeconf_from_sa () {
    local service_account=$1
    local filename=$2

    SERVER=$(kubectl config view --minify=true -o "jsonpath={.clusters[].cluster.server}")
    SECRET_NAME=$(kubectl get sa "${service_account}" -n istio-system-multi -o jsonpath='{.secrets[].name}')
    CA_DATA=$(kubectl get secret "${SECRET_NAME}" -n istio-system-multi -o "jsonpath={.data['ca\\.crt']}")
    TOKEN=$(kubectl get secret "${SECRET_NAME}" -n istio-system-multi -o "jsonpath={.data['token']}" | base64 --decode)

    cat <<EOF > "${filename}"
      apiVersion: v1
      clusters:
         - cluster:
             certificate-authority-data: ${CA_DATA}
             server: ${SERVER}
           name: ${CLUSTER_NAME}
      contexts:
         - context:
             cluster: ${CLUSTER_NAME}
             user: ${CLUSTER_NAME}
           name: ${CLUSTER_NAME}
      current-context: ${CLUSTER_NAME}
      kind: Config
      preferences: {}
      users:
         - name: ${CLUSTER_NAME}
           user:
             token: ${TOKEN}
EOF
}

# Gives a copy of a given topology JSON editing the given key on the entry with the given cluster name
function set_topology_value() {
    local JSON="$1"
    local CLUSTER_NAME="$2"
    local KEY="$3"
    local VALUE="$4"
    VALUE=$(echo "${VALUE}" | awk '{$1=$1};1')

    echo "${JSON}" | jq '(.[] | select(.clusterName =="'"${CLUSTER_NAME}"'") | .'"${KEY}"') |="'"${VALUE}"'"'
}
