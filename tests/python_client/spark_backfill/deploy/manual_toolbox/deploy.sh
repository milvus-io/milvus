#!/usr/bin/env bash

set -euo pipefail

KUBECONFIG_PATH="${1:?usage: deploy.sh <kubeconfig> [namespace]}"
NAMESPACE="${2:-default}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOYMENT_FILE="$SCRIPT_DIR/deployment.yaml"

kubectl --kubeconfig "$KUBECONFIG_PATH" -n "$NAMESPACE" create secret generic \
  spark-milvus-toolbox-credentials \
  --from-literal=milvus-token='root:Milvus' \
  --dry-run=client -o yaml \
  | kubectl --kubeconfig "$KUBECONFIG_PATH" -n "$NAMESPACE" apply -f -

kubectl --kubeconfig "$KUBECONFIG_PATH" -n "$NAMESPACE" create configmap \
  spark-milvus-toolbox-scripts \
  --from-file=build-connector.sh="$SCRIPT_DIR/build-connector.sh" \
  --from-file=spark-submit-milvus.sh="$SCRIPT_DIR/spark-submit-milvus.sh" \
  --dry-run=client -o yaml \
  | kubectl --kubeconfig "$KUBECONFIG_PATH" -n "$NAMESPACE" apply -f -

sed \
  -e "s/namespace: default/namespace: $NAMESPACE/g" \
  "$DEPLOYMENT_FILE" \
  | kubectl --kubeconfig "$KUBECONFIG_PATH" -n "$NAMESPACE" apply -f -

kubectl --kubeconfig "$KUBECONFIG_PATH" -n "$NAMESPACE" rollout status \
  deployment/spark-milvus-toolbox \
  --timeout=180m
