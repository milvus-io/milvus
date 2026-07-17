#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

RELEASE_NAME="${RELEASE_NAME:-}"
NAMESPACE="${NAMESPACE:-jenkins-milvus-ci}"
VALUES_FILE="${VALUES_FILE:-tests/_helm/values/nightly/distributed-woodpecker}"
CHART_REPO_NAME="${CHART_REPO_NAME:-zilliztech}"
CHART_REPO_URL="${CHART_REPO_URL:-https://nexus-ci.zilliz.cc/repository/milvus-proxy}"
CHART_NAME="${CHART_NAME:-zilliztech/milvus}"
CHART_VERSION="${CHART_VERSION:-5.0.16}"
MILVUS_IMAGE_REPOSITORY="${MILVUS_IMAGE_REPOSITORY:-harbor.milvus.io/milvusdb/milvus}"
MILVUS_IMAGE_TAG="${MILVUS_IMAGE_TAG:-}"
WOODPECKER_IMAGE_REPOSITORY="${WOODPECKER_IMAGE_REPOSITORY:-harbor.milvus.io/woodpecker/woodpecker}"
WOODPECKER_IMAGE_TAG="${WOODPECKER_IMAGE_TAG:-}"
PD_IMAGE="${PD_IMAGE:-pingcap/pd:v8.5.3}"
TIKV_IMAGE="${TIKV_IMAGE:-pingcap/tikv:v8.5.3}"
TIKV_STORAGE_SIZE="${TIKV_STORAGE_SIZE:-10Gi}"
TIKV_WAIT_TIMEOUT="${TIKV_WAIT_TIMEOUT:-300s}"
HELM_TIMEOUT="${HELM_TIMEOUT:-1200s}"

usage() {
  cat <<EOF
Usage:
  RELEASE_NAME=<name> MILVUS_IMAGE_TAG=<tag> [WOODPECKER_IMAGE_TAG=<tag>] $0

Optional environment variables:
  NAMESPACE                  default: jenkins-milvus-ci
  VALUES_FILE                default: tests/_helm/values/nightly/distributed-woodpecker
  CHART_VERSION              default: 5.0.16
  PD_IMAGE                   default: pingcap/pd:v8.5.3
  TIKV_IMAGE                 default: pingcap/tikv:v8.5.3
  TIKV_STORAGE_SIZE          default: 10Gi
EOF
}

if [[ -z "$RELEASE_NAME" || -z "$MILVUS_IMAGE_TAG" ]]; then
  usage >&2
  exit 2
fi

cd "$ROOT_DIR"

if [[ ! -f "$VALUES_FILE" ]]; then
  echo "values file not found: $VALUES_FILE" >&2
  exit 2
fi

TIKV_VALUES_FILE="tests/_helm/values/nightly/tikv"
if [[ ! -f "$TIKV_VALUES_FILE" ]]; then
  echo "tikv overlay values file not found: $TIKV_VALUES_FILE" >&2
  exit 2
fi

PD_SERVICE="${RELEASE_NAME}-tikv-pd"
PD_HEADLESS="${RELEASE_NAME}-tikv-pd-peer"
TIKV_HEADLESS="${RELEASE_NAME}-tikv-peer"

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

cat <<EOF | kubectl apply -n "$NAMESPACE" -f -
apiVersion: v1
kind: Service
metadata:
  name: ${PD_SERVICE}
  labels:
    app.kubernetes.io/name: tikv
    app.kubernetes.io/instance: ${RELEASE_NAME}
    app.kubernetes.io/component: pd
spec:
  selector:
    app.kubernetes.io/name: tikv
    app.kubernetes.io/instance: ${RELEASE_NAME}
    app.kubernetes.io/component: pd
  ports:
  - name: client
    port: 2379
    targetPort: 2379
---
apiVersion: v1
kind: Service
metadata:
  name: ${PD_HEADLESS}
  labels:
    app.kubernetes.io/name: tikv
    app.kubernetes.io/instance: ${RELEASE_NAME}
    app.kubernetes.io/component: pd
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app.kubernetes.io/name: tikv
    app.kubernetes.io/instance: ${RELEASE_NAME}
    app.kubernetes.io/component: pd
  ports:
  - name: client
    port: 2379
    targetPort: 2379
  - name: peer
    port: 2380
    targetPort: 2380
---
apiVersion: v1
kind: Service
metadata:
  name: ${TIKV_HEADLESS}
  labels:
    app.kubernetes.io/name: tikv
    app.kubernetes.io/instance: ${RELEASE_NAME}
    app.kubernetes.io/component: tikv
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app.kubernetes.io/name: tikv
    app.kubernetes.io/instance: ${RELEASE_NAME}
    app.kubernetes.io/component: tikv
  ports:
  - name: server
    port: 20160
    targetPort: 20160
  - name: status
    port: 20180
    targetPort: 20180
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ${RELEASE_NAME}-tikv-pd
  labels:
    app.kubernetes.io/name: tikv
    app.kubernetes.io/instance: ${RELEASE_NAME}
    app.kubernetes.io/component: pd
spec:
  serviceName: ${PD_HEADLESS}
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: tikv
      app.kubernetes.io/instance: ${RELEASE_NAME}
      app.kubernetes.io/component: pd
  template:
    metadata:
      labels:
        app.kubernetes.io/name: tikv
        app.kubernetes.io/instance: ${RELEASE_NAME}
        app.kubernetes.io/component: pd
    spec:
      affinity:
        nodeAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - preference:
              matchExpressions:
              - key: jenkins-e2e-amd
                operator: In
                values:
                - "true"
            weight: 100
          - preference:
              matchExpressions:
              - key: node-role.kubernetes.io/e2e
                operator: Exists
            weight: 1
      tolerations:
      - effect: PreferNoSchedule
        key: jenkins-milvus-ci-only
        operator: Equal
        value: "true"
      - effect: NoSchedule
        key: node-role.kubernetes.io/e2e
        operator: Exists
      containers:
      - name: pd
        image: ${PD_IMAGE}
        imagePullPolicy: IfNotPresent
        command:
        - /bin/sh
        - -ec
        - |
          exec /pd-server \\
            --name="\${POD_NAME}" \\
            --data-dir=/data/pd \\
            --client-urls=http://0.0.0.0:2379 \\
            --advertise-client-urls="http://\${POD_NAME}.${PD_HEADLESS}.${NAMESPACE}.svc:2379" \\
            --peer-urls=http://0.0.0.0:2380 \\
            --advertise-peer-urls="http://\${POD_NAME}.${PD_HEADLESS}.${NAMESPACE}.svc:2380" \\
            --initial-cluster="\${POD_NAME}=http://\${POD_NAME}.${PD_HEADLESS}.${NAMESPACE}.svc:2380" \\
            --log-file=""
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        ports:
        - name: client
          containerPort: 2379
        - name: peer
          containerPort: 2380
        resources:
          requests:
            cpu: "0.2"
            memory: 512Mi
          limits:
            cpu: "1"
            memory: 2Gi
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        emptyDir:
          sizeLimit: ${TIKV_STORAGE_SIZE}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ${RELEASE_NAME}-tikv
  labels:
    app.kubernetes.io/name: tikv
    app.kubernetes.io/instance: ${RELEASE_NAME}
    app.kubernetes.io/component: tikv
spec:
  serviceName: ${TIKV_HEADLESS}
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: tikv
      app.kubernetes.io/instance: ${RELEASE_NAME}
      app.kubernetes.io/component: tikv
  template:
    metadata:
      labels:
        app.kubernetes.io/name: tikv
        app.kubernetes.io/instance: ${RELEASE_NAME}
        app.kubernetes.io/component: tikv
    spec:
      affinity:
        nodeAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - preference:
              matchExpressions:
              - key: jenkins-e2e-amd
                operator: In
                values:
                - "true"
            weight: 100
          - preference:
              matchExpressions:
              - key: node-role.kubernetes.io/e2e
                operator: Exists
            weight: 1
      tolerations:
      - effect: PreferNoSchedule
        key: jenkins-milvus-ci-only
        operator: Equal
        value: "true"
      - effect: NoSchedule
        key: node-role.kubernetes.io/e2e
        operator: Exists
      containers:
      - name: tikv
        image: ${TIKV_IMAGE}
        imagePullPolicy: IfNotPresent
        command:
        - /bin/sh
        - -ec
        - |
          exec /tikv-server \\
            --addr=0.0.0.0:20160 \\
            --advertise-addr="\${POD_NAME}.${TIKV_HEADLESS}.${NAMESPACE}.svc:20160" \\
            --status-addr=0.0.0.0:20180 \\
            --pd=${PD_SERVICE}:2379 \\
            --data-dir=/data/tikv \\
            --log-file=""
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        ports:
        - name: server
          containerPort: 20160
        - name: status
          containerPort: 20180
        resources:
          requests:
            cpu: "0.5"
            memory: 1Gi
          limits:
            cpu: "2"
            memory: 4Gi
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        emptyDir:
          sizeLimit: ${TIKV_STORAGE_SIZE}
EOF

kubectl rollout status "statefulset/${RELEASE_NAME}-tikv-pd" -n "$NAMESPACE" --timeout="$TIKV_WAIT_TIMEOUT"
kubectl rollout status "statefulset/${RELEASE_NAME}-tikv" -n "$NAMESPACE" --timeout="$TIKV_WAIT_TIMEOUT"

helm repo add "$CHART_REPO_NAME" "$CHART_REPO_URL" || true
helm repo update

helm_args=(
  upgrade --install "$RELEASE_NAME" "$CHART_NAME"
  --wait --debug --timeout "$HELM_TIMEOUT"
  --namespace "$NAMESPACE"
  --version "$CHART_VERSION"
  -f "$VALUES_FILE"
  -f "$TIKV_VALUES_FILE"
  --set "image.all.repository=${MILVUS_IMAGE_REPOSITORY}"
  --set "image.all.tag=${MILVUS_IMAGE_TAG}"
)

if [[ -n "$WOODPECKER_IMAGE_TAG" ]]; then
  helm_args+=(
    --set "woodpecker.image.repository=${WOODPECKER_IMAGE_REPOSITORY}"
    --set "woodpecker.image.tag=${WOODPECKER_IMAGE_TAG}"
  )
fi

helm "${helm_args[@]}"
