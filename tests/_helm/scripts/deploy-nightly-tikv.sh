#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

RELEASE_NAME="${RELEASE_NAME:-}"
ACTION="${ACTION:-deploy}"
DRY_RUN="${DRY_RUN:-0}"
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
TIKV_STORAGE_CLASS="${TIKV_STORAGE_CLASS:-}"
TIKV_STORAGE_SIZE="${TIKV_STORAGE_SIZE:-10Gi}"
TIKV_WAIT_TIMEOUT="${TIKV_WAIT_TIMEOUT:-300s}"
HELM_TIMEOUT="${HELM_TIMEOUT:-1200s}"

usage() {
  cat <<EOF
Usage:
  RELEASE_NAME=<name> MILVUS_IMAGE_TAG=<tag> [WOODPECKER_IMAGE_TAG=<tag>] $0
  ACTION=cleanup RELEASE_NAME=<name> $0

Teardown order:
  helm uninstall <release> -n <namespace>
  NAMESPACE=<namespace> ACTION=cleanup RELEASE_NAME=<name> $0

Optional environment variables:
  ACTION                     deploy or cleanup (default: deploy)
  DRY_RUN                    print manifests/cleanup commands and exit when set to 1
  NAMESPACE                  default: jenkins-milvus-ci
  VALUES_FILE                default: tests/_helm/values/nightly/distributed-woodpecker
  CHART_VERSION              default: 5.0.16
  PD_IMAGE                   default: pingcap/pd:v8.5.3
  TIKV_IMAGE                 default: pingcap/tikv:v8.5.3
  TIKV_STORAGE_CLASS         default: cluster default storage class
  TIKV_STORAGE_SIZE          default: 10Gi
  TIKV_WAIT_TIMEOUT          default: 300s
  HELM_TIMEOUT               default: 1200s
EOF
}

duration_to_seconds() {
  local remaining="$1"
  local total=0

  if [[ "$remaining" =~ ^[0-9]+$ ]]; then
    echo "$remaining"
    return 0
  fi

  while [[ -n "$remaining" ]]; do
    if [[ "$remaining" =~ ^([0-9]+)h(.*)$ ]]; then
      total=$((total + BASH_REMATCH[1] * 3600))
      remaining="${BASH_REMATCH[2]}"
    elif [[ "$remaining" =~ ^([0-9]+)m(.*)$ ]]; then
      total=$((total + BASH_REMATCH[1] * 60))
      remaining="${BASH_REMATCH[2]}"
    elif [[ "$remaining" =~ ^([0-9]+)s(.*)$ ]]; then
      total=$((total + BASH_REMATCH[1]))
      remaining="${BASH_REMATCH[2]}"
    else
      return 1
    fi
  done

  if ((total == 0)); then
    return 1
  fi

  echo "$total"
}

render_tikv_manifests() {
  local storage_class_line=""

  if [[ -n "$TIKV_STORAGE_CLASS" ]]; then
    storage_class_line="      storageClassName: ${TIKV_STORAGE_CLASS}"
  fi

  cat <<EOF
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
        readinessProbe:
          httpGet:
            path: /health
            port: 2379
          initialDelaySeconds: 5
          periodSeconds: 5
        startupProbe:
          httpGet:
            path: /health
            port: 2379
          periodSeconds: 2
          failureThreshold: 60
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
  volumeClaimTemplates:
  - metadata:
      name: data
      labels:
        app.kubernetes.io/name: tikv
        app.kubernetes.io/instance: ${RELEASE_NAME}
        app.kubernetes.io/component: pd
    spec:
      accessModes: ["ReadWriteOnce"]
${storage_class_line}
      resources:
        requests:
          storage: ${TIKV_STORAGE_SIZE}
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
        readinessProbe:
          httpGet:
            path: /status
            port: 20180
          initialDelaySeconds: 5
          periodSeconds: 5
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
  volumeClaimTemplates:
  - metadata:
      name: data
      labels:
        app.kubernetes.io/name: tikv
        app.kubernetes.io/instance: ${RELEASE_NAME}
        app.kubernetes.io/component: tikv
    spec:
      accessModes: ["ReadWriteOnce"]
${storage_class_line}
      resources:
        requests:
          storage: ${TIKV_STORAGE_SIZE}
EOF
}

cleanup_tikv_resources() {
  local selector="app.kubernetes.io/name=tikv,app.kubernetes.io/instance=${RELEASE_NAME}"

  if [[ "$DRY_RUN" == "1" ]]; then
    cat <<EOF
kubectl delete statefulset,service -n ${NAMESPACE} -l ${selector} --ignore-not-found
kubectl delete pvc -n ${NAMESPACE} -l ${selector} --ignore-not-found
EOF
    return
  fi

  kubectl delete statefulset,service -n "$NAMESPACE" \
    -l "$selector" \
    --ignore-not-found
  kubectl delete pvc -n "$NAMESPACE" \
    -l "$selector" \
    --ignore-not-found
}

wait_for_tikv_store_up() {
  local timeout_seconds
  local deadline
  local last_output=""
  local poll_timeout
  local remaining_seconds
  local sleep_seconds

  if ! timeout_seconds="$(duration_to_seconds "$TIKV_WAIT_TIMEOUT")"; then
    echo "unsupported TIKV_WAIT_TIMEOUT: $TIKV_WAIT_TIMEOUT" >&2
    return 2
  fi

  if ! command -v timeout >/dev/null 2>&1; then
    echo "timeout command not found; cannot bound pd-ctl health checks" >&2
    return 1
  fi

  deadline=$((SECONDS + timeout_seconds))
  echo "Waiting for PD to report a TiKV store in Up state..."

  while ((SECONDS < deadline)); do
    remaining_seconds=$((deadline - SECONDS))
    poll_timeout="$remaining_seconds"
    if ((poll_timeout > 10)); then
      poll_timeout=10
    fi

    if last_output="$(timeout "${poll_timeout}s" kubectl --request-timeout="${poll_timeout}s" \
      exec -n "$NAMESPACE" "${RELEASE_NAME}-tikv-pd-0" -- \
      /pd-ctl -u http://127.0.0.1:2379 store 2>&1)"; then
      if grep -Eq '"state_name"[[:space:]]*:[[:space:]]*"Up"' <<<"$last_output"; then
        echo "PD reports a TiKV store in Up state."
        return 0
      fi
    elif grep -Eqi '(/pd-ctl|pd-ctl).*(not found|no such file|executable file not found)|stat /pd-ctl' <<<"$last_output"; then
      echo "pd-ctl is not available in ${RELEASE_NAME}-tikv-pd-0:" >&2
      echo "$last_output" >&2
      return 1
    fi

    remaining_seconds=$((deadline - SECONDS))
    sleep_seconds="$remaining_seconds"
    if ((sleep_seconds > 5)); then
      sleep_seconds=5
    fi
    if ((sleep_seconds > 0)); then
      sleep "$sleep_seconds"
    fi
  done

  echo "timed out after $TIKV_WAIT_TIMEOUT waiting for a TiKV store in Up state" >&2
  if [[ -n "$last_output" ]]; then
    echo "last pd-ctl output:" >&2
    echo "$last_output" >&2
  fi
  return 1
}

case "$ACTION" in
  deploy | cleanup)
    ;;
  *)
    echo "unsupported ACTION: $ACTION" >&2
    usage >&2
    exit 2
    ;;
esac

if [[ -z "$RELEASE_NAME" ]]; then
  usage >&2
  exit 2
fi

cd "$ROOT_DIR"

PD_SERVICE="${RELEASE_NAME}-tikv-pd"
PD_HEADLESS="${RELEASE_NAME}-tikv-pd-peer"
TIKV_HEADLESS="${RELEASE_NAME}-tikv-peer"

if [[ "$ACTION" == "cleanup" ]]; then
  cleanup_tikv_resources
  exit 0
fi

if [[ "$DRY_RUN" == "1" ]]; then
  render_tikv_manifests
  exit 0
fi

if [[ -z "$MILVUS_IMAGE_TAG" ]]; then
  usage >&2
  exit 2
fi

if [[ ! -f "$VALUES_FILE" ]]; then
  echo "values file not found: $VALUES_FILE" >&2
  exit 2
fi

TIKV_VALUES_FILE="tests/_helm/values/nightly/tikv"
if [[ ! -f "$TIKV_VALUES_FILE" ]]; then
  echo "tikv overlay values file not found: $TIKV_VALUES_FILE" >&2
  exit 2
fi

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
render_tikv_manifests | kubectl apply -n "$NAMESPACE" -f -

kubectl rollout status "statefulset/${RELEASE_NAME}-tikv-pd" -n "$NAMESPACE" --timeout="$TIKV_WAIT_TIMEOUT"
kubectl rollout status "statefulset/${RELEASE_NAME}-tikv" -n "$NAMESPACE" --timeout="$TIKV_WAIT_TIMEOUT"
wait_for_tikv_store_up

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
