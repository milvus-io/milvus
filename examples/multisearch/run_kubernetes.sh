#!/usr/bin/env bash
# Functional check on the explicitly selected local development cluster.
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${KUBECONFIG:?Set KUBECONFIG to the development cluster config}"
ms_context=${MULTISEARCH_KUBE_CONTEXT:-multisearch-prototype}
ms_node=${MULTISEARCH_MINIKUBE_CONTAINER:-multisearch-prototype}
ms_output=artifacts/multisearch-v2
mkdir -p "$ms_output"
# The development node is owned by this experiment; keep it off benchmark CPUs.
docker update --cpuset-cpus=96-107 "$ms_node" >/dev/null
docker save milvus-multisearch:v305 | docker exec -i "$ms_node" ctr -n k8s.io images import -
.venv/bin/python examples/multisearch/make_k8s_manifest.py
kubectl --context "$ms_context" apply -f examples/multisearch/kubernetes.json
kubectl --context "$ms_context" -n multisearch-v305 wait --for=condition=Available deployment --all --timeout=300s
kubectl --context "$ms_context" -n multisearch-v305 get pods -o json > "$ms_output/kubernetes-pods.json"
mapfile -t ms_proxies < <(kubectl --context "$ms_context" -n multisearch-v305 get pods -l app=proxy -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
[[ ${#ms_proxies[@]} == 2 ]]
ms_pf1=''; ms_pf2=''
trap '[[ -z "$ms_pf1" ]] || kill "$ms_pf1" 2>/dev/null || true; [[ -z "$ms_pf2" ]] || kill "$ms_pf2" 2>/dev/null || true' EXIT
kubectl --context "$ms_context" -n multisearch-v305 port-forward "pod/${ms_proxies[0]}" 29562:19530 > .cache/k8s-port-forward1.log 2>&1 & ms_pf1=$!
kubectl --context "$ms_context" -n multisearch-v305 port-forward "pod/${ms_proxies[1]}" 29563:19530 > .cache/k8s-port-forward2.log 2>&1 & ms_pf2=$!
.venv/bin/python - <<'PY'
import socket,time
for port in (29562,29563):
 for attempt in range(30):
  try:
   with socket.create_connection(('localhost',port),timeout=1):break
  except OSError:time.sleep(1)
 else:raise RuntimeError('port-forward did not become ready')
PY
.venv/bin/python examples/multisearch/seed.py --port 29562 --rows 256 --output "$ms_output/kubernetes-fixture.json"
.venv/bin/python examples/multisearch/verify.py --port 29562 --fixture "$ms_output/kubernetes-fixture.json" --output "$ms_output/kubernetes-functional.json"
.venv/bin/python examples/multisearch/verify.py --port 29563 --fixture "$ms_output/kubernetes-fixture.json" --output "$ms_output/kubernetes-functional-proxy2.json"
for ms_proxy in "${ms_proxies[@]}"; do
 kubectl --context "$ms_context" -n multisearch-v305 exec "$ms_proxy" -- sha256sum /milvus/bin/milvus
 done > "$ms_output/kubernetes-binary-sha256.txt"
kubectl --context "$ms_context" -n multisearch-v305 get pods -o wide
printf 'Kubernetes native API checks passed on both Proxy replicas.\n'
