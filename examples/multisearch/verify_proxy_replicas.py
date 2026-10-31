"""Call MultiSearch on each Kubernetes Proxy and attest the running binary."""
import json
import os
import socket
import subprocess
import time
from pathlib import Path

from pymilvus.client.prepare import Prepare
from client import MultiSearchClient

root = Path(__file__).resolve().parents[2]
evidence = root / 'artifacts/multisearch'
env = dict(os.environ, KUBECONFIG=str(root / '.cache/multisearch/kubeconfig'))
kubectl = ['kubectl', '--context', 'multisearch-prototype', '-n', 'multisearch-prototype']
pods = json.loads(subprocess.check_output(kubectl + ['get', 'pods', '-l', 'app=proxy', '-o', 'json'], env=env))
fixture = json.loads((evidence/'k8s-results.json').read_text())
assert fixture['all_passed']
expected_sha = json.loads((evidence/'build-manifest.json').read_text())['binary_sha256']
items = []
for i in range(50):
    s = fixture['collections'][i % len(fixture['collections'])]
    req = Prepare.search_requests_with_expr(collection_name=s['name'], data=[s['query']],
        anns_field=s['vector_field'], param={'metric_type':s['metric'], 'params':{}},
        limit=1+i%9, output_fields=[s['scalar_field']],
        use_default_consistency=False, consistency_level=0)
    items.append((str(i), req))
results = []
for i,pod in enumerate(pods['items']):
    name = pod['metadata']['name']
    binary_sha = subprocess.check_output(kubectl + ['exec', name, '--', 'sha256sum', '/milvus/bin/milvus'], env=env, text=True).split()[0]
    assert binary_sha == expected_sha
    port = 29561 + i
    with (evidence/'logs'/f'port-forward-{name}.log').open('w') as log:
        process = subprocess.Popen(kubectl + ['port-forward', f'pod/{name}', f'{port}:19530', '--address=127.0.0.1'], env=env, stdout=log, stderr=subprocess.STDOUT)
        client = None
        try:
            deadline = time.monotonic() + 15
            while True:
                assert process.poll() is None, 'port-forward exited'
                try:
                    with socket.create_connection(('127.0.0.1',port), timeout=0.2):
                        break
                except OSError:
                    assert time.monotonic() < deadline, 'port-forward did not become ready'
                    time.sleep(0.2)
            client = MultiSearchClient(f'localhost:{port}')
            response = client.multi_search(items)
            assert response.status.code == 0 and len(response.items) == 50
            for outcome in response.items:
                assert outcome.grpc_code == 0 and outcome.HasField('result')
                assert outcome.result.status.code == 0 and outcome.result.status.error_code == 0
            assert sum(client.calls.counts.values()) == 1
            result = dict(pod=name, binary_sha256=binary_sha, items=50, rpc_count=1, passed=True,
                          image_id=pod['status']['containerStatuses'][0]['imageID'])
            results.append(result)
            print(json.dumps(result), flush=True)
        finally:
            if client: client.close()
            process.terminate()
            process.wait(timeout=10)
assert len(results) == 2, 'expected two Proxy replicas'
(evidence/'k8s-proxy-results.json').write_text(json.dumps(results, indent=2)+'\n')
