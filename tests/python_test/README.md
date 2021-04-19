## Requirements
* python 3.6.8+
* pip install -r requirements.txt

## How to Build Test Env
```shell
sudo docker pull registry.zilliz.com/milvus/milvus-test:v0.2
sudo docker run -it -v /home/zilliz:/home/zilliz -d registry.zilliz.com/milvus/milvus-test:v0.2
```

## How to Create Test Env docker in k8s
```shell
# 1. start milvus k8s pod
cd milvus-helm/charts/milvus
helm install --wait --timeout 300s \
  --set image.repository=registry.zilliz.com/milvus/engine \
  --set persistence.enabled=true \
  --set image.tag=PR-3818-gpu-centos7-release \
  --set image.pullPolicy=Always \
  --set service.type=LoadBalancer \
  -f ci/db_backend/mysql_gpu_values.yaml \
  -f ci/filebeat/values.yaml \
  -f test.yaml \
  --namespace milvus \
  milvus-ci-pr-3818-1-single-centos7-gpu .

# 2. remove milvus k8s pod
helm uninstall -n milvus milvus-test

# 3. check k8s pod status
kubectl get svc -n milvus -w milvus-test

# 4. login to pod
kubectl get pods --namespace milvus
kubectl exec -it milvus-test-writable-6cc49cfcd4-rbrns -n milvus bash
```

## How to Run Test cases
```shell
# Test level-1 cases
pytest . --level=1 --ip=127.0.0.1 --port=19530

# Test level-1 cases in 'test_connect.py' only
pytest test_connect.py --level=1
```

## How to list test cases
```shell
# List all cases
pytest --dry-run -qq

# Collect all cases with docstring
pytest --collect-only -qq

# Create test report with allure
pytest --alluredir=test_out . -q -v
allure serve test_out
 ```

## Contribution getting started
* Follow PEP-8 for naming and black for formatting.

