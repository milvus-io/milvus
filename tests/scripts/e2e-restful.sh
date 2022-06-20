#!/bin/bash

# Exit immediately for non zero status
set -e

# Print commands
set -x

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
ROOT="$( cd -P "$( dirname "$SOURCE" )/../.." && pwd )"

DATA_PATH="${ROOT}/tests/scripts/restful-data/"

MILVUS_CLUSTER_ENABLED="${MILVUS_CLUSTER_ENABLED:-false}"

# TODO: use service instead of podIP when milvus-helm supports
if [[ "${MILVUS_CLUSTER_ENABLED}" == "false" ]]; then
    MILVUS_SERVICE_NAME=$(kubectl -n ${MILVUS_HELM_NAMESPACE} get pods -l app.kubernetes.io/name=milvus -l component=standalone -l app.kubernetes.io/instance=${MILVUS_HELM_RELEASE_NAME} -o=jsonpath='{.items[0].status.podIP}')
else
    MILVUS_SERVICE_NAME=$(kubectl -n ${MILVUS_HELM_NAMESPACE} get pods -l app.kubernetes.io/name=milvus -l component=proxy -l app.kubernetes.io/instance=${MILVUS_HELM_RELEASE_NAME} -o=jsonpath='{.items[0].status.podIP}')
fi

# Create a collection
curl -X 'POST' \
  "http://${MILVUS_SERVICE_NAME}:8080/api/v1/collection" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d @${DATA_PATH}/create-collection.json

# Has collection
curl -X 'GET' \
  "http://${MILVUS_SERVICE_NAME}:8080/api/v1/collection/existence" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book"
  }'

# Check collection details
curl -X 'GET' \
  "http://${MILVUS_SERVICE_NAME}:8080/api/v1/collection" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book"
  }'

# Load collection
curl -X 'POST' \
  "http://${MILVUS_SERVICE_NAME}:8080/api/v1/collection/load" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book"
  }'

### Data
# Insert Data
curl -X 'POST' \
  "http://${MILVUS_SERVICE_NAME}:8080/api/v1/entities" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d @${DATA_PATH}/insert-data.json

# Build Index
curl -X 'POST' \
  "http://${MILVUS_SERVICE_NAME}:8080/api/v1/index" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book",
    "field_name": "book_intro",
    "extra_params":[
      {"key": "metric_type", "value": "L2"},
      {"key": "index_type", "value": "IVF_FLAT"},
      {"key": "params", "value": "{\"nlist\":1024}"}
    ]
  }'

# KNN Search
curl -X 'POST' \
  "http://${MILVUS_SERVICE_NAME}:8080/api/v1/search" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d @${DATA_PATH}/search.json

# Drop Index
curl -X 'DELETE' \
  "http://${MILVUS_SERVICE_NAME}:8080/api/v1/index" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book",
    "field_name": "book_intro"
  }'

# Release collection
curl -X 'DELETE' \
  "http://${MILVUS_SERVICE_NAME}:8080/api/v1/collection/load" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book"
  }'

# Drop collection
curl -X 'DELETE' \
  "http://${MILVUS_SERVICE_NAME}:8080/api/v1/collection" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book"
  }'
