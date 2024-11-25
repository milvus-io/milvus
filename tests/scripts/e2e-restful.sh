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

MILVUS_SERVICE_NAME=$(echo "${MILVUS_HELM_RELEASE_NAME}-milvus.${MILVUS_HELM_NAMESPACE}" | tr -d '\n')

MILVUS_SERVICE_ADDRESS="${MILVUS_SERVICE_NAME}:9091"

# Create a collection
if curl -X 'POST' \
  "http://${MILVUS_SERVICE_ADDRESS}/api/v1/collection" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d @${DATA_PATH}/create-collection.json | grep -q "error_code" ; then
  exit 1
fi

# Has collection
if curl -X 'GET' \
  "http://${MILVUS_SERVICE_ADDRESS}/api/v1/collection/existence" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book"
  }' | grep -q "error_code" ; then
  exit 1
fi

# Check collection details
if curl -X 'GET' \
  "http://${MILVUS_SERVICE_ADDRESS}/api/v1/collection" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book"
  }' | grep -q "error_code" ; then
  exit 1
fi


### Data
# Insert Data
if curl -X 'POST' \
  "http://${MILVUS_SERVICE_ADDRESS}/api/v1/entities" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d @${DATA_PATH}/insert-data.json | grep -q "error_code" ; then
  exit 1
fi

# Build Index for book_intro
if curl -X 'POST' \
  "http://${MILVUS_SERVICE_ADDRESS}/api/v1/index" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book",
    "field_name": "book_intro",
    "index_name": "book_intro_index",
    "extra_params":[
      {"key": "metric_type", "value": "L2"},
      {"key": "index_type", "value": "IVF_FLAT"},
      {"key": "params", "value": "{\"nlist\":1024}"}
    ]
  }' | grep -q "error_code" ; then
  exit 1
fi

# Build Index for author_intro
if curl -X 'POST' \
  "http://${MILVUS_SERVICE_ADDRESS}/api/v1/index" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book",
    "field_name": "author_intro",
    "index_name": "author_intro_index",
    "extra_params":[
      {"key": "metric_type", "value": "L2"},
      {"key": "index_type", "value": "IVF_FLAT"},
      {"key": "params", "value": "{\"nlist\":1024}"}
    ]
  }' | grep -q "error_code" ; then
  exit 1
fi

# Build Index for comment
if curl -X 'POST' \
  "http://${MILVUS_SERVICE_ADDRESS}/api/v1/index" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book",
    "field_name": "comment",
    "index_name": "comment_index",
    "extra_params":[
      {"key": "metric_type", "value": "L2"},
      {"key": "index_type", "value": "IVF_FLAT"},
      {"key": "params", "value": "{\"nlist\":1024}"}
    ]
  }' | grep -q "error_code" ; then
  exit 1
fi

# Load collection
if curl -X 'POST' \
  "http://${MILVUS_SERVICE_ADDRESS}/api/v1/collection/load" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book"
  }' | grep -q "error_code" ; then
  exit 1
fi

# KNN Search
# TODO: search fp16/bf16
for SEARCH_JSON in search-book-intro ; do
if curl -X 'POST' \
  "http://${MILVUS_SERVICE_ADDRESS}/api/v1/search" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d @${DATA_PATH}/${SEARCH_JSON}.json | grep -q "error_code" ; then
  exit 1
fi
done

# Release collection
if curl -X 'DELETE' \
  "http://${MILVUS_SERVICE_ADDRESS}/api/v1/collection/load" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book"
  }' | grep -q "error_code" ; then
  exit 1
fi

# Drop Index
for FIELD_NAME in book_intro author_intro search_comment ; do
if curl -X 'DELETE' \
  "http://${MILVUS_SERVICE_ADDRESS}/api/v1/index" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d "{
    \"collection_name\": \"book\",
    \"field_name\": \"${FIELD_NAME}\",
    \"index_name\": \"${FIELD_NAME}_index\"
  }" | grep -q "error_code" ; then
  exit 1
fi
done

# Drop collection
if curl -X 'DELETE' \
  "http://${MILVUS_SERVICE_ADDRESS}/api/v1/collection" \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
    "collection_name": "book"
  }' | grep -q "error_code" ; then
  exit 1
fi

echo "e2e-restful.sh success!"
