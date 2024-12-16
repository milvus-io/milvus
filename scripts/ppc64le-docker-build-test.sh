#!/bin/bash
set -ex

# Setup cleanup
remove_docker_container() { docker rm -f milvus-build || true; docker system prune -f --all; }
trap remove_docker_container EXIT
remove_docker_container

docker run --network host --privileged -t -d --name milvus-build registry.access.redhat.com/ubi9/ubi:9.3
docker cp ./. milvus-build:/
docker exec -it milvus-build bash -c "chmod 777 scripts/ppc64le-build-ubi9.3.sh && ./scripts/ppc64le-build-ubi9.3.sh"
