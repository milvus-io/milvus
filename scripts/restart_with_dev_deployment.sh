#!/bin/bash

# Parse params
USE_GDB=false
MODE=""

for arg in "$@"; do
    if [ "$arg" == "--gdb" ]; then
        USE_GDB=true
    elif [ -z "$MODE" ]; then
        MODE=$arg
    fi
done

if [ -z "$MODE" ]; then
    MODE="standalone"
else
    if [ "$MODE" != "standalone" ] && [ "$MODE" != "cluster" ]; then
        echo "invalid mode: $MODE"
        echo "usage : bash restart_with_dev_deployment.sh [mode] [--gdb]. mode: standalone / cluster. default is standalone"
        exit 1
    fi
fi

# Validate --gdb flag
if [ "$USE_GDB" == "true" ] && [ "$MODE" != "standalone" ]; then
    echo "Error: --gdb flag is only supported in standalone mode"
    exit 1
fi


script_path=$(dirname "$0")
docker_compose_path=$script_path/../deployments/docker/dev

$script_path/stop.sh
docker compose -f $docker_compose_path/docker-compose-redis.yml down -v
sudo rm -rf $docker_compose_path/volumes/*
sudo rm -rf /tmp/milvus/*
sudo rm -rf /var/lib/milvus/*

docker compose -f $docker_compose_path/docker-compose-redis.yml up -d
sleep 5

if [ "$MODE" == "cluster" ]; then
    $script_path/start_cluster.sh
else
    if [ "$USE_GDB" == "true" ]; then
        $script_path/start_standalone.sh --gdb
    else
        $script_path/start_standalone.sh
    fi
fi


