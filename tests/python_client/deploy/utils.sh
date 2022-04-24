#!/bin/bash

function replace_image_tag {
    image_repo=$1
    image_tag=$2
    image_repo=${image_repo//\//\\\/}
    platform='unknown'
    unamestr=$(uname)
    if [[ "$unamestr" == 'Linux' ]]; then
    platform='Linux'
    elif [[ "$unamestr" == 'Darwin' ]]; then
    platform='Mac'
    fi
    echo "before replace: "
    cat docker-compose.yml | grep milvusdb
    if [[ "$platform" == "Mac" ]];
    then
        # for mac os
        echo "replace image tag for mac start"
        sed -i "" "s/milvusdb.*/${image_repo}\:${image_tag}/g" docker-compose.yml   
        echo "replace image tag for mac done"
    else
        #for linux os 
        sed -i "s/milvusdb.*/${image_repo}\:${image_tag}/g" docker-compose.yml
    fi
    echo "after replace: "
    cat docker-compose.yml | grep milvusdb

}

#to check containers all running and minio is healthy
function check_healthy {
    Expect=$(grep "container_name" docker-compose.yml | wc -l)
    Expect_health=$(grep "healthcheck" docker-compose.yml | wc -l)
    cnt=$(docker-compose ps | grep -E "running|Running|Up|up" | wc -l)
    healthy=$(docker-compose ps | grep "healthy" | wc -l)
    time_cnt=0
    echo "running num $cnt expect num $Expect"
    echo "healthy num $healthy expect num $Expect_health"
    while [[ $cnt -ne $Expect || $healthy -ne 1 ]];
    do
    printf "waiting all containers getting running\n"
    sleep 5
    let time_cnt+=5
    # if time is greater than 300s, the condition still not satisfied, we regard it as a failure
    if [ $time_cnt -gt 300 ];
    then
        printf "timeout,there are some issues with deployment!"
        exit 1
    fi
    cnt=$(docker-compose ps | grep -E "running|Running|Up|up" | wc -l)
    healthy=$(docker-compose ps | grep "healthy" | wc -l)
    echo "running num $cnt expect num $Expect"
    echo "healthy num $healthy expect num $Expect_health"
    done
} 