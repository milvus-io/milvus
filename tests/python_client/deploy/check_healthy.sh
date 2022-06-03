#!/bin/bash

#to check containers all running and minio is healthy
function check_healthy {
    Expect=$(yq '.services | length' 'docker-compose.yml')
    Expect_health=$(yq '.services' 'docker-compose.yml' |grep 'healthcheck'|wc -l)
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

check_healthy
