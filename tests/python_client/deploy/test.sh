#!/bin/bash
set -e
# set -x

func() {
    echo "Usage:"
    echo "test.sh [-t Task] [-m Mode] [-r Release] [-p Password]"
    echo "Description"
    echo "Task, the task type of test. reinstall or upgrade"
    echo "Mode, the mode of milvus deploy. standalone or cluster"
    echo "Release, the release of milvus. e.g. 2.0.0-rc5"
    echo "Password, the password of root"
    exit -1
}


echo "check os env"
platform='unknown'
unamestr=`uname`
if [[ "$unamestr" == 'Linux' ]]; then
   platform='Linux'
elif [[ "$unamestr" == 'Darwin' ]]; then
   platform='Mac'
fi
echo "platform: $platform"



Task="reinstall"
Mode="standalone"
Release="2.0.0-rc5"
while getopts "hm:t:p:" OPT;
do
    case $OPT in
	m) Mode="$OPTARG";;
    t) Task="$OPTARG";;
	p) Password="$OPTARG";;
	h) func;;
	?) func;;
    esac
done

ROOT_FOLDER=$(cd "$(dirname "$0")";pwd)

# to export docker-compose logs before exit
function error_exit {
    pushd ${ROOT_FOLDER}/${Deploy_Dir}
    echo "test failed"
    current=`date "+%Y-%m-%d-%H-%M-%S"`
    if [ ! -d logs  ];
    then
        mkdir logs
    fi
    docker-compose logs > ./logs/${Deploy_Dir}-${Task}-${current}.log
    echo "log saved to `pwd`/logs/${Deploy_Dir}-${Task}-${current}.log"
    popd
    exit 1
}

function replace_image_tag {
    image_tag=$1
    if [ "$platform" == "Mac" ];
    then
        # for mac os
        sed -i "" "s/milvusdb\/milvus.*/milvusdb\/milvus-dev\:${image_tag}/g" docker-compose.yml   
    else
        #for linux os 
        sed -i "s/milvusdb\/milvus.*/milvusdb\/milvus-dev\:${image_tag}/g" docker-compose.yml
    fi

}
       

#to check containers all running and minio is healthy
function check_healthy {
    cnt=`docker-compose ps | grep -E "running|Running|Up|up" | wc -l`
    healthy=`docker-compose ps | grep "Healthy" | wc -l`
    time_cnt=0
    echo "running num $cnt expect num $Expect"
    echo "healthy num $healthy expect num $Expect_health"
    while [[ $cnt -ne $Expect || $healthy -ne 1 ]];
    do
    printf "waiting all containers get running\n"
    sleep 5s
    let time_cnt+=5
    # if time is greater than 300s, the condition still not satisfied, we regard it as a failure
    if [ $time_cnt -gt 300 ];
    then
        printf "timeout,there are some issue with deployment!"
        error_exit
    fi
    cnt=`docker-compose ps | grep -E "running|Running|Up|up" | wc -l`
    healthy=`docker-compose ps | grep "healthy" | wc -l`
    echo "running num $cnt expect num $Expect"
    echo "healthy num $healthy expect num $Expect_health"
    done
}

Deploy_Dir=$Mode
Task=$Task
Release=$Release
pw=$Password

echo "mode: $Mode"
echo "task: $Task"
echo "password: $pw"
## if needed, install dependency 
#echo "install dependency"
#pip install -r scripts/requirements.txt

if [ ! -d ${Deploy_Dir}  ];
then
    mkdir ${Deploy_Dir}
fi

echo "get tag info"

python scripts/get_tag.py

latest_tag=`jq -r ".latest_tag" tag_info.json`
latest_rc_tag=`jq -r ".latest_rc_tag" tag_info.json`
release_version=`jq -r ".release_version" tag_info.json`
echo $release_version

pushd ${Deploy_Dir}
# download docker-compose.yml
wget https://github.com/milvus-io/milvus/releases/download/${release_version}/milvus-${Deploy_Dir}-docker-compose.yml -O docker-compose.yml
ls
# clean env to deploy a fresh milvus
docker-compose down
docker-compose ps
echo "$pw"| sudo -S rm -rf ./volumes

# first deployment
if [ "$Task" == "reinstall" ];
then
    printf "start to deploy latest rc tag milvus\n"
    replace_image_tag $latest_tag

fi
if [ "$Task" == "upgrade" ];
then
    printf "start to deploy previous rc tag milvus\n"
    replace_image_tag $latest_rc_tag

fi
cat docker-compose.yml|grep milvusdb
Expect=`grep "container_name" docker-compose.yml | wc -l`
Expect_health=`grep "healthcheck" docker-compose.yml | wc -l`
docker-compose up -d
check_healthy
docker-compose ps
popd

# test for first deployment
printf "test for first deployment\n"
if [ "$Task" == "reinstall" ];
then
    python scripts/action_reinstall.py || error_exit
fi
if [ "$Task" == "upgrade" ];
then
    python scripts/action_before_upgrade.py || error_exit
fi

pushd ${Deploy_Dir}
# uninstall milvus
printf "start to uninstall milvus\n"
docker-compose down
sleep 10s
printf "check all containers removed\n"
docker-compose ps

# second deployment
if [ "$Task" == "reinstall" ];
then
    printf "start to reinstall milvus\n"
    #because the task is reinstall, so don't change images tag
fi
if [ "$Task" == "upgrade" ];
then
    printf "start to upgrade milvus\n"
    # because the task is upgrade, so replace image tag to latest, like rc4-->rc5
    replace_image_tag $latest_tag

fi
cat docker-compose.yml|grep milvusdb
docker-compose up -d
check_healthy
# sleep 60s # Todo use `curl http://localhost:9091/healthz` to check health
docker-compose ps
popd

# test for second deployment
printf "test for second deployment\n"
if [ "$Task" == "reinstall" ];
then
    python scripts/action_reinstall.py || error_exit
fi
if [ "$Task" == "upgrade" ];
then
    python scripts/action_after_upgrade.py || error_exit
fi

pushd ${Deploy_Dir}
# clean env
docker-compose ps
docker-compose down
sleep 10s
docker-compose ps
echo "$pw"|sudo -S rm -rf ./volumes
popd
