#!/bin/bash

# Exit immediately for non zero status
set -e

log_dir=${1:-"logs"}
array=($(docker-compose ps -a|awk 'NR == 1 {next} {print $1}'))
echo ${array[@]}
if [ ! -d $log_dir ];
then
    mkdir -p $log_dir
fi
echo "export logs start"
for container in ${array[*]}
do
echo "export logs for container $container "
docker logs $container > ./$log_dir/$container.log 2>&1 || echo "export logs for container $container failed"
done
echo "export logs done"
