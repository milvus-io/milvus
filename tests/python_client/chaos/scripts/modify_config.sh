#!/bin/bash
set -e
set -x


echo "check os env"
platform='Linux'
unamestr=$(uname)
if [[ "$unamestr" == 'Linux' ]]; then
   platform='Linux'
elif [[ "$unamestr" == 'Darwin' ]]; then
   platform='Mac'
fi
echo "platform: $platform"

if [ "$platform" == "Mac" ];
then
    sed -i "" "s/TESTS_CONFIG_LOCATION =.*/TESTS_CONFIG_LOCATION = \'chaos_objects\/${CHAOS_TYPE/-/_}\/'/g" constants.py
    sed -i "" "s/ALL_CHAOS_YAMLS =.*/ALL_CHAOS_YAMLS = \'chaos_${POD_NAME}_${CHAOS_TYPE/-/_}.yaml\'/g" constants.py
    sed -i "" "s/RELEASE_NAME =.*/RELEASE_NAME = \'${RELEASE_NAME}\'/g" constants.py
else
    sed -i "s/TESTS_CONFIG_LOCATION =.*/TESTS_CONFIG_LOCATION = \'chaos_objects\/${CHAOS_TYPE/-/_}\/'/g" constants.py
    sed -i "s/ALL_CHAOS_YAMLS =.*/ALL_CHAOS_YAMLS = \'chaos_${POD_NAME}_${CHAOS_TYPE/-/_}.yaml\'/g" constants.py
    sed -i "s/RELEASE_NAME =.*/RELEASE_NAME = \'${RELEASE_NAME}\'/g" constants.py
fi