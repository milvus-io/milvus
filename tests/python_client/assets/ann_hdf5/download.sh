#!/bin/bash
# refer to https://github.com/yahoojapan/gongt/blob/master/assets/bench/download.sh

function check () {
    if [ ! -e $1 ]; then
        curl -LO $2
    fi
    # md5sum -c $1.md5
}

# check fashion-mnist-784-euclidean.hdf5 http://vectors.erikbern.com/fashion-mnist-784-euclidean.hdf5
# check glove-25-angular.hdf5 http://vectors.erikbern.com/glove-25-angular.hdf5
# check glove-50-angular.hdf5 http://vectors.erikbern.com/glove-50-angular.hdf5
# check glove-100-angular.hdf5 http://vectors.erikbern.com/glove-100-angular.hdf5
# check glove-200-angular.hdf5 http://vectors.erikbern.com/glove-200-angular.hdf5
# check mnist-784-euclidean.hdf5 http://vectors.erikbern.com/mnist-784-euclidean.hdf5
# check nytimes-256-angular.hdf5 http://vectors.erikbern.com/nytimes-256-angular.hdf5
check sift-128-euclidean.hdf5 http://vectors.erikbern.com/sift-128-euclidean.hdf5