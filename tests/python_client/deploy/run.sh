#!/bin/bash
set -x


func() {
    echo "Usage:"
    echo "run.sh [-p Password]"
    echo "Password, the password of root"
    exit -1

}
while getopts "hp:" OPT;
do
    case $OPT in
	p) Password="$OPTARG";;
	h) func;;
	?) func;;
    esac
done
pw=$Password

# start test standalone reinstall
bash test.sh -m standalone -t reinstall -p $pw
# start test standalone upgrade
bash test.sh -m standalone -t upgrade -p $pw
# start test cluster reinstall
bash test.sh -m cluster -t reinstall -p $pw
# start test cluster upgrade
bash test.sh -m cluster -t pgrade -p $pw
