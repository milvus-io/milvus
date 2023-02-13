#!/bin/bash

# var
readonly STRATEGY='RollingUpdate'

# default option
NAMESPACE="default"
IMAGE_TAG="milvusdb/milvus:v2.2.3"
OPERATION="update"

function env_check() {
    # check kubectl whether exist
    which kubectl &>/dev/null
    if [[ $? != 0 ]]; then
        echo "Require kubectl"
        exit 1
    fi
}

function option_check() {
    if [[ ! $INSTANCE ]]; then # None
        echo "option [-i instance] required" && exit 1
    elif [[ ! $OPERATION ]]; then
        echo "option [-o update] required " && exit 1
    elif [[ $OPERATION == 'update' ]]; then
        if [[ ! $TARGET_VERSION || ! $IMAGE_TAG ]]; then
            echo "option [-t target_version] & [-w image_tag] are required when update" && exit 1
        fi
    else
        Usage && exit 1
    fi
}

function check_instance() {
    DEPLOYMENTS=$(kubectl get deploy -n $NAMESPACE -l app.kubernetes.io/instance=$INSTANCE,app.kubernetes.io/name=milvus --output=jsonpath={.items..metadata.name})
    if [[ ! $DEPLOYMENTS ]]; then
        echo "There is no instance [$INSTANCE] " && exit 1
    fi
}

function check_version() {
    # not in use
    local version_num=$(kubectl get deploy -n $NAMESPACE -l app.kubernetes.io/instance=$INSTANCE,app.kubernetes.io/name=milvus --output=jsonpath='{.items..metadata.labels.app\.kubernetes\.io/version}'|sed 's/ /\n/g'|sort -u|wc -l|awk '{print $1}')
    if [[ $version_num > 1 ]];then
        echo "[WARN]: there are [$version_num] versions(version labels) of instance [$INSTANCE] running now "
    fi
}

function pre_check() {
    env_check && option_check && check_instance
}

function store_metadata() {
    # not in use now

    # if we want to support strong update and rollback,we must keep some state message
    # by kubectl
    true
}

function deploy_update() {
    local deployment=$1
    local now_deploy=$(kubectl -n $NAMESPACE get deployments.apps $deployment -o jsonpath={.spec.strategy.type},{.spec.template.spec.containers..image},{.spec.template.spec.containers[0].name})
    local name=$(echo $now_deploy|awk -F ',' '{print $3}')
    echo "    update $deployment "
    echo $deployment|grep coord >/dev/null && local wait_time='"minReadySeconds":30,' || local wait_time=''
    kubectl -n $NAMESPACE patch deployments.apps $deployment -p '{"metadata":{"labels":{"app.kubernetes.io/version":"'$TARGET_VERSION'"}},"spec":{'$wait_time'"strategy":{"type":"'$STRATEGY'"},"template":{"spec":{"containers":[{"image":"'$IMAGE_TAG'","name":"'$name'"}]}}}}'
    # echo "check $deployment status..."
    check_deploy_status true $deployment
    if [[ $? != 0 ]];then
        echo "error: $(check_deploy_status true $deployment) "
        exit 1
    fi
}

function set_rollingupdate_mode() {
    local configmaps_name=${INSTANCE}-milvus
    local mq=$(kubectl -n $NAMESPACE get configmaps $configmaps_name -o jsonpath='{.data}'|egrep -o 'messageQueue: \w*' --color|tail -1|awk '{print $2}')
    if [[ $mq == "rocksmq" ]];then
        echo "standalone Milvus with mq:rocksmq don't support rolling update now" && exit 1
    fi
    local add_user_yaml='rootCoord:\\n  enableActiveStandby: true\\nqueryCoord:\\n  enableActiveStandby: true\\ndataCoord:\\n  enableActiveStandby: true\\nindexCoord:\\n  enableActiveStandby: true'
    local check_standby=$(kubectl -n $NAMESPACE get configmaps $configmaps_name -o json|grep '^\ *"user\.yaml":'|grep "$add_user_yaml")
    # echo $check_standby
    if [[ ! ${check_standby} ]];then
        kubectl -n $NAMESPACE get configmaps $configmaps_name -o json|sed 's/\(\"user.yaml.*\)"$/\1\\n'"$add_user_yaml"'"/'|kubectl -n $NAMESPACE apply -f -
    fi
}

function prepare() {
    set_rollingupdate_mode
}

function check_deploy_status() {
    local watchOption=$1
    local deploy=$2
    kubectl -n $NAMESPACE rollout status deployment $deploy --watch=$watchOption | grep -i successful 1>/dev/null
    return $?
}

function check_rollout_status() {
    local deploy
    local watchOption=false
    if [[ $1 == true ]];then
        watchOption=$1
    fi
    for deploy in $DEPLOYMENTS; do
        check_deploy_status $watchOption $deploy 1>/dev/null
        if [[ $? != 0 ]];then
            echo "deployment [$deploy] was blocked,[$deploy] pods status:"
            kubectl -n $NAMESPACE get pod|grep $deploy
            echo "maybe it's being deployed or something wrong,need to be fixed first"
            exit 1
        fi
    done
    echo 'check:
    success: all deployments are successful, there are no deployments blocked'
}

function match_deploy() {
    # if TARGET_VERSION in
    local now_version_topology=($(echo $DEPLOYMENTS|sed s'/ /\n/'g|grep 'rootcoord$') $(echo $DEPLOYMENTS|sed s'/ /\n/'g|grep 'datacoord$') $(echo $DEPLOYMENTS|sed s'/ /\n/'g|grep 'indexcoord$') $(echo $DEPLOYMENTS|sed s'/ /\n/'g|grep 'querycoord$') $(echo $DEPLOYMENTS|sed s'/ /\n/'g|grep 'indexnode$') $(echo $DEPLOYMENTS|sed s'/ /\n/'g|grep 'datanode$'),$(echo $DEPLOYMENTS|sed s'/ /\n/'g|grep 'querynode$') $(echo $DEPLOYMENTS|sed s'/ /\n/'g|grep 'proxy$') $(echo $DEPLOYMENTS|sed s'/ /\n/'g|grep 'standalone$'))
    TOPOLOGY=${now_version_topology[@]}
}

function do_rollout() {
    echo 'update:'
    local deploy
    match_deploy
    for deploy in ${TOPOLOGY[@]}; do
        for i in $(echo $deploy|sed 's/,/\n/g');do # care of None
            deploy_update $i
            if [[ $? != 0 ]];then
                exit 1
            fi
        done
    done
}

function do_rollback() {
    # not in use now
    kubectl -n $NAMESPACE rollout history
}

function Usage() {
    echo "

    [Warning]: Based on kubectl
    -n [namespace]                                   Default:[default]
                                                     The namespace that Milvus is installed in.
    -i [instance name]                    [required] The name of milvus instance.
    -t [target_version]                   [required] The milvus target version.
    -w [image_tag]                        [required] The target milvus image tag.
    -o [operation]                                   Only support [update] now.

RollingUpdate Example:

    sh rollingUpdate.sh -n default -i my-release -o update -t 2.2.3 -w 'milvusdb/milvus:v2.2.3'
    "
}



function main() {
    pre_check && prepare && check_rollout_status && do_rollout && check_rollout_status true
    exit $?
}

# entrypoint
while getopts "n:i:t:w:o:" OPT; do
    case $OPT in
    n) NAMESPACE=$OPTARG ;;
    i) INSTANCE=$OPTARG ;;
    t) TARGET_VERSION=$OPTARG ;;
    w) IMAGE_TAG=$OPTARG ;;
    o) OPERATION=$OPTARG ;;
    *) echo "Unknown parameters" ;;
    esac
done
if [[ -z $1 ]]; then
    Usage && exit 1
fi
main