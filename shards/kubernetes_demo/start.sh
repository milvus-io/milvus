#!/bin/bash

UL=`tput smul`
NOUL=`tput rmul`
BOLD=`tput bold`
NORMAL=`tput sgr0`
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
ENDC='\033[0m'

function showHelpMessage () {
    echo -e "${BOLD}Usage:${NORMAL} ${RED}$0${ENDC} [option...] {cleanup${GREEN}|${ENDC}baseup${GREEN}|${ENDC}appup${GREEN}|${ENDC}appdown${GREEN}|${ENDC}allup}" >&2
    echo
    echo "  -h, --help          show help message"
    echo "  ${BOLD}cleanup,            delete all resources${NORMAL}"
    echo "  ${BOLD}baseup,             start all required base resources${NORMAL}"
    echo "  ${BOLD}appup,              start all pods${NORMAL}"
    echo "  ${BOLD}appdown,            remove all pods${NORMAL}"
    echo "  ${BOLD}allup,              start all base resources and pods${NORMAL}"
    echo "  ${BOLD}scale-proxy,        scale proxy${NORMAL}"
    echo "  ${BOLD}scale-ro-server,    scale readonly servers${NORMAL}"
    echo "  ${BOLD}scale-worker,       scale calculation workers${NORMAL}"
}

function showscaleHelpMessage () {
    echo -e "${BOLD}Usage:${NORMAL} ${RED}$0 $1${ENDC} [option...] {1|2|3|4|...}" >&2
    echo
    echo "  -h, --help          show help message"
    echo "  ${BOLD}number,             (int) target scale number"
}

function PrintScaleSuccessMessage() {
    echo -e "${BLUE}${BOLD}Successfully Scaled: ${1} --> ${2}${ENDC}"
}

function PrintPodStatusMessage() {
    echo -e "${BOLD}${1}${NORMAL}"
}

timeout=60

function setUpMysql () {
    mysqlUserName=$(kubectl describe configmap -n mishards mishards-roserver-configmap |
                    grep backend_url |
                    awk '{print $2}' |
                    awk '{split($0, level1, ":");
                    split(level1[2], level2, "/");
                    print level2[3]}')
    mysqlPassword=$(kubectl describe configmap -n mishards mishards-roserver-configmap |
                    grep backend_url |
                    awk '{print $2}' |
                    awk '{split($0, level1, ":");
                    split(level1[3], level3, "@");
                    print level3[1]}')
    mysqlDBName=$(kubectl describe configmap -n mishards mishards-roserver-configmap |
                  grep backend_url |
                  awk '{print $2}' |
                  awk '{split($0, level1, ":");
                  split(level1[4], level4, "/");
                  print level4[2]}')
    mysqlContainer=$(kubectl get pods -n mishards | grep mishards-mysql | awk '{print $1}')

    kubectl exec -n mishards $mysqlContainer -- mysql -h mishards-mysql -u$mysqlUserName -p$mysqlPassword -e "CREATE DATABASE IF NOT EXISTS $mysqlDBName;"

    checkDBExists=$(kubectl exec -n mishards $mysqlContainer -- mysql -h mishards-mysql -u$mysqlUserName -p$mysqlPassword -e "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '$mysqlDBName';" | grep -o $mysqlDBName | wc -l)
    counter=0
    while [ $checkDBExists -lt 1 ]; do
        sleep 1
        let counter=counter+1
        if [ $counter == $timeout ]; then
            echo "Creating MySQL database $mysqlDBName timeout"
            return 1
        fi
        checkDBExists=$(kubectl exec -n mishards $mysqlContainer -- mysql -h mishards-mysql -u$mysqlUserName -p$mysqlPassword -e "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '$mysqlDBName';" | grep -o $mysqlDBName | wc -l)
    done;

    kubectl exec -n mishards $mysqlContainer -- mysql -h mishards-mysql -u$mysqlUserName -p$mysqlPassword -e "GRANT ALL PRIVILEGES ON $mysqlDBName.* TO '$mysqlUserName'@'%';"
    kubectl exec -n mishards $mysqlContainer -- mysql -h mishards-mysql -u$mysqlUserName -p$mysqlPassword -e "FLUSH PRIVILEGES;"
    checkGrant=$(kubectl exec -n mishards $mysqlContainer -- mysql -h mishards-mysql -u$mysqlUserName -p$mysqlPassword -e "SHOW GRANTS for $mysqlUserName;" | grep -o "GRANT ALL PRIVILEGES ON \`$mysqlDBName\`\.\*" | wc -l)
    counter=0
    while [ $checkGrant -lt 1 ]; do
        sleep 1
        let counter=counter+1
        if [ $counter == $timeout ]; then
            echo "Granting all privileges on $mysqlDBName to $mysqlUserName timeout"
            return 1
        fi
        checkGrant=$(kubectl exec -n mishards $mysqlContainer -- mysql -h mishards-mysql -u$mysqlUserName -p$mysqlPassword -e "SHOW GRANTS for $mysqlUserName;" | grep -o "GRANT ALL PRIVILEGES ON \`$mysqlDBName\`\.\*" | wc -l)
    done;
}

function checkStatefulSevers() {
    stateful_replicas=$(kubectl describe statefulset -n mishards mishards-ro-servers | grep "Replicas:" | awk '{print $3}')
    stateful_running_pods=$(kubectl describe statefulset -n mishards mishards-ro-servers | grep "Pods Status:" | awk '{print $3}')

    counter=0
    prev=$stateful_running_pods
    PrintPodStatusMessage "Running mishards-ro-servers Pods: $stateful_running_pods/$stateful_replicas"
    while [ $stateful_replicas != $stateful_running_pods ]; do
        echo -e "${YELLOW}Wait another 1 sec --- ${counter}${ENDC}"
        sleep 1;

        let counter=counter+1
        if [ $counter -eq $timeout ]; then
            return 1;
        fi

        stateful_running_pods=$(kubectl describe statefulset -n mishards mishards-ro-servers | grep "Pods Status:" | awk '{print $3}')
        if [ $stateful_running_pods -ne $prev ]; then
            PrintPodStatusMessage "Running mishards-ro-servers Pods: $stateful_running_pods/$stateful_replicas"
        fi
        prev=$stateful_running_pods
    done;
    return 0;
}

function checkDeployment() {
    deployment_name=$1
    replicas=$(kubectl describe deployment -n mishards $deployment_name | grep "Replicas:" | awk '{print $2}')
    running=$(kubectl get pods -n mishards | grep $deployment_name | grep Running | wc -l)

    counter=0
    prev=$running
    PrintPodStatusMessage "Running $deployment_name Pods: $running/$replicas"
    while [ $replicas != $running ]; do
        echo -e "${YELLOW}Wait another 1 sec --- ${counter}${ENDC}"
        sleep 1;

        let counter=counter+1
        if [ $counter == $timeout ]; then
            return 1
        fi

        running=$(kubectl get pods -n mishards | grep "$deployment_name" | grep Running | wc -l)
        if [ $running -ne $prev ]; then
            PrintPodStatusMessage "Running $deployment_name Pods: $running/$replicas"
        fi
        prev=$running
    done
}


function startDependencies() {
    kubectl apply -f mishards_data_pvc.yaml
    kubectl apply -f mishards_configmap.yaml
    kubectl apply -f mishards_auxiliary.yaml

    counter=0
    while [ $(kubectl get pvc -n mishards | grep Bound | wc -l) != 4 ]; do
        sleep 1;
        let counter=counter+1
        if [ $counter == $timeout ]; then
            echo "baseup timeout"
            return 1
        fi
    done
    checkDeployment "mishards-mysql"
}

function startApps() {
    counter=0
    errmsg=""
    echo -e "${GREEN}${BOLD}Checking required resouces...${NORMAL}${ENDC}"
    while [ $counter -lt $timeout ]; do
        sleep 1;
        if [ $(kubectl get pvc -n mishards 2>/dev/null | grep Bound | wc -l) != 4 ]; then
            echo -e "${YELLOW}No pvc. Wait another sec... $counter${ENDC}";
            errmsg='No pvc';
            let counter=counter+1;
            continue
        fi
        if [ $(kubectl get configmap -n mishards 2>/dev/null | grep mishards | wc -l) != 4 ]; then
            echo -e "${YELLOW}No configmap. Wait another sec... $counter${ENDC}";
            errmsg='No configmap';
            let counter=counter+1;
            continue
        fi
        if [ $(kubectl get ep -n mishards 2>/dev/null | grep mishards-mysql | awk '{print $2}') == "<none>" ]; then
            echo -e "${YELLOW}No mysql. Wait another sec... $counter${ENDC}";
            errmsg='No mysql';
            let counter=counter+1;
            continue
        fi
        # if [ $(kubectl get ep -n milvus 2>/dev/null | grep milvus-redis | awk '{print $2}') == "<none>" ]; then
        #     echo -e "${NORMAL}${YELLOW}No redis. Wait another sec... $counter${ENDC}";
        #     errmsg='No redis';
        #     let counter=counter+1;
        #     continue
        # fi
        break;
    done

    if [ $counter -ge $timeout ]; then
        echo -e "${RED}${BOLD}Start APP Error: $errmsg${NORMAL}${ENDC}"
        exit 1;
    fi

    echo -e "${GREEN}${BOLD}Setup requried database ...${NORMAL}${ENDC}"
    setUpMysql
    if [ $? -ne 0 ]; then
        echo -e "${RED}${BOLD}Setup MySQL database timeout${NORMAL}${ENDC}"
        exit 1
    fi

    echo -e "${GREEN}${BOLD}Start servers ...${NORMAL}${ENDC}"
    kubectl apply -f mishards_stateful_servers.yaml
    kubectl apply -f mishards_write_servers.yaml

    checkStatefulSevers
    if [ $? -ne 0 ]; then
        echo -e "${RED}${BOLD}Starting mishards-ro-servers timeout${NORMAL}${ENDC}"
        exit 1
    fi

    checkDeployment "mishards-wo-servers"
    if [ $? -ne 0 ]; then
        echo -e "${RED}${BOLD}Starting mishards-wo-servers timeout${NORMAL}${ENDC}"
        exit 1
    fi

    echo -e "${GREEN}${BOLD}Start rolebinding ...${NORMAL}${ENDC}"
    kubectl apply -f mishards_rbac.yaml

    echo -e "${GREEN}${BOLD}Start proxies ...${NORMAL}${ENDC}"
    kubectl apply -f mishards_proxy.yaml

    checkDeployment "mishards-proxy"
    if [ $? -ne 0 ]; then
        echo -e "${RED}${BOLD}Starting mishards-proxy timeout${NORMAL}${ENDC}"
        exit 1
    fi

    # echo -e "${GREEN}${BOLD}Start flower ...${NORMAL}${ENDC}"
    # kubectl apply -f milvus_flower.yaml
    # checkDeployment "milvus-flower"
    # if [ $? -ne 0 ]; then
    #     echo -e "${RED}${BOLD}Starting milvus-flower timeout${NORMAL}${ENDC}"
    #     exit 1
    # fi

}

function removeApps () {
    # kubectl delete -f milvus_flower.yaml 2>/dev/null
    kubectl delete -f mishards_proxy.yaml 2>/dev/null
    kubectl delete -f mishards_stateful_servers.yaml 2>/dev/null
    kubectl delete -f mishards_write_servers.yaml 2>/dev/null
    kubectl delete -f mishards_rbac.yaml 2>/dev/null
    # kubectl delete -f milvus_monitor.yaml 2>/dev/null
}

function scaleDeployment() {
    deployment_name=$1
    subcommand=$2
    des=$3

    case $des in
    -h|--help|"")
        showscaleHelpMessage $subcommand
        exit 3
        ;;
    esac

    cur=$(kubectl get deployment -n mishards $deployment_name |grep $deployment_name |awk '{split($2, status, "/"); print status[2];}')
    echo -e "${GREEN}Current Running ${BOLD}$cur ${GREEN}${deployment_name}, Scaling to ${BOLD}$des ...${ENDC}";
    scalecmd="kubectl scale deployment -n mishards ${deployment_name} --replicas=${des}"
    ${scalecmd}
    if [ $? -ne 0 ]; then
        echo -e "${RED}${BOLD}Scale Error: ${GREEN}${scalecmd}${ENDC}"
        exit 1
    fi

    checkDeployment $deployment_name

    if [ $? -ne 0 ]; then
        echo -e "${RED}${BOLD}Scale ${deployment_name} timeout${NORMAL}${ENDC}"
        scalecmd="kubectl scale deployment -n mishards ${deployment_name} --replicas=${cur}"
        ${scalecmd}
        if [ $? -ne 0 ]; then
            echo -e "${RED}${BOLD}Scale Rollback Error: ${GREEN}${scalecmd}${ENDC}"
            exit 2
        fi
        echo -e "${BLUE}${BOLD}Scale Rollback to ${cur}${ENDC}"
        exit 1
    fi
    PrintScaleSuccessMessage $cur $des
}

function scaleROServers() {
    subcommand=$1
    des=$2
    case $des in
    -h|--help|"")
        showscaleHelpMessage $subcommand
        exit 3
        ;;
    esac

    cur=$(kubectl get statefulset -n mishards mishards-ro-servers |tail -n 1 |awk '{split($2, status, "/"); print status[2];}')
    echo -e "${GREEN}Current Running ${BOLD}$cur ${GREEN}Readonly Servers, Scaling to ${BOLD}$des ...${ENDC}";
    scalecmd="kubectl scale sts mishards-ro-servers -n mishards --replicas=${des}"
    ${scalecmd}
    if [ $? -ne 0 ]; then
        echo -e "${RED}${BOLD}Scale Error: ${GREEN}${scalecmd}${ENDC}"
        exit 1
    fi

    checkStatefulSevers
    if [ $? -ne 0 ]; then
        echo -e "${RED}${BOLD}Scale mishards-ro-servers timeout${NORMAL}${ENDC}"
        scalecmd="kubectl scale sts mishards-ro-servers -n mishards --replicas=${cur}"
        ${scalecmd}
        if [ $? -ne 0 ]; then
            echo -e "${RED}${BOLD}Scale Rollback Error: ${GREEN}${scalecmd}${ENDC}"
            exit 2
        fi
        echo -e "${BLUE}${BOLD}Scale Rollback to ${cur}${ENDC}"
        exit 1
    fi

    PrintScaleSuccessMessage $cur $des
}


case "$1" in

cleanup)
    kubectl delete -f . 2>/dev/null
    echo -e "${BLUE}${BOLD}All resources are removed${NORMAL}${ENDC}"
    ;;

appdown)
    removeApps;
    echo -e "${BLUE}${BOLD}All pods are removed${NORMAL}${ENDC}"
    ;;

baseup)
    startDependencies;
    echo -e "${BLUE}${BOLD}All pvc, configmap and services up${NORMAL}${ENDC}"
    ;;

appup)
    startApps;
    echo -e "${BLUE}${BOLD}All pods up${NORMAL}${ENDC}"
    ;;

allup)
    startDependencies;
    sleep 2
    startApps;
    echo -e "${BLUE}${BOLD}All resources and pods up${NORMAL}${ENDC}"
    ;;

scale-ro-server)
    scaleROServers $1 $2
    ;;

scale-proxy)
    scaleDeployment "mishards-proxy" $1 $2
    ;;

-h|--help|*)
    showHelpMessage
    ;;

esac
