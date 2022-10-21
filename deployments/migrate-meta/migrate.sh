#!/bin/bash

namespace="default"
root_path="by-dev"
operation="migrate"
image_tag="milvusdb/milvus:v2.2.0"
remove_migrate_pod_after_migrate="false"

#-n namespace: The namespace that Milvus is installed in.
#-i milvus_instance: The name of milvus instance.
#-s source_version: The milvus source version.
#-t target_version: The milvus target version.
#-r root_path: The milvus meta root path.
#-w image_tag: The new milvus image tag.
#-o operation: The operation: migrate/rollback.
#-d remove_migrate_pod_after_migrate: Remove migration pod after successful migration.
while getopts "n:i:s:t:r:w:o:d" opt_name
do
  case $opt_name in
    n) namespace=$OPTARG;;
    i) instance_name=$OPTARG;;
    s) source_version=$OPTARG;;
    t) target_version=$OPTARG;;
    r) root_path=$OPTARG;;
    w) image_tag=$OPTARG;;
    o) operation=$OPTARG;;
    d) remove_migrate_pod_after_migrate="true";;
    *) echo "Unkonwen parameters";;
  esac
done

if [ ! $instance_name ]; then
  echo "Missing argument instance_name, please add it. For example:'./migrate.sh -i milvus-instance-name'"
  exit 1
fi

if [ ! $source_version ]; then
  echo "Missing argument source_version, please add it. For example:'./migrate.sh -s 2.1.4'"
  exit 1
fi

if [ ! $target_version ]; then
  echo "Missing argument target_version, please add it. For example:'./migrate.sh -t 2.2.0'"
  exit 1
fi

if [ ! $image_tag ]; then
  echo "Missing argument image_tag, please add it. For example:'./migrate.sh -w milvusdb/milvus:v2.2.0'"
  exit 1
fi

echo "Migration milvus meta will take four steps:"
echo "1. Stop the milvus components"
echo "2. Backup the milvus meta"
echo "3. Migrate the milvus meta"
echo "4. Startup milvus components in new image version"
echo


replicas=()
function fetch_deploy_replica(){
  for deploy in $1;
  do
    replica=$(kubectl get deploy $deploy -n $namespace --output=jsonpath={.spec.replicas})
    replicas+=("$deploy:$replica")
  done
}

function stop_milvus_deploy(){
  echo "Stop milvus deployments: $1"
  kubectl scale deploy -n $namespace $1 --replicas=0 > /dev/null
  wait_for_milvus_stopped $1
  echo "Stopped..."
}

function wait_for_milvus_stopped(){
  while true
  do
    total=0
    for deploy in $1;
    do 
      count=$(kubectl get deploy $deploy -n $namespace --output=jsonpath={.spec.replicas})
      total=`expr $total + $count`
    done
    if [ $total -eq 0 ]; then
      break
    fi
    sleep 5
  done
}

function wait_for_backup_done(){
  backup_pod_name="milvus-meta-migration-backup-${instance_name}"
  configmap_name="milvus-meta-migration-config-${instance_name}"
  while true
  do
    status=$(kubectl get pod -n $namespace $backup_pod_name --output=jsonpath={.status.phase})
    case $status in
      Succeeded)
        echo "Meta backup is done..."
        kubectl annotate configmap $configmap_name -n $namespace backup=succeed
        break
        ;;
      Failed)
        echo "Meta backup is failed..."
        echo "Here is the log:"
        kubectl logs $backup_pod_name -n $namespace
        echo
        exit 2
        ;;
      *)
        sleep 10
        continue
        ;;
    esac
  done
}

function wait_for_migrate_done(){
  migrate_pod_name="milvus-meta-migration-${instance_name}"
  while true
  do
    status=$(kubectl get pod -n $namespace $migrate_pod_name --output=jsonpath={.status.phase})
    case $status in
      Succeeded)
        echo "Migration is done..."
        echo
        break
        ;;
      Failed)
        echo "Migration is failed..."
        echo "Here is the log:"
        kubectl logs $migrate_pod_name -n $namespace
        echo
        exit 3
        ;;
      *)
        sleep 10
        continue
        ;;
    esac
  done
}

function wait_for_rollback_done(){
  rollback_pod_name="milvus-meta-migration-rollback-${instance_name}"
  while true
  do
    status=$(kubectl get pod -n $namespace $rollback_pod_name --output=jsonpath={.status.phase})
    case $status in
      Succeeded)
        echo "Rollback is done..."
        echo
        break
        ;;
      Failed)
        echo "Rollback is failed..."
        echo "Here is the log:"
        kubectl logs $rollback_pod_name -n $namespace
        echo
        exit 5
        ;;
      *)
        sleep 10
        continue
        ;;
    esac
  done
}
function generate_migrate_meta_cm_and_pvc(){
  etcd_svc=$(kubectl get svc -n $namespace -l app.kubernetes.io/instance=$instance_name,app.kubernetes.io/name=etcd -o name | grep -v headless | cut -d '/' -f 2)
  cat <<EOF | kubectl apply -n $namespace -f -
  apiVersion: v1
  kind: PersistentVolumeClaim
  metadata:
    name: milvus-meta-migration-backup-${instance_name}
  spec:
    accessModes:
    - ReadWriteOnce
    resources:
      requests:
        storage: 10Gi
EOF
  cat <<EOF | kubectl apply -n $namespace -f -
  apiVersion: v1
  kind: ConfigMap
  metadata:
    name: milvus-meta-migration-config-${instance_name}
  data:
    backup.yaml: |+
      cmd:
        type: backup

      config:
        sourceVersion: $source_version
        targetVersion: $target_version
        backupFilePath: /milvus/data/migration.bak

      metastore:
        type: etcd

      etcd:
        endpoints:
          - $etcd_svc:2379
        rootPath: $root_path
        metaSubPath: meta
        kvSubPath: kv
    migration.yaml: |+
      cmd:
        type: run

      config:
        sourceVersion: $source_version
        targetVersion: $target_version
        backupFilePath: /milvus/data/migration.bak

      metastore:
        type: etcd

      etcd:
        endpoints:
          - $etcd_svc:2379
        rootPath: $root_path
        metaSubPath: meta
        kvSubPath: kv
    rollback.yaml: |+
      cmd:
        type: rollback

      config:
        sourceVersion: $source_version
        targetVersion: $target_version
        backupFilePath: /milvus/data/migration.bak

      metastore:
        type: etcd

      etcd:
        endpoints:
          - $etcd_svc:2379
        rootPath: $root_path
        metaSubPath: meta
        kvSubPath: kv
EOF
}

function backup_meta(){
  echo "Backuping meta..."
  echo "Checking whether backup exists..."
  configmap_name="milvus-meta-migration-config-${instance_name}"
  backup_exists=$(kubectl get configmap $configmap_name -n $namespace --output=jsonpath={.metadata.annotations.backup})
  if [ "$backup_exists" == "succeed" ]; then
    echo "Found previous backups, skip meta backup..."
  else
    cat <<EOF | kubectl apply -n $namespace -f -
    apiVersion: v1
    kind: Pod
    metadata:
      name: milvus-meta-migration-backup-${instance_name}
    spec:
      restartPolicy: Never
      containers:
      - name: meta-migration
        image: harbor.milvus.io/milvus/meta-migration:20221019-b79687687
        command: ["/bin/sh"]
        args:
        - -c
        - /milvus/bin/meta-migration -config=/milvus/configs/meta/backup.yaml
        volumeMounts:
        - name: backup
          mountPath: /milvus/data
        - name: config
          mountPath: /milvus/configs/meta
      volumes:
      - name: backup
        persistentVolumeClaim:
          claimName: milvus-meta-migration-backup-${instance_name}
      - name: config
        configMap:
          name: milvus-meta-migration-config-${instance_name}
EOF
    wait_for_backup_done
  fi
}

function rollback_meta(){
  generate_migrate_meta_cm_and_pvc
  echo "Checking whether backup exists..."
  backup_exists=$(kubectl get configmap milvus-meta-migration-config -n $namespace --output=jsonpath={.metadata.annotations.backup})
  if [ "$backup_exists" == "succeed" ]; then
    echo "Found previous backups, start meta rollback..."
  cat <<EOF | kubectl apply -n $namespace -f -
  apiVersion: v1
  kind: Pod
  metadata:
    name: milvus-meta-migration-rollback-${instance_name}
  spec:
    restartPolicy: Never
    containers:
    - name: meta-migration
      image: harbor.milvus.io/milvus/meta-migration:20221019-b79687687
      command: ["/bin/sh"]
      args:
      - -c
      - /milvus/bin/meta-migration -config=/milvus/configs/meta/rollback.yaml
      volumeMounts:
      - name: backup
        mountPath: /milvus/data
      - name: config
        mountPath: /milvus/configs/meta
    volumes:
    - name: backup
      persistentVolumeClaim:
        claimName: milvus-meta-migration-backup-${instance_name}
    - name: config
      configMap:
        name: milvus-meta-migration-config-${instance_name}
EOF
    wait_for_rollback_done
  else
    echo "No backup exists, abort..."
    exit 4
  fi
}

function migrate_meta(){
  generate_migrate_meta_cm_and_pvc

  backup_meta
  echo "Migrating meta..."
  cat <<EOF | kubectl apply -n $namespace -f -
  apiVersion: v1
  kind: Pod
  metadata:
    name: milvus-meta-migration-${instance_name}
  spec:
    restartPolicy: Never
    containers:
    - name: meta-migration
      image: harbor.milvus.io/milvus/meta-migration:20221019-b79687687
      command: ["/bin/sh"]
      args:
      - -c
      - /milvus/bin/meta-migration -config=/milvus/configs/meta/migration.yaml
      volumeMounts:
      - name: backup
        mountPath: /milvus/data
      - name: config
        mountPath: /milvus/configs/meta
    volumes:
    - name: backup
      persistentVolumeClaim:
        claimName: milvus-meta-migration-backup-${instance_name}
    - name: config
      configMap:
        name: milvus-meta-migration-config-${instance_name}
EOF
  wait_for_migrate_done
}

function start_milvus_deploy(){
  echo "Starting milvus components..."
  for deploy in "${replicas[@]}"
  do
    IFS=':'
    read -a item <<< "$deploy"
    deploy_name="${item[0]}"
    replica_count="${item[1]}"
    echo "Starting $deploy_name..."
    component=$(kubectl get deploy -n $namespace $deploy_name --output=jsonpath={.metadata.labels.component})
    kubectl patch deployment/$deploy_name -n $namespace --patch-file=/dev/stdin <<-EOF
    spec:
      replicas: $replica_count
      template:
        spec:
          containers:
          - name: $component
            image: $image_tag
EOF
  done
}

function wait_for_milvus_ready(){
  total_component_count=${#replicas[@]}
  while true
  do
    ready_count=0
    for deploy in "${replicas[@]}"
    do 
      IFS=':'
      read -a item <<< "$deploy"
      deploy_name="${item[0]}"
      replica_count="${item[1]}"
      count=$(kubectl get deploy $deploy_name -n $namespace --output=jsonpath={.status.readyReplicas})
      if [ "$count" == "$replica_count" ] ; then
        ready_count=`expr $ready_count + 1` 
      fi
    done
    if [ "$total_component_count" == "$ready_count" ]; then
      break
    fi
    sleep 5
  done
}

deployments=$(kubectl get deploy -n $namespace -l app.kubernetes.io/instance=$instance_name,app.kubernetes.io/name=milvus --output=jsonpath={.items..metadata.name})
if [ ${#deployments} == 0 ]; then
	echo "There is no Milvus instance $instance_name in the namespace $namespace"
else
	 fetch_deploy_replica "${deployments[*]}"
   stop_milvus_deploy "$deployments"
   echo 
   case $operation in
     migrate)
       echo "Starting to migrate milvus meta..."
       migrate_meta
       start_milvus_deploy
       echo
       echo "Upgrading is done. Waiting for milvus components to be ready again..."
       wait_for_milvus_ready
       if [ "$remove_migrate_pod_after_migrate" == "true" ]; then
         kubectl delete pods -n $namespace "milvus-meta-migration-backup-${instance_name}" "milvus-meta-migration-${instance_name}"
         kubectl delete pvc -n $namespace "milvus-meta-migration-backup-${instance_name}"
         kubectl delete configmap -n $namespace "milvus-meta-migration-config-${instance_name}"
       fi
       echo "All milvus components are running. Enjoy your vector search..."
       ;;
     rollback)
       echo "Starting to rollback milvus meta..."
       rollback_meta
       start_milvus_deploy
       echo
       echo "Rollbacking is done. Waiting for milvus components to be ready again..."
       wait_for_milvus_ready
       echo "All milvus components are running. Enjoy your vector search..."
       ;;
     *)
       echo "Invalid operation..."
       exit 6
       ;;
   esac
fi
