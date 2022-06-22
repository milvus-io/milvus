pipeline {
    options {
        timestamps()
    }
    agent {
        kubernetes {
            label "milvus-test"
            defaultContainer 'main'
            yamlFile "build/ci/jenkins/pod/chaos-test.yaml"
            customWorkspace '/home/jenkins/agent/workspace'
            // idle 5 minutes to wait clean up tasks
            idleMinutes 5
        }
    }
    parameters{
        choice(
            description: 'Milvus Mode',
            name: 'milvus_mode',
            choices: ["standalone", "cluster"]
        )
        choice(
            description: 'MQ Type',
            name: 'mq_type',
            choices: ["kafka"]
        )        
        choice(
            description: 'Deploy Test Task',
            name: 'deploy_task',
            choices: ['reinstall']
        )
        string(
            description: 'Old Image Repository',
            name: 'old_image_repository',
            defaultValue: 'milvusdb/milvus'
        )
        string(
            description: 'Old Version Image Tag',
            name: 'old_image_tag',
            defaultValue: 'latest'
        )
        string(
            description: 'New Image Repository',
            name: 'new_image_repository',
            defaultValue: 'registry.milvus.io/milvus/milvus'
        )
        string(
            description: 'New Version Image Tag',
            name: 'new_image_tag',
            defaultValue: 'master-latest'
        )
        string(
            description: 'Etcd Image Repository',
            name: 'etcd_image_repository',
            defaultValue: "milvusdb/etcd"
        )
        string(
            description: 'Etcd Image Tag',
            name: 'etcd_image_tag',
            defaultValue: "3.5.0-r3"
        )
        string(
            description: 'Querynode Nums',
            name: 'querynode_nums',
            defaultValue: '3'
        )
        string(
            description: 'DataNode Nums',
            name: 'datanode_nums',
            defaultValue: '2'
        )
        string(
            description: 'IndexNode Nums',
            name: 'indexnode_nums',
            defaultValue: '1'
        )
        string(
            description: 'Proxy Nums',
            name: 'proxy_nums',
            defaultValue: '1'
        )
        string(
            description: 'Data Size',
            name: 'data_size',
            defaultValue: '3000'
        )
        string(
            description: 'Idle Time in Minutes',
            name: 'idel_time',
            defaultValue: '1'
        )
        booleanParam(
            description: 'Keep Env',
            name: 'keep_env',
            defaultValue: 'false'
        )
    }
    
    environment {
        ARTIFACTS = "${env.WORKSPACE}/_artifacts"
        RELEASE_NAME = "${params.milvus_mode}-${params.deploy_task}-${env.BUILD_ID}"
        NAMESPACE = "chaos-testing"
        new_image_tag_modified = ""
    }

    stages {
        stage ('Install Dependency') {
            steps {
                container('main') {
                    dir ('tests/python_client') {
                        script {
                        sh "pip install -r requirements.txt --trusted-host https://test.pypi.org"       
                        }
                    }
                }
            }
        }
        stage ('Modify Milvus chart values') {
            steps {
                container('main') {
                    dir ('tests/python_client/deploy') {
                        script {
                        // disable all mq
                        sh "yq -i '.kafka.enabled = false' cluster-values.yaml"
                        sh "yq -i '.pulsar.enabled = false' cluster-values.yaml"
                        // enable mq_type
                        if ("${params.mq_type}" == "pulsar") {
                            sh "yq -i '.pulsar.enabled = true' cluster-values.yaml"
                        } else if ("${params.mq_type}" == "kafka") {
                            sh "yq -i '.kafka.enabled = true' cluster-values.yaml"
                            sh "yq -i '.kafka.enabled = true' standalone-values.yaml"
                        }
                        sh"""
                        yq -i '.queryNode.replicas = "${params.querynode_nums}"' cluster-values.yaml
                        yq -i '.dataNode.replicas = "${params.datanode_nums}"' cluster-values.yaml
                        yq -i '.indexNode.replicas = "${params.indexnode_nums}"' cluster-values.yaml
                        yq -i '.proxy.replicas = "${params.proxy_nums}"' cluster-values.yaml
                        """
                        if ("${params.milvus_mode}" == "cluster"){
                            sh "cat cluster-values.yaml"
                        }
                        if ("${params.mq_type}" == "standalone"){
                            sh "cat standalone-values.yaml"
                        }
                        
                        }
                        }
                    }
                }
        }        
        stage ('First Milvus Deployment') {
            options {
              timeout(time: 10, unit: 'MINUTES')   // timeout on this stage
            }
            steps {
                container('main') {
                    dir ('tests/python_client/deploy') {
                        script {
                            def old_image_tag_modified = ""
                            def new_image_tag_modified = ""

                            def old_image_repository_modified = ""
                            def new_image_repository_modified = ""

                            if ("${params.old_image_tag}" == "master-latest") {
                                old_image_tag_modified = sh(returnStdout: true, script: 'bash ../../../scripts/docker_image_find_tag.sh -n milvusdb/milvus-dev -t master-latest -f master- -F -L -q').trim()    
                            }
                            else if ("${params.old_image_tag}" == "latest") {
                                old_image_tag_modified = sh(returnStdout: true, script: 'bash ../../../scripts/docker_image_find_tag.sh -n milvusdb/milvus -t latest -F -L -q').trim()
                            }
                            else {
                                old_image_tag_modified = "${params.old_image_tag}"
                            }

                            if ("${params.new_image_tag}" == "master-latest") {
                                new_image_tag_modified = sh(returnStdout: true, script: 'bash ../../../scripts/docker_image_find_tag.sh -n milvusdb/milvus-dev -t master-latest -f master- -F -L -q').trim()    
                            }
                            else {
                                new_image_tag_modified = "${params.new_image_tag}"
                            }
                            sh "echo ${old_image_tag_modified}"
                            sh "echo ${new_image_tag_modified}"
                            sh "echo ${new_image_tag_modified} > new_image_tag_modified.txt"
                            stash includes: 'new_image_tag_modified.txt', name: 'new_image_tag_modified'
                            env.new_image_tag_modified = new_image_tag_modified
                            sh "docker pull ${params.old_image_repository}:${old_image_tag_modified}"
                            sh "docker pull ${params.new_image_repository}:${new_image_tag_modified}"
                            if ("${params.deploy_task}" == "reinstall"){
                                echo "reinstall Milvus with new image tag"
                                old_image_tag_modified = new_image_tag_modified
                            }
                            if ("${params.deploy_task}" == "reinstall"){
                                echo "reinstall Milvus with new image repository"
                                old_image_repository_modified = "${params.new_image_repository}"
                            }
                            else {
                                old_image_repository_modified = "${params.old_image_repository}"
                            }

                            sh "helm repo add milvus https://milvus-io.github.io/milvus-helm"
                            sh "helm repo update"
                            if ("${params.milvus_mode}" == "standalone") {
                                sh "helm install --wait --timeout 720s ${env.RELEASE_NAME} milvus/milvus  --set image.all.repository=${old_image_repository_modified} --set image.all.tag=${old_image_tag_modified} -f standalone-values.yaml;"    
                            }
                            if ("${params.milvus_mode}" == "cluster") {
                                sh "helm install --wait --timeout 720s ${env.RELEASE_NAME} milvus/milvus  --set image.all.repository=${old_image_repository_modified} --set image.all.tag=${old_image_tag_modified} -f cluster-values.yaml;"    
                            }
                            sh "kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance=${env.RELEASE_NAME} -n ${env.NAMESPACE} --timeout=360s"
                            sh "kubectl wait --for=condition=Ready pod -l release=${env.RELEASE_NAME} -n ${env.NAMESPACE} --timeout=360s"
                            sh "kubectl get pods -o wide|grep ${env.RELEASE_NAME}"
                            }
                        }
                    }
                }
        }
        stage ('Run first test') {
            options {
              timeout(time: 20, unit: 'MINUTES')   // timeout on this stage
            }            
            steps {
                container('main') {
                    dir ('tests/python_client/deploy/scripts') {
                        script {
                        def host = sh(returnStdout: true, script: "kubectl get svc/${env.RELEASE_NAME}-milvus -o jsonpath=\"{.spec.clusterIP}\"").trim()
                        
                        if ("${params.deploy_task}" == "reinstall") {
                            sh "python3 action_before_reinstall.py --host ${host} --data_size ${params.data_size}"
                        }

                        if ("${params.deploy_task}" == "upgrade") {
                            sh "python3 action_before_upgrade.py --host ${host} --data_size ${params.data_size}"
                        }
                        }
                    }
                }
            }
            
        }

        stage ('Milvus Idle Time') {

            steps {
                container('main') {
                    dir ('tests/python_client/deploy') {
                        script {
                        echo "sleep ${params.idel_time}m"
                        sh "sleep ${params.idel_time}m"
                        }
                    }
                }
            }
        }

        stage ('Export log for first deployment') {

            steps {
                container('main') {
                    dir ('tests/python_client/deploy') {
                        script {
                        echo "get pod status"
                        sh "kubectl get pods -o wide|grep ${env.RELEASE_NAME} || true"
                        echo "collecte logs"
                        sh "bash ../../scripts/export_log_k8s.sh ${env.NAMESPACE} ${env.RELEASE_NAME} k8s_log/${env.RELEASE_NAME}/first_deployment || echo 'export log failed'"
                        }
                    }
                }
            }
        }

        stage ('Restart Milvus') {
            options {
              timeout(time: 15, unit: 'MINUTES')   // timeout on this stage
            }
            steps {
                container('main') {
                    dir ('tests/python_client/deploy') {
                        script {
                            sh "kubectl delete pod -l app.kubernetes.io/instance=${env.RELEASE_NAME} --grace-period=0 --force"
                            sh "kubectl delete pod -l release=${env.RELEASE_NAME} --grace-period=0 --force"
                            sh "kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance=${env.RELEASE_NAME} -n ${env.NAMESPACE} --timeout=360s"
                            sh "kubectl wait --for=condition=Ready pod -l release=${env.RELEASE_NAME} -n ${env.NAMESPACE} --timeout=360s"
                        }
                    }
                }
            }
            
        }

        stage ('Second Milvus Deployment') {
            options {
              timeout(time: 15, unit: 'MINUTES')   // timeout on this stage
            }
            steps {
                container('main') {
                    dir ('tests/python_client/deploy') {                                     
                        script {

                            // in case of master-latest is different in two stages, we need use the new_image_tag_modified.txt to store the new_image_tag in first stage
                            def new_image_tag_modified = ""

                            dir ("new_image_tag_modified"){
                                try{
                                    unstash 'new_image_tag_modified'
                                    new_image_tag_modified=sh(returnStdout: true, script: 'cat new_image_tag_modified.txt | tr -d \'\n\r\'')
                                }catch(e){
                                    print "No image tag info remained"
                                    exit 1
                                }
                            }

                            if ("${params.milvus_mode}" == "standalone") {
                                sh "helm upgrade --wait --timeout 720s ${env.RELEASE_NAME} milvus/milvus  --set image.all.repository=${params.new_image_repository} --set image.all.tag=${new_image_tag_modified} -f standalone-values.yaml"    
                            }
                            if ("${params.milvus_mode}" == "cluster") {
                                sh "helm upgrade --wait --timeout 720s ${env.RELEASE_NAME} milvus/milvus  --set image.all.repository=${params.new_image_repository} --set image.all.tag=${new_image_tag_modified} -f cluster-values.yaml"    
                            }
                            sh "kubectl wait --for=condition=Ready pod -l app.kubernetes.io/instance=${env.RELEASE_NAME} -n ${env.NAMESPACE} --timeout=360s"
                            sh "kubectl wait --for=condition=Ready pod -l release=${env.RELEASE_NAME} -n ${env.NAMESPACE} --timeout=360s"                               
                            sh "kubectl get pods -o wide|grep ${env.RELEASE_NAME}"
                        }
                    }
                }
            }
            
        }

        stage ('Run Second Test') {
            options {
              timeout(time: 20, unit: 'MINUTES')   // timeout on this stage
            }
            steps {
                container('main') {
                    dir ('tests/python_client/deploy/scripts') {
                        script {
                        sh "sleep 60s" // wait loading data for the second deployment to be ready
                        def host = sh(returnStdout: true, script: "kubectl get svc/${env.RELEASE_NAME}-milvus -o jsonpath=\"{.spec.clusterIP}\"").trim()
                        if ("${params.deploy_task}" == "reinstall") {
                            sh "python3 action_after_reinstall.py --host ${host} --data_size ${params.data_size}"
                        }

                        if ("${params.deploy_task}" == "upgrade") {
                            sh "python3 action_after_upgrade.py --host ${host} --data_size ${params.data_size}"
                        }
                        }
                    }
                }
            }
            
        }
 
    }
    post {
        always {
            echo 'upload logs'
            container('main') {
                dir ('tests/python_client/chaos') {
                    script {
                        echo "get pod status"
                        sh "kubectl get pods -o wide|grep ${env.RELEASE_NAME} || true"
                        echo "collecte logs"
                        sh "bash ../../scripts/export_log_k8s.sh ${env.NAMESPACE} ${env.RELEASE_NAME} k8s_log/${env.RELEASE_NAME}/second_deployment || echo 'export log failed'"
                        echo "upload logs"
                        sh "tar -zcvf artifacts-${env.RELEASE_NAME}-logs.tar.gz k8s_log/ --remove-files || true"
                        archiveArtifacts artifacts: "artifacts-${env.RELEASE_NAME}-logs.tar.gz", allowEmptyArchive: true
                        if ("${params.keep_env}" == "false"){
                            sh "bash scripts/uninstall_milvus.sh ${env.RELEASE_NAME}"
                        }
                    }
                }
            }
        
        }
        success {
            echo 'I succeeeded!'
            container('main') {
                dir ('tests/python_client/chaos/scripts') {
                    script {
                        sh "bash uninstall_milvus.sh ${env.RELEASE_NAME} || true"
                    }
                }
            }  

        }
        unstable {
            echo 'I am unstable :/'
        }
        failure {
            echo 'I failed :('
        }
        changed {
            echo 'Things were different before...'
        }
    }
}