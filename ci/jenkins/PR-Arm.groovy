#!/usr/bin/env groovy

int total_timeout_minutes = 60 * 5
int e2e_timeout_seconds = 120 * 60
def imageTag=''
int case_timeout_seconds = 20 * 60
def chart_version='4.2.48'
pipeline {
    options {
        timestamps()
        timeout(time: total_timeout_minutes, unit: 'MINUTES')
        buildDiscarder logRotator(artifactDaysToKeepStr: '30')
        parallelsAlwaysFailFast()
        preserveStashes(buildCount: 5)
        disableConcurrentBuilds(abortPrevious: true)

    }
    agent {
            kubernetes {
                cloud '4am'
                defaultContainer 'main'
                yamlFile 'ci/jenkins/pod/rte-arm.yaml'
                customWorkspace '/home/jenkins/agent/workspace'
            }
    }
    environment {
        PROJECT_NAME = 'milvus'
        SEMVER = "${BRANCH_NAME.contains('/') ? BRANCH_NAME.substring(BRANCH_NAME.lastIndexOf('/') + 1) : BRANCH_NAME}"
        DOCKER_BUILDKIT = 1
        ARTIFACTS = "${env.WORKSPACE}/_artifacts"
        CI_DOCKER_CREDENTIAL_ID = "harbor-milvus-io-registry"
        MILVUS_HELM_NAMESPACE = "milvus-ci"
        DISABLE_KIND = true
        HUB = 'harbor.milvus.io/milvus'
        JENKINS_BUILD_ID = "${env.BUILD_ID}"
        CI_MODE="pr"
        SHOW_MILVUS_CONFIGMAP= true

        DOCKER_CREDENTIALS_ID = "dockerhub"
        TARGET_REPO = "milvusdb"
        HARBOR_REPO = "harbor.milvus.io"
    }

    stages {
        stage ('Build'){
            steps {
                container('main') {
                        script {
                            sh 'printenv'
                            def date = sh(returnStdout: true, script: 'date +%Y%m%d').trim()
                            sh 'git config --global --add safe.directory /home/jenkins/agent/workspace'
                            def gitShortCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()    
                            imageTag="${env.BRANCH_NAME}-${date}-${gitShortCommit}"


                            sh """
                            echo "Building image with tag: ${imageTag}"

                            set -a  # automatically export all variables from .env
                            . .env
                            set +a  # stop automatically


                            docker run --net=host -v /root/.conan:/root/.conan -v \$(pwd):/root/milvus -w /root/milvus milvusdb/milvus-env:ubuntu20.04-\${DATE_VERSION} sh -c "make clean && make install"
                            """

                            withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]){
                                sh "docker login ${env.HARBOR_REPO} -u '${CI_REGISTRY_USERNAME}' -p '${CI_REGISTRY_PASSWORD}'"
                                sh """
                                    export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"
                                    export MILVUS_IMAGE_TAG="${imageTag}"

                                    docker build --build-arg TARGETARCH=arm64  -f "./build/docker/milvus/ubuntu20.04/Dockerfile" -t \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} .

                                    docker push \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                    docker logout
                                """
                            }

                            // stash imageTag info for rebuild install & E2E Test only
                            sh "echo ${imageTag} > imageTag.txt"
                            stash includes: 'imageTag.txt', name: 'imageTag'

                        }
                }
            }
        }


        stage('Install & E2E Test') {
            matrix {
                axes {
                    axis {
                        name 'MILVUS_SERVER_TYPE'
                        values 'standalone'
                    }
                    axis {
                        name 'MILVUS_CLIENT'
                        values 'pymilvus'
                    }
                }

                stages {
                    stage('Install') {
                        agent {
                            kubernetes {
                                cloud '4am'
                                inheritFrom 'milvus-e2e-4am'
                                defaultContainer 'main'
                                yamlFile 'ci/jenkins/pod/rte-build.yaml'
                                customWorkspace '/home/jenkins/agent/workspace'
                            }
                        }
                        steps {
                            container('main') {
                                stash includes: 'tests/**', name: 'testCode', useDefaultExcludes: false
                                dir ('tests/scripts') {
                                    script {
                                        sh 'printenv'
                                        def clusterEnabled = "false"
                                        def valuesFile = "pr-arm.yaml"

                                        if ("${MILVUS_SERVER_TYPE}" == "standalone-one-pod") {
                                            valuesFile = "nightly-one-pod.yaml"
                                        }

                                        if ("${MILVUS_CLIENT}" == "pymilvus") {
                                            if ("${imageTag}"==''){
                                                dir ("imageTag"){
                                                    try{
                                                        unstash 'imageTag'
                                                        imageTag=sh(returnStdout: true, script: 'cat imageTag.txt | tr -d \'\n\r\'')
                                                    }catch(e){
                                                        print "No Image Tag info remained ,please rerun build to build new image."
                                                        exit 1
                                                    }
                                                }
                                            }
                                            // modify values file to enable kafka
                                            if ("${MILVUS_SERVER_TYPE}".contains("kafka")) {
                                                sh '''
                                                    apt-get update
                                                    apt-get install wget -y
                                                    wget https://github.com/mikefarah/yq/releases/download/v4.34.1/yq_linux_amd64 -O /usr/bin/yq
                                                    chmod +x /usr/bin/yq
                                                '''
                                                sh """
                                                    cp values/ci/pr-4am.yaml values/ci/pr_kafka.yaml
                                                    yq -i '.pulsar.enabled=false' values/ci/pr_kafka.yaml
                                                    yq -i '.kafka.enabled=true' values/ci/pr_kafka.yaml
                                                    yq -i '.kafka.metrics.kafka.enabled=true' values/ci/pr_kafka.yaml
                                                    yq -i '.kafka.metrics.jmx.enabled=true' values/ci/pr_kafka.yaml
                                                    yq -i '.kafka.metrics.serviceMonitor.enabled=true' values/ci/pr_kafka.yaml
                                                """
                                            }
                                            withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]){
                                                if ("${MILVUS_SERVER_TYPE}" == "standalone-one-pod") {
                                                    try {
                                                        sh """
                                                        MILVUS_CLUSTER_ENABLED=${clusterEnabled} \
                                                        MILVUS_HELM_REPO="https://nexus-ci.zilliz.cc/repository/milvus-proxy" \
                                                        TAG=${imageTag}\
                                                        ./e2e-k8s.sh \
                                                        --skip-export-logs \
                                                        --skip-cleanup \
                                                        --skip-setup \
                                                        --skip-test \
                                                        --skip-build \
                                                        --skip-build-image \
                                                        --install-extra-arg "
                                                        --set etcd.metrics.enabled=true \
                                                        --set etcd.metrics.podMonitor.enabled=true \
                                                        --set indexCoordinator.gc.interval=1 \
                                                        --set indexNode.disk.enabled=true \
                                                        --set queryNode.disk.enabled=true \
                                                        --set standalone.disk.enabled=true \
                                                        --version ${chart_version} \
                                                        -f values/ci/${valuesFile}"
                                                        """
                                                    } catch (Exception e) {
                                                        echo "Tests failed, but the build will not be marked as failed."
                                                    }
           
                                                }else{
                                                    sh """
                                                        MILVUS_CLUSTER_ENABLED=${clusterEnabled} \
                                                        MILVUS_HELM_REPO="https://nexus-ci.zilliz.cc/repository/milvus-proxy" \
                                                        TAG=${imageTag}\
                                                        ./e2e-k8s.sh \
                                                        --skip-export-logs \
                                                        --skip-cleanup \
                                                        --skip-setup \
                                                        --skip-test \
                                                        --skip-build \
                                                        --skip-build-image \
                                                        --install-extra-arg "
                                                        --set etcd.metrics.enabled=true \
                                                        --set etcd.metrics.podMonitor.enabled=true \
                                                        --set indexCoordinator.gc.interval=1 \
                                                        --set indexNode.disk.enabled=true \
                                                        --set queryNode.disk.enabled=true \
                                                        --set standalone.disk.enabled=true \
                                                        --version ${chart_version} \
                                                        -f values/ci/${valuesFile}"
                                                        """
                                                } 
                                            }
                                        } else {
                                            error "Error: Unsupported Milvus client: ${MILVUS_CLIENT}"
                                        }
                                    }
                                }
                            }

                        }
                    }
                    stage('E2E Test'){
                        options { 
                            skipDefaultCheckout() 
                        }
                        agent {
                          kubernetes {
                              cloud '4am'
                              inheritFrom 'default'
                              defaultContainer 'main'
                              yamlFile 'ci/jenkins/pod/e2e.yaml'
                              customWorkspace '/home/jenkins/agent/workspace'
                          }
                        }
                        steps {
                            container('pytest') {
                                unstash('testCode')
                                script {
                                        sh 'ls -lah'
                                }
                                dir ('tests/scripts') {
                                    script {
                                        def release_name=sh(returnStdout: true, script: './get_release_name.sh')
                                        def clusterEnabled = 'false'
                                        if ("${MILVUS_SERVER_TYPE}".contains("distributed")) {
                                            clusterEnabled = "true"
                                        }
                                        if ("${MILVUS_CLIENT}" == "pymilvus") {
                                            if ("${MILVUS_SERVER_TYPE}" == "standalone-one-pod") {
                                                try {
                                                    sh """
                                                    MILVUS_HELM_RELEASE_NAME="${release_name}" \
                                                    MILVUS_HELM_NAMESPACE="milvus-ci" \
                                                    MILVUS_CLUSTER_ENABLED="${clusterEnabled}" \
                                                    TEST_TIMEOUT="${e2e_timeout_seconds}" \
                                                    ./ci_e2e_4am.sh  "-n 6 -x --tags L0 L1 --timeout ${case_timeout_seconds}"
                                                    """
                                                } catch (Exception e) {
                                                    echo "Tests failed, but the build will not be marked as failed."
                                                }
                                            }else{
                                                sh """
                                                MILVUS_HELM_RELEASE_NAME="${release_name}" \
                                                MILVUS_HELM_NAMESPACE="milvus-ci" \
                                                MILVUS_CLUSTER_ENABLED="${clusterEnabled}" \
                                                TEST_TIMEOUT="${e2e_timeout_seconds}" \
                                                ./ci_e2e_4am.sh  "-n 6 -x --tags L0 L1 --timeout ${case_timeout_seconds}"
                                                """
                                            }
                                        } else {
                                        error "Error: Unsupported Milvus client: ${MILVUS_CLIENT}"
                                        }
                                    }
                                }
                            }
                        }
                        post{
                            always {
                                container('pytest'){
                                    dir("${env.ARTIFACTS}") {
                                            sh "tar -zcvf ${PROJECT_NAME}-${MILVUS_SERVER_TYPE}-${MILVUS_CLIENT}-pytest-logs.tar.gz /tmp/ci_logs/test --remove-files || true"
                                            archiveArtifacts artifacts: "${PROJECT_NAME}-${MILVUS_SERVER_TYPE}-${MILVUS_CLIENT}-pytest-logs.tar.gz ", allowEmptyArchive: true
                                    }
                                }
                            }

                        }
                    }
                }
                post{
                    always {
                        container('main') {
                            dir ('tests/scripts') {  
                                script {
                                    def release_name=sh(returnStdout: true, script: './get_release_name.sh')
                                    sh "kubectl get pods -n ${MILVUS_HELM_NAMESPACE} | grep ${release_name} "
                                    sh "./uninstall_milvus.sh --release-name ${release_name}"
                                    sh "./ci_logs.sh --log-dir /ci-logs  --artifacts-name ${env.ARTIFACTS}/artifacts-${PROJECT_NAME}-${MILVUS_SERVER_TYPE}-${SEMVER}-${env.BUILD_NUMBER}-${MILVUS_CLIENT}-e2e-logs \
                                    --release-name ${release_name}"
                                    dir("${env.ARTIFACTS}") {
                                        archiveArtifacts artifacts: "artifacts-${PROJECT_NAME}-${MILVUS_SERVER_TYPE}-${SEMVER}-${env.BUILD_NUMBER}-${MILVUS_CLIENT}-e2e-logs.tar.gz", allowEmptyArchive: true
                                    }
                                }
                            }
                        }
                    }
                }

            }

        }
    }
    post{
        unsuccessful {
                container('jnlp') {
                    dir ('tests/scripts') {
                        script {
                            def authorEmail = sh(returnStdout: true, script: './get_author_email.sh ')
                            emailext subject: '$DEFAULT_SUBJECT',
                            body: '$DEFAULT_CONTENT',
                            recipientProviders: [developers(), culprits()],
                            replyTo: '$DEFAULT_REPLYTO',
                            to: "${authorEmail},devops@zilliz.com"
                        }
                    }
                }
            }
        }
}
