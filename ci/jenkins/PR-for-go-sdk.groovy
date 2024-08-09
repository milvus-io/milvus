#!/usr/bin/env groovy

int total_timeout_minutes = 60 * 5
int e2e_timeout_seconds = 120 * 60
def imageTag = '123'
int case_timeout_seconds = 20 * 60
def chart_version = '4.1.8'
def release_name = ''
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
                inheritFrom 'milvus-e2e-4am'
                defaultContainer 'main'
                yamlFile 'ci/jenkins/pod/rte-build.yaml'
                customWorkspace '/home/jenkins/agent/milvus'
            }
    }
    environment {
        PROJECT_NAME = 'milvus'
        SEMVER = "${BRANCH_NAME.contains('/') ? BRANCH_NAME.substring(BRANCH_NAME.lastIndexOf('/') + 1) : BRANCH_NAME}"
        DOCKER_BUILDKIT = 1
        ARTIFACTS = "${env.WORKSPACE}/_artifacts"
        CI_DOCKER_CREDENTIAL_ID = 'harbor-milvus-io-registry'
        MILVUS_HELM_NAMESPACE = 'milvus-ci'
        DISABLE_KIND = true
        HUB = 'harbor.milvus.io/milvus'
        JENKINS_BUILD_ID = "${env.BUILD_ID}"
        CI_MODE = 'pr'
        SHOW_MILVUS_CONFIGMAP = true
    }

    stages {
        stage('Build') {
            steps {
                container('main') {
                    dir('build') {
                            sh '''
                            MIRROR_URL="https://docker-nexus-ci.zilliz.cc" ./set_docker_mirror.sh
                            '''
                    }
                    dir('tests/scripts') {
                        script {
                            sh 'printenv'
                            def date = sh(returnStdout: true, script: 'date +%Y%m%d').trim()
                            sh 'git config --global --add safe.directory /home/jenkins/agent/workspace'
                            def gitShortCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
                            imageTag = "${env.BRANCH_NAME}-${date}-${gitShortCommit}"
                            withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]) {
                                sh """
                                TAG="${imageTag}" \
                                ./e2e-k8s.sh \
                                --skip-export-logs \
                                --skip-install \
                                --skip-cleanup \
                                --skip-setup \
                                --skip-test
                                """

                                // stash imageTag info for rebuild install & E2E Test only
                                sh "echo ${imageTag} > imageTag.txt"
                                stash includes: 'imageTag.txt', name: 'imageTag'
                            }
                        }
                    }
                }
            }
        }

        stage('Install & E2E Test') {
            matrix {
                axes {
                    axis {
                        name 'MILVUS_SERVER_TYPE'
                        values 'standalone', 'distributed'
                    }
                    axis {
                        name 'MILVUS_CLIENT'
                        values 'milvus-sdk-go'
                    }
                }

                stages {
                    stage('Install') {
                        steps {
                            container('main') {
                                stash includes: 'tests/**', name: 'testCode', useDefaultExcludes: false
                                stash includes: 'client/**', name: 'clientCode', useDefaultExcludes: false
                                dir('tests/scripts') {
                                    script {
                                        sh 'printenv'
                                        def clusterEnabled = 'false'
                                        def valuesFile = 'pr-4am.yaml'
                                        if ("${MILVUS_SERVER_TYPE}".contains('distributed')) {
                                            clusterEnabled = 'true'
                                        }
                                            if ("${imageTag}" == '') {
                                                dir('imageTag') {
                                                    try {
                                                        unstash 'imageTag'
                                                        imageTag = sh(returnStdout: true, script: 'cat imageTag.txt | tr -d \'\n\r\'')
                                                    }catch (e) {
                                                        print 'No Image Tag info remained ,please rerun build to build new image.'
                                                        exit 1
                                                    }
                                                }
                                            }
                                            // modify values file to enable kafka
                                            if ("${MILVUS_SERVER_TYPE}".contains('kafka')) {
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
                                            withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]) {
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

                                            release_name = sh(returnStdout: true, script: './get_release_name.sh').trim()
                                    }
                                }
                            }
                        }
                    }
                    stage('E2E Test') {
                        options {
                            skipDefaultCheckout()
                        }
                        agent {
                                kubernetes {
                                    cloud '4am'
                                    inheritFrom 'default'
                                    defaultContainer 'main'
                                    yamlFile 'ci/jenkins/pod/e2e-go-sdk.yaml'
                                    customWorkspace '/home/jenkins/agent/milvus'
                                }
                        }
                        steps {
                            container('gotestsum') {
                                unstash('testCode')
                                unstash('clientCode')
                                dir('tests/scripts') {
                                    script {
                                        def clusterEnabled = 'false'
                                        if ("${MILVUS_SERVER_TYPE}".contains('distributed')) {
                                            clusterEnabled = 'true'
                                        }
                                    }
                                }

                                dir('tests/go_client') {

                                    script {
                                        sh """
                                        gotestsum --format testname --hide-summary=output ./testcases/... --tags L0 --addr=${release_name}-milvus.milvus-ci:19530 -timeout=60m
                                        """

                                    }

                                }
                            }
                        }
                    }
                }
                post {
                    always {
                        container('main') {
                            dir('tests/scripts') {
                                script {
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
    post {
        unsuccessful {
                container('jnlp') {
                    dir('tests/scripts') {
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
