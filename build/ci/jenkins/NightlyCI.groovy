#!/usr/bin/env groovy

// When scheduling a job that gets automatically triggered by changes,
// you need to include a [cronjob] tag within the commit message.
String cron_timezone = "TZ=Asia/Shanghai"
String cron_string = BRANCH_NAME == "master" ? "50 22 * * * " : ""

int total_timeout_minutes = 660
int e2e_timeout_seconds = 6 * 60 * 60

pipeline {
    agent none
    triggers {
        cron """${cron_timezone}
            ${cron_string}"""
    }
    options {
        timestamps()
        timeout(time: total_timeout_minutes, unit: 'MINUTES')
        buildDiscarder logRotator(artifactDaysToKeepStr: '30')
        // parallelsAlwaysFailFast()
    }
    stages {
        stage ('E2E Test') {
            matrix {
                axes {
                    axis {
                        name 'MILVUS_SERVER_TYPE'
                        values 'standalone', 'distributed'
                    }
                    axis {
                        name 'MILVUS_CLIENT'
                        values 'pymilvus'
                    }
                }
                agent {
                    kubernetes {
                        label "milvus-e2e-test-kind-nightly"
                        inheritFrom 'default'
                        defaultContainer 'main'
                        yamlFile "build/ci/jenkins/pod/krte.yaml"
                        customWorkspace '/home/jenkins/agent/workspace'
                    }
                }
                environment {
                    PROJECT_NAME = "milvus"
                    SEMVER = "${BRANCH_NAME.contains('/') ? BRANCH_NAME.substring(BRANCH_NAME.lastIndexOf('/') + 1) : BRANCH_NAME}"
                    IMAGE_REPO = "dockerhub-mirror-sh.zilliz.cc/milvusdb"
                    DOCKER_BUILDKIT = 1
                    ARTIFACTS = "${env.WORKSPACE}/_artifacts"
                    DOCKER_CREDENTIALS_ID = "f0aacc8e-33f2-458a-ba9e-2c44f431b4d2"
                    TARGET_REPO = "milvusdb"
                }
                stages {
                    stage('Test') {
                        steps {
                            container('etcd') {
                                script {
                                    sh 'ETCDCTL_API=3 etcdctl del "" --from-key=true'
                                }
                            }
                            container('main') {
                                dir ('tests/scripts') {
                                    script {
                                        sh 'printenv'
                                        def clusterEnabled = "false"
                                        if ("${MILVUS_SERVER_TYPE}" == "distributed") {
                                            clusterEnabled = "true"
                                            e2e_timeout_seconds = 10 * 60 * 60
                                        }

                                        if ("${MILVUS_CLIENT}" == "pymilvus") {
                                            sh """
                                            MILVUS_CLUSTER_ENABLED=${clusterEnabled} \
                                            ./e2e-k8s.sh \
                                            --kind-config "${env.WORKSPACE}/build/config/topology/trustworthy-jwt-ci.yaml" \
                                            --node-image registry.zilliz.com/kindest/node:v1.20.2 \
                                            --install-extra-arg "--set etcd.enabled=false --set externalEtcd.enabled=true --set externalEtcd.endpoints={\$KRTE_POD_IP:2379}" \
                                            --skip-export-logs \
                                            --skip-cleanup \
                                            --test-extra-arg "--tags L0 L1 L2 --repeat-scope=session" \
                                            --test-timeout ${e2e_timeout_seconds}
                                            """
                                        } else {
                                            error "Error: Unsupported Milvus client: ${MILVUS_CLIENT}"
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
                            script {
                                emailext subject: '$DEFAULT_SUBJECT',
                                body: '$DEFAULT_CONTENT',
                                recipientProviders: [requestor()],
                                replyTo: '$DEFAULT_REPLYTO',
                                to: 'qa@zilliz.com'
                            }
                        }
                    }
                    success {
                        container('main') {
                            script {
                                def date = sh(returnStdout: true, script: 'date +%Y%m%d').trim()
                                def gitShortCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()

                                withCredentials([usernamePassword(credentialsId: "${env.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]) {
                                    sh 'docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD}'
                                    sh """
                                        docker tag localhost:5000/milvus:latest ${TARGET_REPO}/milvus-nightly:${env.BRANCH_NAME}-${date}-${gitShortCommit}
                                        docker tag localhost:5000/milvus:latest ${TARGET_REPO}/milvus-nightly:${env.BRANCH_NAME}-latest
                                        docker push ${TARGET_REPO}/milvus-nightly:${env.BRANCH_NAME}-${date}-${gitShortCommit}
                                        docker push ${TARGET_REPO}/milvus-nightly:${env.BRANCH_NAME}-latest
                                    """
                                    sh 'docker logout'
                                }
                            }
                        }
                    }
                    always {
                        container('main') {
                            script {
                                sh "./tests/scripts/export_logs.sh"
                                dir("${env.ARTIFACTS}") {
                                    sh "find ./kind -path '*/history/*' -type f | xargs tar -zcvf artifacts-${PROJECT_NAME}-${MILVUS_SERVER_TYPE}-${SEMVER}-${env.BUILD_NUMBER}-e2e-nightly-logs.tar.gz --transform='s:^[^/]*/[^/]*/[^/]*/[^/]*/::g' || true"
                                    if ("${MILVUS_CLIENT}" == "pymilvus") {
                                        sh "tar -zcvf artifacts-${PROJECT_NAME}-${MILVUS_SERVER_TYPE}-${MILVUS_CLIENT}-pytest-logs.tar.gz ./tests/pytest_logs --remove-files || true"
                                    }
                                    archiveArtifacts artifacts: "**.tar.gz", allowEmptyArchive: true
                                }
                            }
                        }
                    }
                    cleanup {
                        container('main') {
                            script {
                                sh "kind delete cluster --name kind -v9 || true"
                                sh 'find . -name . -o -prune -exec rm -rf -- {} +' /* clean up our workspace */
                            }
                        }
                    }
                }
            }
        }
    }
}
