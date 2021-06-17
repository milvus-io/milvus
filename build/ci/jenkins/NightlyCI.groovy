#!/usr/bin/env groovy

// When scheduling a job that gets automatically triggered by changes,
// you need to include a [cronjob] tag within the commit message.
String cron_timezone = "TZ=Asia/Shanghai"
String cron_string = BRANCH_NAME == "master" ? "50 22 * * * " : ""

pipeline {
    agent none
    triggers {
        cron """${cron_timezone}
            ${cron_string}"""
    }
    options {
        timestamps()
        timeout(time: 1, unit: 'HOURS')
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
                    ARTIFACTS = "${env.WORKSPACE}/artifacts"
                    DOCKER_CREDENTIALS_ID = "ba070c98-c8cc-4f7c-b657-897715f359fc"
                    DOKCER_REGISTRY_URL = "registry.zilliz.com"
                    TARGET_REPO = "${DOKCER_REGISTRY_URL}/milvus"
                    MILVUS_HELM_BRANCH = "recovery"
                }
                stages {
                    stage('Test') {
                        steps {
                            container('main') {
                                dir ('tests/scripts') {
                                    script {
                                        def standaloneEnabled = "true"
                                        if ("${MILVUS_SERVER_TYPE}" == "distributed") {
                                            standaloneEnabled = "false"
                                        }

                                        sh "MILVUS_STANDALONE_ENABLED=${standaloneEnabled} ./e2e-k8s.sh --node-image registry.zilliz.com/kindest/node:v1.20.2"
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
                    always {
                        container('main') {
                            script {
                                def date = sh(returnStdout: true, script: 'date +%Y%m%d').trim()
                                def gitShortCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()

                                withCredentials([usernamePassword(credentialsId: "${env.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]) {
                                    sh 'docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD} ${DOKCER_REGISTRY_URL}'
                                    sh """
                                        docker tag localhost:5000/milvus:latest ${TARGET_REPO}/milvus:${env.BRANCH_NAME}-${date}-${gitShortCommit}
                                        docker tag localhost:5000/milvus:latest ${TARGET_REPO}/milvus:${env.BRANCH_NAME}-latest
                                        docker push ${TARGET_REPO}/milvus:${env.BRANCH_NAME}-${date}-${gitShortCommit}
                                        docker push ${TARGET_REPO}/milvus:${env.BRANCH_NAME}-latest
                                    """
                                    sh 'docker logout ${DOKCER_REGISTRY_URL}'
                                }

                                dir("${env.ARTIFACTS}") {
                                    sh "find ./kind -path '*/history/*' -type f | xargs tar -zcvf artifacts-${PROJECT_NAME}-${MILVUS_SERVER_TYPE}-${SEMVER}-${env.BUILD_NUMBER}-e2e-nightly-logs.tar.gz --transform='s:^[^/]*/[^/]*/[^/]*/[^/]*/::g' || true"
                                    archiveArtifacts artifacts: "**.tar.gz", allowEmptyArchive: true
                                    sh 'rm -rf ./*'
                                    sh 'docker rm -f \$(docker network inspect -f \'{{ range \$key, \$value := .Containers }}{{ printf "%s " \$key}}{{ end }}\' kind) || true'
                                    sh 'docker network rm kind 2>&1 > /dev/null || true'
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
