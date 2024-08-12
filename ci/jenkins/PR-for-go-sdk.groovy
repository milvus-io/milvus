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
                            container('main') {
                                script{
                                      sh 'sleep 3600'
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
