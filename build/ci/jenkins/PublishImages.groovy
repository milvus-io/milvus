#!/usr/bin/env groovy

pipeline {
    agent {
        kubernetes {
            label "milvus-publish-image"
            defaultContainer 'main'
            yamlFile "build/ci/jenkins/pod/rte.yaml"
            customWorkspace '/home/jenkins/agent/workspace'
            // We allow this pod to remain active for a while, later jobs can
            // reuse cache in previous created nodes.
            // idleMinutes 120
        }
    }

    options {
        timestamps()
        timeout(time: 36, unit: 'MINUTES')
        // parallelsAlwaysFailFast()
    }

    environment {
        DOCKER_CREDENTIALS_ID = "f0aacc8e-33f2-458a-ba9e-2c44f431b4d2"
        TARGET_REPO = "milvusdb"
        CI_DOCKER_CREDENTIAL_ID = "ci-docker-registry"
        HARBOR_REPO = "registry.milvus.io"
    }

    stages {
        stage('Publish Milvus Images & Build Centos Milvus'){
            parallel {
                stage('Publish Milvus Images') {
                    steps {
                        container('main') {
                            script {
                                sh './build/set_docker_mirror.sh'
                                sh "build/builder.sh /bin/bash -c \"make install\""

                                def date = sh(returnStdout: true, script: 'date +%Y%m%d').trim()
                                def gitShortCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()

                                withCredentials([usernamePassword(credentialsId: "${env.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]) {
                                    sh 'docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD}'
                                    sh """
                                        export MILVUS_IMAGE_REPO="${env.TARGET_REPO}/milvus"
                                        export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"
                                        export MILVUS_IMAGE_TAG="${env.BRANCH_NAME}-${date}-${gitShortCommit}"
                                        build/build_image.sh
                                        docker push \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                        docker tag \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} \${MILVUS_IMAGE_REPO}:${env.BRANCH_NAME}-latest
                                        docker tag \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                        docker push \${MILVUS_IMAGE_REPO}:${env.BRANCH_NAME}-latest
                                        docker logout
                                    """
                                }

                                withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]){
                                    sh "docker login ${env.HARBOR_REPO} -u '${CI_REGISTRY_USERNAME}' -p '${CI_REGISTRY_PASSWORD}'"
                                    sh """
                                        export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"
                                        export MILVUS_IMAGE_TAG="${env.BRANCH_NAME}-${date}-${gitShortCommit}"
                                        docker push \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                        docker logout
                                    """
                                }
                            }
                        }
                    }
                }
                stage('Build Centos Milvus Image'){
                    agent {
                        kubernetes {
                            label "milvus-centos-image"
                            defaultContainer 'main'
                            yamlFile "build/ci/jenkins/pod/rte.yaml"
                            customWorkspace '/home/jenkins/agent/workspace'
                        }
                    }
                    steps {
                        container('main') {
                            script {
                                sh './build/set_docker_mirror.sh'
                                sh "OS_NAME=centos7 build/builder.sh /bin/bash -c \"make install\""
                                sh "OS_NAME=centos7 build/build_image.sh "
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
                    def authorEmail = sh returnStdout: true, script: 'git --no-pager show -s --format=\'%ae\' HEAD'
                    emailext subject: '$DEFAULT_SUBJECT',
                    body: '$DEFAULT_CONTENT',
                    recipientProviders: [developers(), culprits()],
                    replyTo: '$DEFAULT_REPLYTO',
                    to: "${authorEmail},qa@zilliz.com,devops@zilliz.com"
                }
            }
        }
        cleanup {
            container('main') {
                script {
                    sh 'find . -name . -o -prune -exec rm -rf -- {} +' /* clean up our workspace */
                }
            }
        }
    }
}
