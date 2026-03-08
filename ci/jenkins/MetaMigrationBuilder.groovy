#!/usr/bin/env groovy
def app="meta-migration-builder"
def date=""
def gitShortCommit=""
pipeline {
    agent {
        kubernetes {
            defaultContainer 'main'
            yamlFile "ci/jenkins/pod/meta-builder.yaml"
            customWorkspace '/home/jenkins/agent/workspace'
        }
    }

    options {
        timestamps()
        timeout(time: 36, unit: 'MINUTES')
        disableConcurrentBuilds(abortPrevious: true)
    }

    environment {
        HARBOR_REPO = "harbor.milvus.io"
        CI_DOCKER_CREDENTIAL_ID="harbor-milvus-io-registry"
    }

    stages {
        stage('Publish Meta Migration builder Images') {
            steps {
                container('main'){
                    script{
                        date=sh(returnStdout: true, script: 'date +%Y%m%d').trim()
                        gitShortCommit=sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
                        sh './build/set_docker_mirror.sh'
                        def tag="${date}-${gitShortCommit}"
                        def image="${env.HARBOR_REPO}/milvus/${app}:${tag}"
                        withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]){
                                sh "docker login ${env.HARBOR_REPO} -u '${CI_REGISTRY_USERNAME}' -p '${CI_REGISTRY_PASSWORD}'"
                                sh """
                                    docker build -t  ${image} -f build/docker/meta-migration/builder/Dockerfile .
                                    docker push ${image}
                                    docker logout
                                """
                            }
                        }
                }
            }
        }
    }
}
