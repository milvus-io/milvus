#!/usr/bin/env groovy

pipeline {
    agent {
        label 'arm'
    }

    options {
        timestamps()
        timeout(time: 300, unit: 'MINUTES')
        // parallelsAlwaysFailFast()
        disableConcurrentBuilds()
    }

    environment {
        DOCKER_CREDENTIALS_ID = "dockerhub"
        DOCKER_BUILDKIT = 1
        TARGET_REPO = "milvusdb"
        CI_DOCKER_CREDENTIAL_ID = "harbor-milvus-io-registry"
        HARBOR_REPO = "harbor.milvus.io"
    }

    stages {
        stage('Publish Milvus GPU Images'){

            steps {
                    script {
                        sh """
                        set -a  # automatically export all variables from .env
                        . ${WORKSPACE}/.env
                        set +a  # stop automatically

                        docker run -v \$(pwd):/root/milvus -v \$(pwd)/.docker/.conan:/root/.conan -w /root/milvus milvusdb/milvus-env:gpu-ubuntu22.04-\${GPU_DATE_VERSION} sh -c "make clean && make gpu-install"
                        """

                        def date = sh(returnStdout: true, script: 'date +%Y%m%d').trim()
                        def gitShortCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()

                        withCredentials([usernamePassword(credentialsId: "${env.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]) {
                            sh 'docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD}'
                            sh """
                                export MILVUS_IMAGE_REPO="${env.TARGET_REPO}/milvus"
                                export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"
                                export MILVUS_IMAGE_TAG="${env.BRANCH_NAME}-${date}-${gitShortCommit}-gpu-arm"

                                docker build --build-arg TARGETARCH=arm64  -f "./build/docker/milvus/gpu/ubuntu22.04/Dockerfile" -t \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} .

                                docker push \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                docker tag \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                docker logout
                            """
                        }

                        withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]){
                            sh "docker login ${env.HARBOR_REPO} -u '${CI_REGISTRY_USERNAME}' -p '${CI_REGISTRY_PASSWORD}'"
                            sh """
                                export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"
                                export MILVUS_IMAGE_TAG="${env.BRANCH_NAME}-${date}-${gitShortCommit}-gpu-arm"
                                docker push \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                docker logout
                            """
                        }
                    }
            }
        }
    }

}
