#!/usr/bin/env groovy

pipeline {
    agent {
        kubernetes {
            cloud '4am'
            defaultContainer 'main'
            yamlFile "ci/jenkins/pod/rte-arm.yaml"
            customWorkspace '/home/jenkins/agent/workspace'
            // We allow this pod to remain active for a while, later jobs can
            // reuse cache in previous created nodes.
            // idleMinutes 120
        }
    }
    parameters {
        string(name: 'image-tag', defaultValue: '', description: 'the image tag to be pushed to image registry')
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
        stage('Publish Milvus cpu Images'){

            steps {
                    script {
                        sh """
                            git config --global --add safe.directory /home/jenkins/agent/workspace
                        """

                        def tag = ""
                        if (params['image-tag'] == '') {
                            def date = sh(returnStdout: true, script: 'date +%Y%m%d').trim()
                            def gitShortCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
                            tag = "${env.BRANCH_NAME}-${date}-${gitShortCommit}-arm"
                        }else{
                            tag = params['image-tag']
                        }

                        sh """
                        echo "Building image with tag: ${tag}"

                        set -a  # automatically export all variables from .env
                        . .env
                        set +a  # stop automatically


                        docker run --net=host -v /root/.conan:/root/.conan  -v \$(pwd):/root/milvus -w /root/milvus milvusdb/milvus-env:ubuntu20.04-\${DATE_VERSION} sh -c "make clean && make install"
                        """


                        withCredentials([usernamePassword(credentialsId: "${env.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]) {
                            sh 'docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD}'
                            sh """
                                export MILVUS_IMAGE_REPO="${env.TARGET_REPO}/milvus"
                                export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"
                                export MILVUS_IMAGE_TAG="${tag}"

                                docker build --build-arg TARGETARCH=arm64  -f "./build/docker/milvus/ubuntu20.04/Dockerfile" -t \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} .

                                docker push \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                docker tag \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                docker logout
                            """
                        }

                        withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]){
                            sh "docker login ${env.HARBOR_REPO} -u '${CI_REGISTRY_USERNAME}' -p '${CI_REGISTRY_PASSWORD}'"
                            sh """
                                export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"
                                export MILVUS_IMAGE_TAG="${tag}"
                                docker push \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                docker logout
                            """
                        }
                    }
            }
        }
    }

}
