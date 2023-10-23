#!/usr/bin/env groovy

pipeline {
    agent {
        kubernetes {
            defaultContainer 'main'
            yamlFile "ci/jenkins/pod/rte.yaml"
            customWorkspace '/home/jenkins/agent/workspace'
            // We allow this pod to remain active for a while, later jobs can
            // reuse cache in previous created nodes.
            // idleMinutes 120
        }
    }

    options {
        timestamps()
        timeout(time: 100, unit: 'MINUTES')
        // parallelsAlwaysFailFast()
        disableConcurrentBuilds()
    }

    environment {
        DOCKER_CREDENTIALS_ID = "dockerhub"
        TARGET_REPO = "milvusdb"
        CI_DOCKER_CREDENTIAL_ID = "harbor-milvus-io-registry"
        HARBOR_REPO = "harbor.milvus.io"
    }

    stages {
        stage('Generate Image Tag') {
            steps {
                script {
                    def date = sh(returnStdout: true, script: 'date +%Y%m%d').trim()
                    def gitShortCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
                    def imageTag = "${env.BRANCH_NAME}-${date}-${gitShortCommit}"
                    sh "echo ${imageTag} > imageTag.txt"
                    stash includes: 'imageTag.txt', name: 'imageTag'
                }
            }
        }

        stage('Build & Publish Milvus Images'){
            parallel {
                stage('Build Milvus Images on amd') {
                    steps {
                        container('main') {
                            script {
                                sh './build/set_docker_mirror.sh'
                                sh "build/builder.sh /bin/bash -c \"make install\""

                                dir ("imageTag"){
                                    try{
                                        unstash 'imageTag'
                                        imageTag=sh(returnStdout: true, script: 'cat imageTag.txt | tr -d \'\n\r\'')
                                    }catch(e){
                                        print "No Image Tag info remained ,please rerun build to build new image."
                                        exit 1
                                    }
                                }

                                withCredentials([usernamePassword(credentialsId: "${env.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]){
                                    sh "docker login -u '${DOCKER_USERNAME}' -p '${DOCKER_PASSWORD}'"
                                    sh """
                                        export MILVUS_IMAGE_REPO="${env.TARGET_REPO}/milvus"
                                        export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"
                                        export MILVUS_IMAGE_TAG="${imageTag}-amd64"
                                        build/build_image.sh
                                        docker push \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                        docker logout
                                    """
                                }


                                withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]){
                                    sh "docker login ${env.HARBOR_REPO} -u '${CI_REGISTRY_USERNAME}' -p '${CI_REGISTRY_PASSWORD}'"
                                    sh """
                                        export MILVUS_IMAGE_REPO="${env.TARGET_REPO}/milvus"
                                        export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"
                                        export MILVUS_IMAGE_TAG="${imageTag}-amd64"
                                        docker tag \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                        docker push \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                        docker logout
                                    """
                                }
                            }
                        }
                    }
                }


                stage('Build Milvus Images on arm') {
                    agent {
                        label 'arm'
                    }
                    steps {
                        script {
                            sh """
                            cp -r /tmp/krte/cache/.docker .
                            PLATFORM_ARCH="arm64" build/builder.sh /bin/bash -c "make install"
                            """

                            dir ("imageTag"){
                                try{
                                    unstash 'imageTag'
                                    imageTag=sh(returnStdout: true, script: 'cat imageTag.txt | tr -d \'\n\r\'')
                                }catch(e){
                                    print "No Image Tag info remained ,please rerun build to build new image."
                                    exit 1
                                }
                            }

                            withCredentials([usernamePassword(credentialsId: "${env.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]){
                                sh "docker login -u '${DOCKER_USERNAME}' -p '${DOCKER_PASSWORD}'"
                                sh """
                                    export MILVUS_IMAGE_REPO="${env.TARGET_REPO}/milvus"
                                    export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"
                                    export MILVUS_IMAGE_TAG="${imageTag}-arm64"
                                    BUILD_ARGS="--build-arg TARGETARCH=arm64" build/build_image.sh
                                    docker push \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                    docker logout
                                """
                            }


                            withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]){
                                sh "docker login ${env.HARBOR_REPO} -u '${CI_REGISTRY_USERNAME}' -p '${CI_REGISTRY_PASSWORD}'"
                                sh """
                                    export MILVUS_IMAGE_REPO="${env.TARGET_REPO}/milvus"
                                    export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"
                                    export MILVUS_IMAGE_TAG="${imageTag}-arm64"
                                    docker tag \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                    docker push \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                                    docker logout
                                    docker rmi \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} -f
                                    docker rmi \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} -f
                                """
                            }
                        }
                    }
                    post {
                        always {
                            script {
                                // if (currentBuild.currentResult == "SUCCESS") {
                                //     sh "cp -r .docker /tmp/krte/cache/"
                                // }
                                sh """
                                pwd
                                sudo rm -rf .env .docker
                                sudo rm -rf *
                                """
                            }
                        }
                    }
                }

            }
        }
        stage ('publish multi-platform image') {
            steps {
                script {
                    dir ("imageTag"){
                        try{
                            unstash 'imageTag'
                            imageTag=sh(returnStdout: true, script: 'cat imageTag.txt | tr -d \'\n\r\'')
                        }catch(e){
                            print "No Image Tag info remained ,please rerun build to build new image."
                            exit 1
                        }
                    }

                    withCredentials([usernamePassword(credentialsId: "${env.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]) {
                        sh 'docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD}'
                        sh """
                            export MILVUS_IMAGE_REPO="${env.TARGET_REPO}/milvus"
                            export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"

                            export ARM_MILVUS_IMAGE_TAG="${imageTag}-arm64"
                            export AMD_MILVUS_IMAGE_TAG="${imageTag}-amd64"
                            export MILVUS_IMAGE_TAG="${imageTag}"

                            docker pull \${MILVUS_HARBOR_IMAGE_REPO}:\${ARM_MILVUS_IMAGE_TAG}
                            docker tag \${MILVUS_HARBOR_IMAGE_REPO}:\${ARM_MILVUS_IMAGE_TAG} \${MILVUS_IMAGE_REPO}:\${ARM_MILVUS_IMAGE_TAG}


                            docker manifest create \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} \${MILVUS_IMAGE_REPO}:\${AMD_MILVUS_IMAGE_TAG} \${MILVUS_IMAGE_REPO}:\${ARM_MILVUS_IMAGE_TAG}
                            docker manifest annotate \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} \${MILVUS_IMAGE_REPO}:\${AMD_MILVUS_IMAGE_TAG} --os linux --arch amd64
                            docker manifest annotate \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} \${MILVUS_IMAGE_REPO}:\${ARM_MILVUS_IMAGE_TAG} --os linux --arch arm64
                            docker manifest push \${MILVUS_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}

                            docker manifest create \${MILVUS_IMAGE_REPO}:${env.BRANCH_NAME}-latest \${MILVUS_IMAGE_REPO}:\${AMD_MILVUS_IMAGE_TAG} \${MILVUS_IMAGE_REPO}:\${ARM_MILVUS_IMAGE_TAG}
                            docker manifest annotate \${MILVUS_IMAGE_REPO}:${env.BRANCH_NAME}-latest \${MILVUS_IMAGE_REPO}:\${AMD_MILVUS_IMAGE_TAG} --os linux --arch amd64
                            docker manifest annotate \${MILVUS_IMAGE_REPO}:${env.BRANCH_NAME}-latest \${MILVUS_IMAGE_REPO}:\${ARM_MILVUS_IMAGE_TAG} --os linux --arch arm64
                            docker manifest push \${MILVUS_IMAGE_REPO}:${env.BRANCH_NAME}-latest

                            docker logout
                        """
                    }

                    withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]){
                        sh """
                        docker login ${env.HARBOR_REPO} -u '${CI_REGISTRY_USERNAME}' -p '${CI_REGISTRY_PASSWORD}'

                        export MILVUS_IMAGE_REPO="${env.TARGET_REPO}/milvus"
                        export MILVUS_HARBOR_IMAGE_REPO="${env.HARBOR_REPO}/milvus/milvus"

                        export ARM_MILVUS_IMAGE_TAG="${imageTag}-arm64"
                        export AMD_MILVUS_IMAGE_TAG="${imageTag}-amd64"
                        export MILVUS_IMAGE_TAG="${imageTag}"


                        docker manifest create \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} \${MILVUS_HARBOR_IMAGE_REPO}:\${AMD_MILVUS_IMAGE_TAG} \${MILVUS_HARBOR_IMAGE_REPO}:\${ARM_MILVUS_IMAGE_TAG}
                        docker manifest annotate \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} \${MILVUS_HARBOR_IMAGE_REPO}:\${AMD_MILVUS_IMAGE_TAG} --os linux --arch amd64
                        docker manifest annotate \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG} \${MILVUS_HARBOR_IMAGE_REPO}:\${ARM_MILVUS_IMAGE_TAG} --os linux --arch arm64

                        docker manifest push \${MILVUS_HARBOR_IMAGE_REPO}:\${MILVUS_IMAGE_TAG}
                        """
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
