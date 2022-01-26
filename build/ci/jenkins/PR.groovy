#!/usr/bin/env groovy

int total_timeout_minutes = 120
int e2e_timeout_seconds = 70 * 60
def imageTag=''
int case_timeout_seconds = 10 * 60
def chart_version='3.0.0'
pipeline {
    options {
        timestamps()
        timeout(time: total_timeout_minutes, unit: 'MINUTES')
        buildDiscarder logRotator(artifactDaysToKeepStr: '30')
        parallelsAlwaysFailFast()
        preserveStashes(buildCount: 5)

    }
    agent {
            kubernetes {
                label 'milvus-e2e-test-pipeline'
                inheritFrom 'default'
                defaultContainer 'main'
                yamlFile 'build/ci/jenkins/pod/rte.yaml'
                customWorkspace '/home/jenkins/agent/workspace'
            }
    }
    environment {
        PROJECT_NAME = 'milvus'
        SEMVER = "${BRANCH_NAME.contains('/') ? BRANCH_NAME.substring(BRANCH_NAME.lastIndexOf('/') + 1) : BRANCH_NAME}"
        DOCKER_BUILDKIT = 1
        ARTIFACTS = "${env.WORKSPACE}/_artifacts"
        DOCKER_CREDENTIALS_ID = "f0aacc8e-33f2-458a-ba9e-2c44f431b4d2"
        TARGET_REPO = "milvusdb"
        CI_DOCKER_CREDENTIAL_ID = "ci-docker-registry"
        MILVUS_HELM_NAMESPACE = "milvus-ci"
        DISABLE_KIND = true
        HUB = 'registry.milvus.io/milvus'
        JENKINS_BUILD_ID = "${env.BUILD_ID}"
        CI_MODE="pr"
    }

    stages {
        stage ('Build'){
            steps {
                container('main') {
                    dir ('build'){
                            sh './set_docker_mirror.sh'
                    }
                    dir ('tests/scripts') {
                        script {
                            sh 'printenv'
                            def date = sh(returnStdout: true, script: 'date +%Y%m%d').trim()
                            def gitShortCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()    
                            imageTag="${env.BRANCH_NAME}-${date}-${gitShortCommit}"
                            withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]){
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
                        values 'pymilvus'
                    }
                }

                stages {
                    stage('Install') {
                        steps {
                            container('main') {
                                dir ('tests/scripts') {
                                    script {
                                        sh 'printenv'
                                        def clusterEnabled = "false"
                                        if ("${MILVUS_SERVER_TYPE}" == 'distributed') {
                                            clusterEnabled = "true"
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
                                            withCredentials([usernamePassword(credentialsId: "${env.CI_DOCKER_CREDENTIAL_ID}", usernameVariable: 'CI_REGISTRY_USERNAME', passwordVariable: 'CI_REGISTRY_PASSWORD')]){
                                                sh """
                                                MILVUS_CLUSTER_ENABLED=${clusterEnabled} \
                                                TAG=${imageTag}\
                                                ./e2e-k8s.sh \
                                                --skip-export-logs \
                                                --skip-cleanup \
                                                --skip-setup \
                                                --skip-test \
                                                --skip-build \
                                                --skip-build-image \
                                                --install-extra-arg "--set etcd.persistence.storageClass=local-path \
                                                --set minio.persistence.storageClass=local-path \
                                                --set etcd.metrics.enabled=true \
                                                --set etcd.metrics.podMonitor.enabled=true \
                                                --set etcd.nodeSelector.disk=fast \
                                                --set metrics.serviceMonitor.enabled=true \
                                                --version ${chart_version} \
                                                -f values/pr.yaml" 
                                                """
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
                        agent {
                                kubernetes {
                                    label 'milvus-e2e-test-pr'
                                    inheritFrom 'default'
                                    defaultContainer 'main'
                                    yamlFile 'build/ci/jenkins/pod/rte.yaml'
                                    customWorkspace '/home/jenkins/agent/workspace'
                                }
                        }
                        steps {
                            container('pytest') {
                                dir ('tests/scripts') {
                                    script {
                                        def release_name=sh(returnStdout: true, script: './get_release_name.sh')
                                        def clusterEnabled = 'false'
                                        if ("${MILVUS_SERVER_TYPE}" == "distributed") {
                                            clusterEnabled = "true"
                                        }
                                        if ("${MILVUS_CLIENT}" == "pymilvus") {
                                            sh """
                                            MILVUS_HELM_RELEASE_NAME="${release_name}" \
                                            MILVUS_HELM_NAMESPACE="milvus-ci" \
                                            MILVUS_CLUSTER_ENABLED="${clusterEnabled}" \
                                            TEST_TIMEOUT="${e2e_timeout_seconds}" \
                                            ./ci_e2e.sh  "-n 6 -x --tags L0 L1 --timeout ${case_timeout_seconds}"
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
                post{
                    always {
                        container('pytest'){
                            dir("${env.ARTIFACTS}") {
                                    sh "tar -zcvf artifacts-${PROJECT_NAME}-${MILVUS_SERVER_TYPE}-${MILVUS_CLIENT}-pytest-logs.tar.gz /tmp/ci_logs/test --remove-files || true"
                                    archiveArtifacts artifacts: "artifacts-${PROJECT_NAME}-${MILVUS_SERVER_TYPE}-${MILVUS_CLIENT}-pytest-logs.tar.gz ", allowEmptyArchive: true
                            }
                        }
                        container('main') {
                            dir ('tests/scripts') {  
                                script {
                                    def release_name=sh(returnStdout: true, script: './get_release_name.sh')
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