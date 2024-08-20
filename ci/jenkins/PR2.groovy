@Library('jenkins-shared-library@v0.27.0') _

def pod = libraryResource 'io/milvus/pod/tekton-4am.yaml'
def milvus_helm_chart_version = '4.1.27'

pipeline {
    options {
        skipDefaultCheckout true
        parallelsAlwaysFailFast()
        buildDiscarder logRotator(artifactDaysToKeepStr: '30')
        preserveStashes(buildCount: 5)
        disableConcurrentBuilds(abortPrevious: true)
    }
    agent {
        kubernetes {
            cloud '4am'
            yaml pod
        }
    }
    stages {
        stage('build') {
            steps {
                container('tkn') {
                    script {
                        isPr = env.CHANGE_ID != null
                        gitMode = isPr ? 'merge' : 'fetch'
                        gitBaseRef = isPr ? "$env.CHANGE_TARGET" : "$env.BRANCH_NAME"

                        job_name = tekton.run arch: 'amd64',
                                              isPr: isPr,
                                              gitMode: gitMode ,
                                              gitBaseRef: gitBaseRef,
                                              pullRequestNumber: "$env.CHANGE_ID",
                                              suppress_suffix_of_image_tag: true,
                                              test_client_type: '["pytest"]'

                        milvus_image_tag = tekton.query_result job_name, 'milvus-image-tag'
                        pytest_image =  tekton.query_result job_name, 'pytest-image-fqdn'
                    }
                }
            }
            post {
                always {
                    container('tkn') {
                        script {
                            tekton.sure_stop()
                        }
                    }
                }
            }
        }
        stage('E2E Test') {
            matrix {
                agent {
                    kubernetes {
                        cloud '4am'
                        yaml pod
                    }
                }
                axes {
                    axis {
                        name 'milvus_deployment_option'
                        values 'standalone', 'distributed', 'standalone-kafka'
                    }
                }
                stages {
                    stage('E2E Test') {
                        steps {
                            container('tkn') {
                                script {
                                    def helm_release_name =  tekton.release_name milvus_deployment_option: milvus_deployment_option,
                                                                             client: 'py',
                                                                             changeId: "${env.CHANGE_ID}",
                                                                             buildId:"${env.BUILD_ID}"

                                    if (milvus_deployment_option == 'distributed-streaming-service') {
                                        try {
                                            tekton.pytest helm_release_name: helm_release_name,
                                                    milvus_helm_version: milvus_helm_chart_version,
                                                    ciMode: 'e2e',
                                                    milvus_image_tag: milvus_image_tag,
                                                    pytest_image: pytest_image,
                                                    milvus_deployment_option: milvus_deployment_option
                                        } catch (Exception e) {
                                            println e
                                        }
                                    } else {
                                        tekton.pytest helm_release_name: helm_release_name,
                                                    milvus_helm_version: milvus_helm_chart_version,
                                                    ciMode: 'e2e',
                                                    milvus_image_tag: milvus_image_tag,
                                                    pytest_image: pytest_image,
                                                    milvus_deployment_option: milvus_deployment_option
                                    }
                                }
                            }
                        }

                        post {
                            always {
                                container('tkn') {
                                    script {
                                        tekton.sure_stop()
                                    }
                                }

                                container('archive') {
                                    script {
                                        def helm_release_name =  tekton.release_name milvus_deployment_option: milvus_deployment_option,
                                                                                 client: 'py',
                                                                                 changeId: "${env.CHANGE_ID}",
                                                                                 buildId:"${env.BUILD_ID}"

                                        tekton.archive  milvus_deployment_option: milvus_deployment_option,
                                                                    release_name: helm_release_name ,
                                                                     change_id: env.CHANGE_ID,
                                                                     build_id: env.BUILD_ID
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
