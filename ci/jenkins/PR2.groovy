@Library('jenkins-shared-library@v0.46.0') _

def pod = libraryResource 'io/milvus/pod/tekton-4am.yaml'
def milvus_helm_chart_version = '4.2.8'

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
        stage('meta') {
            steps {
                container('jnlp') {
                    script {
                        isPr = env.CHANGE_ID != null
                        gitMode = isPr ? 'merge' : 'fetch'
                        gitBaseRef = isPr ? "$env.CHANGE_TARGET" : "$env.BRANCH_NAME"

                        get_helm_release_name =  tekton.helm_release_name client: 'py',
                                                             changeId: "${env.CHANGE_ID}",
                                                             buildId:"${env.BUILD_ID}"
                    }
                }
            }
        }
        stage('build') {
            steps {
                container('tkn') {
                    script {
                        def job_name = tekton.run arch: 'amd64',
                                              isPr: isPr,
                                              gitMode: gitMode ,
                                              gitBaseRef: gitBaseRef,
                                              pullRequestNumber: "$env.CHANGE_ID",
                                              suppress_suffix_of_image_tag: true,
                                              images: '["milvus","pytest","helm"]'

                        milvus_image_tag = tekton.query_result job_name, 'milvus-image-tag'
                        pytest_image =  tekton.query_result job_name, 'pytest-image-fqdn'
                        helm_image =  tekton.query_result job_name, 'helm-image-fqdn'
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
                        values 'standalone', 'distributed', 'standalone-kafka', 'distributed-streaming-service'
                    }
                }
                stages {
                    stage('E2E Test') {
                        steps {
                            container('tkn') {
                                script {
                                    def helm_release_name =  get_helm_release_name milvus_deployment_option

                                    if (milvus_deployment_option == 'distributed-streaming-service') {
                                        try {
                                            tekton.pytest helm_release_name: helm_release_name,
                                                    milvus_helm_version: milvus_helm_chart_version,
                                                    ciMode: 'e2e',
                                                    milvus_image_tag: milvus_image_tag,
                                                    pytest_image: pytest_image,
                                                    helm_image: helm_image,
                                                    milvus_deployment_option: milvus_deployment_option,
                                                    verbose: 'false'
                                        } catch (Exception e) {
                                            println e
                                        }
                                    } else {
                                        tekton.pytest helm_release_name: helm_release_name,
                                                    milvus_helm_version: milvus_helm_chart_version,
                                                    ciMode: 'e2e',
                                                    milvus_image_tag: milvus_image_tag,
                                                    pytest_image: pytest_image,
                                                    helm_image: helm_image,
                                                    milvus_deployment_option: milvus_deployment_option,
                                                    verbose: 'false'
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
                                        def helm_release_name =  get_helm_release_name milvus_deployment_option

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
