@Library('jenkins-shared-library@tekton') _

def pod = libraryResource 'io/milvus/pod/tekton-4am.yaml'
def milvus_helm_chart_version = '4.2.48'

pipeline {
    options {
        skipDefaultCheckout true
        parallelsAlwaysFailFast()
        buildDiscarder logRotator(artifactDaysToKeepStr: '30')
        preserveStashes(buildCount: 5)
        disableConcurrentBuilds(abortPrevious: true)
        timeout(time: 3, unit: 'HOURS')
        throttleJobProperty(
            categories: ['cpu-e2e'],
            throttleEnabled: true,
            throttleOption: 'category'

        )
    }

    environment {
        LOKI_ADDR = 'http://loki-1-loki-distributed-gateway.loki.svc.cluster.local'
        LOKI_CLIENT_RETRIES = 3
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
                                              make_cmd: 'make clean && make install USE_ASAN=ON use_disk_index=ON',
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
                        // 'milvus' template defined a ephemeral volume used for pytest result archiving
                        // pvc name would be <pod-name>-volume-0
                        inheritFrom 'milvus'
                        yaml pod
                    }
                }
                axes {
                    axis {
                        name 'milvus_deployment_option'
                        values 'standalone', 'distributed-pulsar', 'standalone-kafka-mmap'
                    }
                }
                stages {
                    stage('E2E Test') {
                        steps {
                            container('tkn') {
                                script {
                                    def helm_release_name =  get_helm_release_name milvus_deployment_option
                                    // pvc name would be <pod-name>-volume-0, used for pytest result archiving
                                    def pvc = env.JENKINS_AGENT_NAME + '-volume-0'
                                    tekton.pytest helm_release_name: helm_release_name,
                                                pvc: pvc,
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
                                container('jnlp') {
                                    script {
                                        tekton.archive_pytest_logs(milvus_deployment_option)
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
