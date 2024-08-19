@Library('jenkins-shared-library@v0.6.14') _

def store = new java.util.concurrent.ConcurrentHashMap<String, Boolean>()

def pod = libraryResource 'io/milvus/pod/tekton-4am.yaml'
def output = [:]

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
                container('kubectl') {
                    script {
                        isPr = env.CHANGE_ID != null
                        gitMode = isPr ? 'merge' : 'fetch'
                        gitBaseRef = isPr ? "$env.CHANGE_TARGET" : "$env.BRANCH_NAME"

                        job_name = tekton.run arch: 'amd64',
                                              isPr: isPr,
                                              gitMode: gitMode ,
                                              gitBaseRef: gitBaseRef,
                                              pullRequestNumber: "$env.CHANGE_ID",
                                              suppress_suffix_of_image_tag: true
                    }
                }

                container('tkn') {
                    script {
                        try {
                            tekton.print_log(job_name)
                        } catch (Exception e) {
                            println e
                        }

                        tekton.check_result(job_name)
                        milvus_image_tag = tekton.query_result job_name, 'milvus-image-tag'
                        milvus_sdk_go_image =  tekton.query_result job_name, 'gotestsum-image-fqdn'
                    }
                }
            }
            post {
                always {
                    container('tkn') {
                        script {
                                tekton.sure_stop(job_name)
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
                        values 'standalone', 'distributed'
                    }
                }
                stages {
                    stage('E2E Test') {
                        steps {
                            container('kubectl') {
                                script {
                                    def helm_release_name =  tekton.release_name milvus_deployment_option: milvus_deployment_option,
                                                                             changeId: "${env.CHANGE_ID}",
                                                                             buildId:"${env.BUILD_ID}"

                                    job_name = tekton.test helm_release_name: helm_release_name,
                                              milvus_image_tag: milvus_image_tag,
                                              milvus_sdk_go_image: milvus_sdk_go_image,
                                              milvus_deployment_option: milvus_deployment_option

                                    store["${milvus_deployment_option}"] = job_name
                                }
                            }

                            container('tkn') {
                                script {
                                    def job_name = store["${milvus_deployment_option}"]
                                    try {
                                        tekton.print_log(job_name)
                                    } catch (Exception e) {
                                        println e
                                    }

                                    tekton.check_result(job_name)
                                }
                            }
                        }

                        post {
                            always {
                                    container('tkn') {
                                        script {
                                        def job_name = store["${milvus_deployment_option}"]
                                            tekton.sure_stop(job_name)
                                        }
                                    }

                                    container('archive') {
                                        script {
                                            def helm_release_name =  tekton.release_name milvus_deployment_option: milvus_deployment_option,
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

