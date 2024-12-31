@Library('jenkins-shared-library@tekton') _

def pod = libraryResource 'io/milvus/pod/tekton-4am.yaml'
def milvus_helm_chart_version = '4.2.8'

pipeline {
    options {
        skipDefaultCheckout true
        parallelsAlwaysFailFast()
        buildDiscarder logRotator(artifactDaysToKeepStr: '30')
        preserveStashes(buildCount: 5)
        // abort previous build if it's a PR, otherwise queue the build
        disableConcurrentBuilds(abortPrevious: env.CHANGE_ID != null)
        timeout(time: 6, unit: 'HOURS')
        throttleJobProperty(
            categories: ['cpp-unit-test'],
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
                    }
                }
            }
        }
        stage('build & test') {
            steps {
                container('tkn') {
                    script {
                        def job_name = tekton.cpp_ut arch: 'amd64',
                                              isPr: isPr,
                                              gitMode: gitMode ,
                                              gitBaseRef: gitBaseRef,
                                              pullRequestNumber: "$env.CHANGE_ID",
                                              make_cmd: "make clean && make USE_ASAN=ON build-cpp-with-coverage",
                                              test_entrypoint: "./scripts/run_cpp_codecov.sh",
                                              codecov_report_name: "cpp-unit-test",
                                              codecov_files: "./lcov_output.info",
                                              tekton_pipeline_timeout: '3h'
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
    }
}
