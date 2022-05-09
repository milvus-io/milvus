
String cron_timezone = 'TZ=Asia/Shanghai'
String cron_string = BRANCH_NAME == "master" ? "05 21 * * * " : ""

int total_timeout_minutes = 90

// pipeline
pipeline {
    triggers {
        cron """${cron_timezone}
            ${cron_string}"""
    }
    options {
        timestamps()
        timeout(time: total_timeout_minutes, unit: 'MINUTES')
    }

    agent {
        kubernetes {
            label 'milvus-scale-test'
//             inheritFrom 'milvus-test'
            defaultContainer 'milvus-test'
            yamlFile "build/ci/jenkins/pod/scale-test.yaml"
            customWorkspace "/home/jenkins/agent"
            // idle 5 minutes to wait clean up tasks
            idleMinutes 5
        }
    }
    environment {
        PROJECT_NAME = "milvus"
        TEST_TYPE = "scale-test"
//        SEMVER = "${BRANCH_NAME.contains('/') ? BRANCH_NAME.substring(BRANCH_NAME.lastIndexOf('/') + 1) : BRANCH_NAME}"
        ARTIFACTS = "${env.WORKSPACE}/_artifacts"
        MILVUS_LOGS = "/tmp/milvus_logs/*"
    }

    stages {
        stage ('Install'){
            steps {
                container('milvus-test') {
                    dir ('tests/python_client'){
                        sh """
                        pip install -r requirements.txt
                        pip install --upgrade protobuf
                        """
                    }
                }
            }
        }
        stage ('Scale Test') {
            steps {
                container('milvus-test') {
                    dir ('tests/python_client/scale') {
                        script {
                            // pytest run scale case in parallel
                            sh 'pytest . -n 5 -v -s'
                        }
                    }
                }
            }
        }
    }
    post {
        unsuccessful {
            container ('jnlp') {
                script {
                    emailext subject: '$DEFAULT_SUBJECT',
                    body: '$DEFAULT_CONTENT',
//                     recipientProviders: [requestor()],
//                     replyTo: '$DEFAULT_REPLYTO',
                    to: 'qa@zilliz.com'
                }
            }
        }
        always {
            container('milvus-test') {
                dir ('tests/scripts') {
                    script {
                        dir("${env.ARTIFACTS}") {
                            sh "tar -zcvf artifacts-${PROJECT_NAME}-${TEST_TYPE}-pytest-logs.tar.gz /tmp/ci_logs --remove-files || true"
                            archiveArtifacts artifacts: "artifacts-${PROJECT_NAME}-${TEST_TYPE}-pytest-logs.tar.gz ", allowEmptyArchive: true
                            DIR_LIST = sh(returnStdout: true, script: 'ls -d1 ${MILVUS_LOGS}').trim()
                            for (d in DIR_LIST.tokenize("\n")) {
                                sh "echo $d"
                                def release_name = d.split('/')[-1]
                                sh "tar -zcvf artifacts-${PROJECT_NAME}-${TEST_TYPE}-${release_name}-logs.tar.gz ${d} --remove-files || true"
                                archiveArtifacts artifacts: "artifacts-${PROJECT_NAME}-${TEST_TYPE}-${release_name}-logs.tar.gz ", allowEmptyArchive: true

                            }
                        }
                    }
                }
            }
        }
    }
}