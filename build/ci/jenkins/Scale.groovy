
String cron_timezone = 'TZ=Asia/Shanghai'
String cron_string = BRANCH_NAME == "master" ? "30 20 * * * " : ""

int total_timeout_minutes = 60

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
            inheritFrom 'milvus-test'
            // idle 5 minutes to wait clean up tasks
            idleMinutes 5
        }
    }
    environment {
        PROJECT_NAME = "milvus"
        TEST_TYPE = "scale-test"
//        SEMVER = "${BRANCH_NAME.contains('/') ? BRANCH_NAME.substring(BRANCH_NAME.lastIndexOf('/') + 1) : BRANCH_NAME}"
        ARTIFACTS = "${env.WORKSPACE}/_artifacts"
    }

    stages {
        stage ('Install'){
            steps {
                container('milvus-test') {
                    dir ('tests/python_client'){
                        sh """
                        printenv
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
                            sh 'pytest . -n 5 -v -s'
                        }
                    }
                }
            }
        }
    }
    post {
        always {
            container('milvus-test') {
                dir ('tests/scripts') {
                    script {
                        dir("${env.ARTIFACTS}") {
                            sh "tar -zcvf artifacts-${PROJECT_NAME}-${TEST_TYPE}-pytest-logs.tar.gz /tmp/ci_logs --remove-files || true"
                            archiveArtifacts artifacts: "artifacts-${PROJECT_NAME}-${TEST_TYPE}-pytest-logs.tar.gz ", allowEmptyArchive: true                        }
                    }
                }
            }
        }
    }
}