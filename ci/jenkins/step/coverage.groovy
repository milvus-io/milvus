timeout(time: 30, unit: 'MINUTES') {
    dir ("ci/scripts") {
        sh "./coverage.sh"
        boolean isNightlyTest = currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause').size() != 0 ? true : false
        String formatFlag = "${BINARY_VERSION}-version-${OS_NAME}-unittest".replaceAll("\\.", "_").replaceAll("-", "_")
        if (isNightlyTest) {
            withCredentials([[$class: 'StringBinding', credentialsId: "milvus-ci-codecov-token", variable: 'CODECOV_TOKEN']]) {
                sh "curl -s https://codecov.io/bash | bash -s - -f output_new.info -n ${BINARY_VERSION}-version-${OS_NAME}-unittest -F nightly -F ${formatFlag} || echo \"Codecov did not collect coverage reports\""
            }
        } else {
            withCredentials([[$class: 'StringBinding', credentialsId: "milvus-ci-codecov-token", variable: 'CODECOV_TOKEN']]) {
                sh "curl -s https://codecov.io/bash | bash -s - -f output_new.info -n ${BINARY_VERSION}-version-${OS_NAME}-unittest -F ${formatFlag} || echo \"Codecov did not collect coverage reports\""
            }
        }
    }
}
