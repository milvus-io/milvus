timeout(time: 10, unit: 'MINUTES') {
    dir ("ci/scripts") {
        sh ". ./before-install.sh && ./coverage.sh"
        String formatFlag = "${BINARY_VERSION}-version-${OS_NAME}-unittest".replaceAll("\\.", "_").replaceAll("-", "_")
        if (isTimeTriggeredBuild()) {
            withCredentials([[$class: 'StringBinding', credentialsId: "milvus-ci-codecov-token", variable: 'CODECOV_TOKEN']]) {
                sh "curl -s https://codecov.io/bash | bash -s - -f output_new.info -U \"--proxy http://proxy.zilliz.tech:1088\" -A \"--proxy http://proxy.zilliz.tech:1088\" -n ${BINARY_VERSION}-version-${OS_NAME}-unittest -F nightly -F ${formatFlag} || echo \"Codecov did not collect coverage reports\""
            }
        } else {
            withCredentials([[$class: 'StringBinding', credentialsId: "milvus-ci-codecov-token", variable: 'CODECOV_TOKEN']]) {
                sh "curl -s https://codecov.io/bash | bash -s - -f output_new.info -U \"--proxy http://proxy.zilliz.tech:1088\" -A \"--proxy http://proxy.zilliz.tech:1088\" -n ${BINARY_VERSION}-version-${OS_NAME}-unittest -F ${formatFlag} || echo \"Codecov did not collect coverage reports\""
            }
        }
    }
}

boolean isTimeTriggeredBuild() {
    return (currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause').size() != 0) ? true : false;
}
