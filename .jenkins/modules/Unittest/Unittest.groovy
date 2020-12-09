timeout(time: 15, unit: 'MINUTES') {
    dir ("ci/scripts") {
        String formatName = "${BINARY_VERSION}-version-${OS_NAME}-unittest";
        String formatFlag = "${formatName}".replaceAll("\\.", "_").replaceAll("-", "_")
        String coverageInfoName = "milvus_output.info"
        def isTimeTriggeredBuild = currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause').size() != 0
        sh ". ./before-install.sh && ./run_unittest.sh -i ${env.MILVUS_INSTALL_PREFIX} -o ${coverageInfoName}"
        if (isTimeTriggeredBuild) {
            withCredentials([[$class: 'StringBinding', credentialsId: "milvus-ci-codecov-token", variable: 'CODECOV_TOKEN']]) {
                sh "curl -s https://codecov.io/bash | bash -s - -f ${coverageInfoName} -n ${formatName} -F nightly -F ${formatFlag} || echo \"Codecov did not collect coverage reports\""
            }
        } else {
            withCredentials([[$class: 'StringBinding', credentialsId: "milvus-ci-codecov-token", variable: 'CODECOV_TOKEN']]) {
                sh "curl -s https://codecov.io/bash | bash -s - -f ${coverageInfoName} -n ${formatName} -F ${formatFlag} || echo \"Codecov did not collect coverage reports\""
            }
        }
    }
}
