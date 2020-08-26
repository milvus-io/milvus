timeout(time: 15, unit: 'MINUTES') {
    dir ("ci/scripts") {
        String formatFlag = "${BINARY_VERSION}-version-${OS_NAME}-unittest".replaceAll("\\.", "_").replaceAll("-", "_")
        String coverageInfoName = "milvus_output.info"
        String proxyAddr = "http://proxy.zilliz.tech:1088"
        def isTimeTriggeredBuild = currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause').size() != 0
        sh "./before-install.sh && ./run_unittest.sh -i ${env.MILVUS_INSTALL_PREFIX} -o ${coverageInfoName}"
        if (isTimeTriggeredBuild) {
            withCredentials([[$class: 'StringBinding', credentialsId: "milvus-ci-codecov-token", variable: 'CODECOV_TOKEN']]) {
                sh "curl -s https://codecov.io/bash | bash -s - -f ${coverageInfoName} -U \"--proxy ${proxyAddr}\" -A \"--proxy ${proxyAddr}\" -n ${BINARY_VERSION}-version-${OS_NAME}-unittest -F nightly -F ${formatFlag} || echo \"Codecov did not collect coverage reports\""
            }
        } else {
            withCredentials([[$class: 'StringBinding', credentialsId: "milvus-ci-codecov-token", variable: 'CODECOV_TOKEN']]) {
                sh "curl -s https://codecov.io/bash | bash -s - -f ${coverageInfoName} -U \"--proxy ${proxyAddr}\" -A \"--proxy ${proxyAddr}\" -n ${BINARY_VERSION}-version-${OS_NAME}-unittest -F ${formatFlag} || echo \"Codecov did not collect coverage reports\""
            }
        }
    }
}
