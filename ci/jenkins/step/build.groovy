timeout(time: 120, unit: 'MINUTES') {
    dir ("ci/scripts") {
        withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
            if (!isTimeTriggeredBuild()) {
                def checkResult = sh(script: "./check_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache", returnStatus: true)
            }
    
            if ("${BINARY_VERSION}" == "gpu") {
                if (isTimeTriggeredBuild()) {
                    sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -j4 -i ${env.MILVUS_INSTALL_PREFIX} --with_fiu --coverage -l -g -u -s '-gencode=arch=compute_61,code=sm_61;-gencode=arch=compute_75,code=sm_75' \""
                } else {
                    sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -j4 -i ${env.MILVUS_INSTALL_PREFIX} --with_fiu --coverage -l -g -u\""
                }
            } else {
                sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -j4 -i ${env.MILVUS_INSTALL_PREFIX} --with_fiu --coverage -l -u\""
            }
            sh "./update_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache -u ${USERNAME} -p ${PASSWORD}"
        }
    }
}

boolean isTimeTriggeredBuild() {
    if (currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause').size() != 0) {
        return true
    }
    return false
}
