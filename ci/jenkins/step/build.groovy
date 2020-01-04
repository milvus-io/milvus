timeout(time: 75, unit: 'MINUTES') {
    dir ("ci/scripts") {
        withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
            def checkResult = sh(script: "./check_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache", returnStatus: true)

            if ("${BINARY_VERSION}" == "gpu") {
                sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -o ${env.MILVUS_INSTALL_PREFIX} -l -g -u -c\""
            } else {
                sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -o ${env.MILVUS_INSTALL_PREFIX} -l -u -c\""
            }
            sh "./update_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache -u ${USERNAME} -p ${PASSWORD}"
        }
    }
}
