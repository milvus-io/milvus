timeout(time: 120, unit: 'MINUTES') {
    dir ("ci/scripts") {
        withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
            def checkResult = sh(script: "./check_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache", returnStatus: true)
    
            if ("${BINARY_VERSION}" == "gpu") {
                sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -j4 -i ${env.MILVUS_INSTALL_PREFIX} --with_fiu --coverage -l -g -u\""
            } else {
                sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -j4 -i ${env.MILVUS_INSTALL_PREFIX} --with_fiu --coverage -l -u\""
            }
            sh "./update_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache -u ${USERNAME} -p ${PASSWORD}"
        }
    }
}
