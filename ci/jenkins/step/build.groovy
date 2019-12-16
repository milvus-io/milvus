timeout(time: 60, unit: 'MINUTES') {
    dir ("ci/scripts") {
        withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
            def checkResult = sh(script: "./check_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache", returnStatus: true)
            if ("${BINRARY_VERSION}" == "gpu") {
                if ("${OS_NAME}" == "centos7") {
                    sh ". ./before-install.sh && source scl_source enable devtoolset-7 && source scl_source enable llvm-toolset-7.0 && ./build.sh -t ${params.BUILD_TYPE} -o ${env.MILVUS_INSTALL_PREFIX} -l -g -u -c"
                } else {
                    sh ". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -o ${env.MILVUS_INSTALL_PREFIX} -l -g -u -c"
                }
            } else {
                if ("${OS_NAME}" == "centos7") {
                    sh ". ./before-install.sh && source scl_source enable devtoolset-7 && source scl_source enable llvm-toolset-7.0 && ./build.sh -t ${params.BUILD_TYPE} -o ${env.MILVUS_INSTALL_PREFIX} -l -u -c"
                } else {
                    sh ". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -o ${env.MILVUS_INSTALL_PREFIX} -l -u -c"
                }
            }
            sh "./update_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache -u ${USERNAME} -p ${PASSWORD}"
        }
    }
}
