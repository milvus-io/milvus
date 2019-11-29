timeout(time: 60, unit: 'MINUTES') {
    dir ("ci/scripts") {
        withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
            def checkResult = sh(script: "./check_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache", returnStatus: true)
            if ("${env.BINRARY_VERSION}" == "gpu") {
                sh ". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -o /opt/milvus -l -g -x -u -c"
            } else {
                sh ". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -o /opt/milvus -l -u -c"
            }
            sh "./update_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache -u ${USERNAME} -p ${PASSWORD}"
        }
    }
}
