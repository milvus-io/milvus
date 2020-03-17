sh "rm -rf ${env.MILVUS_INSTALL_PREFIX}/unittest"
sh "tar -zcvf ./${env.PROJECT_NAME}-${env.PACKAGE_VERSION}.tar.gz -C ${env.MILVUS_ROOT_PATH}/ ${env.PROJECT_NAME}"
withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'JFROG_USERNAME', passwordVariable: 'JFROG_PASSWORD')]) {
    def uploadStatus = sh(returnStatus: true, script: "curl -u${JFROG_USERNAME}:${JFROG_PASSWORD} -T ./${env.PROJECT_NAME}-${env.PACKAGE_VERSION}.tar.gz ${params.JFROG_ARTFACTORY_URL}/milvus/package/${env.PROJECT_NAME}-${env.PACKAGE_VERSION}.tar.gz")
    if (uploadStatus != 0) {
        error("\" ${env.PROJECT_NAME}-${env.PACKAGE_VERSION}.tar.gz \" upload to \" ${params.JFROG_ARTFACTORY_URL}/milvus/package/${env.PROJECT_NAME}-${env.PACKAGE_VERSION}.tar.gz \" failed!")
    }
}
