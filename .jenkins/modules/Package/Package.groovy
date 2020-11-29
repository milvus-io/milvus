sh 'tar -zcvf ./${PACKAGE_NAME} ./bin ./configs ./lib'
withCredentials([usernamePassword(credentialsId: "${env.JFROG_CREDENTIALS_ID}", usernameVariable: 'JFROG_USERNAME', passwordVariable: 'JFROG_PASSWORD')]) {
    def uploadStatus = sh(returnStatus: true, script: 'curl -u${JFROG_USERNAME}:${JFROG_PASSWORD} -T ./${PACKAGE_NAME} ${PACKAGE_ARTFACTORY_URL}')
    if (uploadStatus != 0) {
        error("\" ${PACKAGE_NAME} \" upload to \" ${PACKAGE_ARTFACTORY_URL} \" failed!")
    }
}
