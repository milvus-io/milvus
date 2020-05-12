dir ("docker/deploy") {
    def binaryPackage = "${PROJECT_NAME}-${PACKAGE_VERSION}.tar.gz"

    withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'JFROG_USERNAME', passwordVariable: 'JFROG_PASSWORD')]) {
        def downloadStatus = sh(returnStatus: true, script: "curl -u${JFROG_USERNAME}:${JFROG_PASSWORD} -O ${params.JFROG_ARTFACTORY_URL}/milvus/package/${binaryPackage}")

        if (downloadStatus != 0) {
            error("\" Download \" ${params.JFROG_ARTFACTORY_URL}/milvus/package/${binaryPackage} \" failed!")
        }
    }
    sh "tar zxvf ${binaryPackage}"
    def imageName = "${PROJECT_NAME}/engine:${DOCKER_VERSION}"

    try {
        sh "docker-compose pull --ignore-pull-failures ${BINARY_VERSION}_${OS_NAME}"
        sh "docker-compose build ${BINARY_VERSION}_${OS_NAME}"
        docker.withRegistry("https://${params.DOKCER_REGISTRY_URL}", "${params.DOCKER_CREDENTIALS_ID}") {
            sh "docker-compose push ${BINARY_VERSION}_${OS_NAME}"
        }
    } catch (exc) {
        throw exc
    } finally {
        sh "docker-compose down ${BINARY_VERSION}_${OS_NAME}"
    }
}
