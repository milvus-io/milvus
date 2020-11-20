dir ("docker/deploy") {
    def binaryPackage = "${PROJECT_NAME}-${PACKAGE_VERSION}.tar.gz"

    withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'JFROG_USERNAME', passwordVariable: 'JFROG_PASSWORD')]) {
        def downloadStatus = sh(returnStatus: true, script: 'curl -u$JFROG_USERNAME:$JFROG_PASSWORD -O ${params.JFROG_ARTFACTORY_URL}/milvus/package/${binaryPackage}')

        if (downloadStatus != 0) {
            error("\" Download \" ${params.JFROG_ARTFACTORY_URL}/milvus/package/${binaryPackage} \" failed!")
        }
    }
    sh "tar zxvf ${binaryPackage}"
    def sourceImage = "${params.DOKCER_REGISTRY_URL}/${PROJECT_NAME}/engine:${SOURCE_TAG}"

    try {
        sh(returnStatus: true, script: "docker pull ${sourceImage}")
        sh "docker-compose build --force-rm ${BINARY_VERSION}_${OS_NAME}"
        try {
            withCredentials([usernamePassword(credentialsId: "${params.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]) {
                sh 'docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD ${params.DOKCER_REGISTRY_URL}'
                sh "docker-compose push ${BINARY_VERSION}_${OS_NAME}"
            }
        } catch (exc) {
            throw exc
        } finally {
            sh "docker logout ${params.DOKCER_REGISTRY_URL}"
        }
    } catch (exc) {
        throw exc
    } finally {
        def isExistImage = sh(returnStatus: true, script: "docker inspect --type=image ${sourceImage} 2>&1 > /dev/null")
        if (isExistImage == 0) {
            sh(returnStatus: true, script: "docker rmi -f \$(docker inspect --type=image --format \"{{.ID}}\" ${sourceImage})")
        }
        sh "docker-compose down --rmi all"
        sh(returnStatus: true, script: "docker rmi -f \$(docker images | grep '<none>' | awk '{print \$3}')")
    }
}
