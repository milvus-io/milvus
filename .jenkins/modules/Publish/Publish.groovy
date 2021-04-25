withCredentials([usernamePassword(credentialsId: "${env.JFROG_CREDENTIALS_ID}", usernameVariable: 'JFROG_USERNAME', passwordVariable: 'JFROG_PASSWORD')]) {
    def downloadStatus = sh(returnStatus: true, script: 'curl -u${JFROG_USERNAME}:${JFROG_PASSWORD} -O ${PACKAGE_ARTFACTORY_URL}')

    if (downloadStatus != 0) {
        error("\" Download \" ${PACKAGE_ARTFACTORY_URL} \" failed!")
    }
}

sh 'tar zxvf ${PACKAGE_NAME}'

try {
    withCredentials([usernamePassword(credentialsId: "${env.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]) {
        sh 'docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD} ${DOKCER_REGISTRY_URL}'

        sh 'docker pull registry.zilliz.com/milvus/openblas:latest || true'
        sh "docker build -f build/docker/milvus/Dockerfile -t ${TARGET_REPO}/milvus:${TARGET_TAG} ."
        sh "docker push ${TARGET_REPO}/milvus:${TARGET_TAG}"
    }
} catch (exc) {
    throw exc
} finally {
    sh 'docker logout ${DOKCER_REGISTRY_URL}'
    sh "docker rmi -f \$(docker images | grep '<none>' | awk '{print \$3}') || true"
    sh "docker rmi ${TARGET_REPO}/milvus:${TARGET_TAG}"
}
