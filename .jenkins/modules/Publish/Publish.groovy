withCredentials([usernamePassword(credentialsId: "${env.JFROG_CREDENTIALS_ID}", usernameVariable: 'JFROG_USERNAME', passwordVariable: 'JFROG_PASSWORD')]) {
    def downloadStatus = sh(returnStatus: true, script: 'curl -u${JFROG_USERNAME}:${JFROG_PASSWORD} -O ${PACKAGE_ARTFACTORY_URL}')

    if (downloadStatus != 0) {
        error("\" Download \" ${PACKAGE_ARTFACTORY_URL} \" failed!")
    }
}

sh 'tar zxvf ${PACKAGE_NAME}'

dir ('build/docker/deploy') {
	try {
        withCredentials([usernamePassword(credentialsId: "${env.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]) {
            sh 'docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD} ${DOKCER_REGISTRY_URL}'
            sh 'docker pull ${SOURCE_REPO}/master:${SOURCE_TAG} || true'
            sh 'docker-compose build --force-rm master'
            sh 'docker-compose push master'
            sh 'docker pull ${SOURCE_REPO}/proxy:${SOURCE_TAG} || true'
            sh 'docker-compose build --force-rm proxy'
            sh 'docker-compose push proxy'
            sh 'docker pull ${SOURCE_REPO}/querynode:${SOURCE_TAG} || true'
            sh 'docker-compose build --force-rm querynode'
            sh 'docker-compose push querynode'
        }
    } catch (exc) {
        throw exc
    } finally {
        sh 'docker logout ${DOKCER_REGISTRY_URL}'
        sh "docker rmi -f \$(docker images | grep '<none>' | awk '{print \$3}') || true"
        sh 'docker-compose down --rmi all'
    }
}
