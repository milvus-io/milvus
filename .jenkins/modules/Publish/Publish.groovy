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

            sh 'docker pull ${SOURCE_REPO}/proxyservice:${SOURCE_TAG} || true'
            sh 'docker-compose build --force-rm proxyservice'
            sh 'docker-compose push proxyservice'

            sh 'docker pull ${SOURCE_REPO}/proxynode:${SOURCE_TAG} || true'
            sh 'docker-compose build --force-rm proxynode'
            sh 'docker-compose push proxynode'

            sh 'docker pull ${SOURCE_REPO}/indexservice:${SOURCE_TAG} || true'
            sh 'docker-compose build --force-rm indexservice'
            sh 'docker-compose push indexservice'

            sh 'docker pull ${SOURCE_REPO}/indexnode:${SOURCE_TAG} || true'
            sh 'docker-compose build --force-rm indexnode'
            sh 'docker-compose push indexnode'

            sh 'docker pull ${SOURCE_REPO}/queryservice:${SOURCE_TAG} || true'
            sh 'docker-compose build --force-rm queryservice'
            sh 'docker-compose push queryservice'

            sh 'docker pull ${SOURCE_REPO}/dataservice:${SOURCE_TAG} || true'
            sh 'docker-compose build --force-rm dataservice'
            sh 'docker-compose push dataservice'

            sh 'docker pull registry.zilliz.com/milvus-distributed/milvus-distributed-dev:latest || true'
            sh 'docker pull ${SOURCE_REPO}/querynode:${SOURCE_TAG} || true'
            sh 'docker-compose build --force-rm querynode'
            sh 'docker-compose push querynode'

            sh 'docker pull registry.zilliz.com/milvus-distributed/milvus-distributed-dev:latest || true'
            sh 'docker pull ${SOURCE_REPO}/datanode:${SOURCE_TAG} || true'
            sh 'docker-compose build --force-rm datanode'
            sh 'docker-compose push datanode'
        }
    } catch (exc) {
        throw exc
    } finally {
        sh 'docker logout ${DOKCER_REGISTRY_URL}'
        sh "docker rmi -f \$(docker images | grep '<none>' | awk '{print \$3}') || true"
        sh 'docker-compose down --rmi all'
    }
}
