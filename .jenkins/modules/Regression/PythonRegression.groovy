try {

    sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} up -d etcd'
    sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} up -d pulsar'
    sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} up -d minio'
    dir ('build/docker/deploy') {
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} pull'
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} up -d master'
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} up -d proxy'
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} run -e QUERY_NODE_ID=1 -d querynode'
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} run -e QUERY_NODE_ID=2 -d querynode'
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} run -e WRITE_NODE_ID=3 -d writenode'
    }

    dir ('build/docker/test') {
        sh 'docker pull ${SOURCE_REPO}/pytest:${SOURCE_TAG} || true'
        sh 'docker-compose build --force-rm regression'
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} run --rm regression'
        try {
            withCredentials([usernamePassword(credentialsId: "${env.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]) {
                sh 'docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD} ${DOKCER_REGISTRY_URL}'
                sh 'docker-compose push regression'
            }
        } catch (exc) {
            throw exc
        } finally {
            sh 'docker logout ${DOKCER_REGISTRY_URL}'
        }
    }
} catch(exc) {
    throw exc
} finally {
    sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} rm -f -s -v pulsar'
    sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} rm -f -s -v etcd'
    sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} rm -f -s -v minio'
    dir ('build/docker/deploy') {
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} down --rmi all -v || true'
    }
    dir ('build/docker/test') {
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} run --rm regression /bin/bash -c "rm -rf __pycache__ && rm -rf .pytest_cache"'
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME} down --rmi all -v || true'
    }
}
