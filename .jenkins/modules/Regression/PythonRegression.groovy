timeout(time: 60, unit: 'MINUTES') {
    try {
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} up -d etcd'
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} up -d pulsar'
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} up -d minio'
        dir ('build/docker/deploy') {
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} pull'
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} up -d master'
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} up -d indexservice'
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} up -d indexnode'
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} up -d proxyservice'
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} up -d dataservice'
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} up -d queryservice'
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} run -e DATA_NODE_ID=3 -d datanode'
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} up -d proxynode'
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} run -e QUERY_NODE_ID=1 -d querynode'
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} run -e QUERY_NODE_ID=2 -d querynode'
        }

        dir ('build/docker/test') {
            sh 'docker pull ${SOURCE_REPO}/pytest:${SOURCE_TAG} || true'
            sh 'docker-compose build --force-rm ${REGRESSION_SERVICE_NAME}'
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} run --rm ${REGRESSION_SERVICE_NAME}'
            try {
                withCredentials([usernamePassword(credentialsId: "${env.DOCKER_CREDENTIALS_ID}", usernameVariable: 'DOCKER_USERNAME', passwordVariable: 'DOCKER_PASSWORD')]) {
                    sh 'docker login -u ${DOCKER_USERNAME} -p ${DOCKER_PASSWORD} ${DOKCER_REGISTRY_URL}'
                    sh 'docker-compose push ${REGRESSION_SERVICE_NAME} || true'
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
        dir ('build/docker/deploy') {
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} ps -a | tail -n +3 | awk \'{ print $1 }\' | ( while read arg; do docker logs -t $arg > $arg.log 2>&1; done )'
            archiveArtifacts artifacts: "**.log", allowEmptyArchive: true
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} down --rmi all -v || true'
        }
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} rm -f -s -v pulsar'
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} rm -f -s -v etcd'
        sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} rm -f -s -v minio'
        dir ('build/docker/test') {
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} run --rm ${REGRESSION_SERVICE_NAME} /bin/bash -c "find . -name \'__pycache__\' | xargs rm -rf && find . -name \'.pytest_cache\' | xargs rm -rf"'
            sh 'docker-compose -p ${DOCKER_COMPOSE_PROJECT_NAME}-${REGRESSION_SERVICE_NAME} down --rmi all -v || true'
        }
    }
}
