try {
    sh 'helm init --client-only --skip-refresh'
    sh 'helm repo add milvus https://registry.zilliz.com/chartrepo/milvus'
    sh 'helm repo update'
    sh "helm install --set engine.image.repository=registry.zilliz.com/${PROJECT_NAME}/engine --set engine.image.tag=${DOCKER_VERSION} --set expose.type=clusterIP --name ${env.JOB_NAME}-${env.BUILD_NUMBER} --version 0.3.0 milvus/milvus-gpu"
} catch (exc) {
    updateGitlabCommitStatus name: 'Deloy to Dev', state: 'failed'
    echo 'Helm running failed!'
    sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}"
    throw exc
}
