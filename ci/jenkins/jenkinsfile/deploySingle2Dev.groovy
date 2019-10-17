try {
    sh 'helm init --client-only --skip-refresh --stable-repo-url https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts'
    sh 'helm repo update'
    dir ('milvus-helm') {
        checkout([$class: 'GitSCM', branches: [[name: "0.5.0"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_CREDENTIALS_ID}", url: "https://github.com/milvus-io/milvus-helm.git", name: 'origin', refspec: "+refs/heads/0.5.0:refs/remotes/origin/0.5.0"]]])
        dir ("milvus-gpu") {
            sh "helm install --wait --timeout 300 --set engine.image.tag=${DOCKER_VERSION} --set expose.type=clusterIP --name ${env.PIPELIEN_NAME}-${env.BUILD_NUMBER}-single-gpu -f ci/values.yaml --namespace milvus ."
        }
    }
} catch (exc) {
    echo 'Helm running failed!'
    sh "helm del --purge ${env.PIPELIEN_NAME}-${env.BUILD_NUMBER}-single-gpu"
    throw exc
}
