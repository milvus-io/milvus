try {
    sh 'helm init --client-only --skip-refresh --stable-repo-url https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts'
    sh 'helm repo add milvus https://registry.zilliz.com/chartrepo/milvus'
    sh 'helm repo update'
    dir ("milvus-helm") {
        checkout([$class: 'GitSCM', branches: [[name: "${SEMVER}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_USER}", url: "git@192.168.1.105:megasearch/milvus-helm.git", name: 'origin', refspec: "+refs/heads/${SEMVER}:refs/remotes/origin/${SEMVER}"]]])
        dir ("milvus/milvus-cluster") {
            sh "helm install --set roServers.image.tag=${DOCKER_VERSION} --set woServers.image.tag=${DOCKER_VERSION} --set expose.type=clusterIP -f ci/values.yaml --name ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster --namespace milvus-cluster --version 0.3.1 . "
        }
        timeout(time: 2, unit: 'MINUTES') {
            waitUntil {
                def result = sh script: "nc -z -w 2 ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster-milvus-cluster-proxy.milvus-cluster.svc.cluster.local 19530", returnStatus: true
                return !result
            }
        }
    }
} catch (exc) {
    updateGitlabCommitStatus name: 'Deloy to Dev', state: 'failed'
    echo 'Helm running failed!'
    sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster"
    throw exc
}

