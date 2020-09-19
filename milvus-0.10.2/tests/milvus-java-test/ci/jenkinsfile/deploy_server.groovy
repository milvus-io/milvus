try {
    sh 'helm init --client-only --skip-refresh --stable-repo-url https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts'
    sh 'helm repo add milvus https://registry.zilliz.com/chartrepo/milvus'
    sh 'helm repo update'
    dir ("milvus-helm") {
        checkout([$class: 'GitSCM', branches: [[name: "${HELM_BRANCH}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_USER}", url: "git@192.168.1.105:megasearch/milvus-helm.git", name: 'origin', refspec: "+refs/heads/${HELM_BRANCH}:refs/remotes/origin/${HELM_BRANCH}"]]])
        dir ("milvus/milvus-gpu") {
            sh "helm install --wait --timeout 300 --set engine.image.tag=${IMAGE_TAG} --set expose.type=clusterIP --name ${env.JOB_NAME}-${env.BUILD_NUMBER} -f ci/values.yaml --namespace milvus-sdk-test --version 0.3.1 ."
        }
    }
} catch (exc) {
    echo 'Helm running failed!'
    sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}"
    throw exc
}

