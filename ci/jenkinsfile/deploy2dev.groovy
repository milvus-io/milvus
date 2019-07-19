try {
    sh 'helm init --client-only --skip-refresh --stable-repo-url https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts'
    sh 'helm repo add milvus https://registry.zilliz.com/chartrepo/milvus'
    sh 'helm repo update'
    dir ("milvus-helm") {
        checkout([$class: 'GitSCM', branches: [[name: "${SEMVER}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_USER}", url: "git@192.168.1.105:megasearch/milvus-helm.git", name: 'origin', refspec: "+refs/heads/${SEMVER}:refs/remotes/origin/${SEMVER}"]]])
        dir ("milvus/milvus-gpu") {
            sh "helm install --wait --set engine.image.tag=${DOCKER_VERSION} --set expose.type=clusterIP --name ${env.JOB_NAME}-${env.BUILD_NUMBER} -f ci/values.yaml --version 0.3.1 ."
        }
    }
    /*
    timeout(time: 2, unit: 'MINUTES') {
        waitUntil {
            def result = sh script: "nc -z -w 3 ${env.JOB_NAME}-${env.BUILD_NUMBER}-milvus-gpu-engine.kube-opt.svc.cluster.local 19530", returnStatus: true
            return !result
        }
    }
    */
} catch (exc) {
    echo 'Helm running failed!'
    sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}"
    throw exc
}

