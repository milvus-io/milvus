sh 'helm init --client-only --skip-refresh --stable-repo-url https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts'
sh 'helm repo update'
dir ('milvus-helm') {
    checkout([$class: 'GitSCM', branches: [[name: "0.5.3"]], userRemoteConfigs: [[url: "https://github.com/milvus-io/milvus-helm.git", name: 'origin', refspec: "+refs/heads/0.5.3:refs/remotes/origin/0.5.3"]]])
    dir ("milvus-gpu") {
        sh "helm install --wait --timeout 300 --set engine.image.tag=${DOCKER_VERSION} --set expose.type=clusterIP --name ${env.PIPELINE_NAME}-${env.BUILD_NUMBER}-single-gpu -f ci/db_backend/sqlite_values.yaml -f ci/filebeat/values.yaml --namespace milvus ."
    }
}

