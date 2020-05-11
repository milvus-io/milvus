timeout(time: 180, unit: 'MINUTES') {
    dir ('milvus-helm') {
        sh 'helm version'
        sh 'helm repo add stable https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts'
        sh 'helm repo update'
        checkout([$class: 'GitSCM', branches: [[name: "nightly"]], userRemoteConfigs: [[url: "https://github.com/milvus-io/milvus-helm.git", name: 'origin', refspec: "+refs/heads/nightly:refs/remotes/origin/nightly"]]])
        retry(3) {
            try {
                sh "helm install --wait --timeout 300s --set mishards.enabled=true --set persistence.enabled=true --set image.repository=registry.zilliz.com/milvus/engine --set image.tag=${DOCKER_VERSION} --set image.pullPolicy=Always --set service.type=ClusterIP -f ci/db_backend/mysql_${BINARY_VERSION}_values.yaml --namespace milvus ${env.SHARDS_HELM_RELEASE_NAME} ."
            } catch (exc) {
                def helmStatusCMD = "helm get manifest --namespace milvus ${env.SHARDS_HELM_RELEASE_NAME} | kubectl describe -n milvus -f - && \
                                     kubectl logs --namespace milvus -l \"app=milvus,release=${env.SHARDS_HELM_RELEASE_NAME}\" -c milvus && \
                                     helm status -n milvus ${env.SHARDS_HELM_RELEASE_NAME}"
                def helmResult = sh script: helmStatusCMD, returnStatus: true
                if (!helmResult) {
                    sh "helm uninstall -n milvus ${env.SHARDS_HELM_RELEASE_NAME} && sleep 1m"
                }
                throw exc
            }
        }
    }
    
    dir ("tests/milvus_python_test") {
        sh 'python3 -m pip install -r requirements.txt'
        sh "pytest . --alluredir=\"test_out/dev/shards/\" --ip ${env.SHARDS_HELM_RELEASE_NAME}-milvus-mishards.milvus.svc.cluster.local"
    }
}
