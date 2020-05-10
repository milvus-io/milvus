timeout(time: 180, unit: 'MINUTES') {
    dir ('milvus-helm') {
        sh 'helm version'
        sh 'helm repo add stable https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts'
        sh 'helm repo update'
        checkout([$class: 'GitSCM', branches: [[name: "${env.HELM_BRANCH}"]], userRemoteConfigs: [[url: "https://github.com/milvus-io/milvus-helm.git", name: 'origin', refspec: "+refs/heads/${env.HELM_BRANCH}:refs/remotes/origin/${env.HELM_BRANCH}"]]])
        retry(3) {
            try {
                sh "helm install --wait --timeout 300s --set image.repository=registry.zilliz.com/milvus/engine --set image.tag=${DOCKER_VERSION} --set image.pullPolicy=Always --set service.type=ClusterIP -f ci/db_backend/mysql_${BINARY_VERSION}_values.yaml -f ci/filebeat/values.yaml --namespace milvus ${env.HELM_RELEASE_NAME} ."
            } catch (exc) {
                def helmStatusCMD = "helm get manifest --namespace milvus ${env.HELM_RELEASE_NAME} | kubectl describe -n milvus -f - && \
                                     kubectl logs --namespace milvus -l \"app=milvus,release=${env.HELM_RELEASE_NAME}\" -c milvus && \
                                     helm status -n milvus ${env.HELM_RELEASE_NAME}"
                def helmResult = sh script: helmStatusCMD, returnStatus: true
                if (!helmResult) {
                    sh "helm uninstall -n milvus ${env.HELM_RELEASE_NAME} && sleep 1m"
                }
                throw exc
            }
        }
    }
    
    dir ("tests/milvus_python_test") {
        // sh 'python3 -m pip install -r requirements.txt -i http://pypi.douban.com/simple --trusted-host pypi.douban.com'
        sh 'python3 -m pip install -r requirements.txt'
        sh "pytest . --alluredir=\"test_out/dev/single/mysql\" --ip ${env.HELM_RELEASE_NAME}.milvus.svc.cluster.local"
    }
    // sqlite database backend test
    load "ci/jenkins/step/cleanupSingleDev.groovy"
    
    if (!fileExists('milvus-helm')) {
        dir ("milvus-helm") {
            checkout([$class: 'GitSCM', branches: [[name:"${env.HELM_BRANCH}"]], userRemoteConfigs: [[url: "https://github.com/milvus-io/milvus-helm.git", name: 'origin', refspec: "+refs/heads/${env.HELM_BRANCH}:refs/remotes/origin/${env.HELM_BRANCH}"]]])
        }
    }
    dir ("milvus-helm") {
        retry(3) {
            try {
                sh "helm install --wait --timeout 300s --set image.repository=registry.zilliz.com/milvus/engine --set image.tag=${DOCKER_VERSION} --set image.pullPolicy=Always --set service.type=ClusterIP -f ci/db_backend/sqlite_${BINARY_VERSION}_values.yaml -f ci/filebeat/values.yaml --namespace milvus ${env.HELM_RELEASE_NAME} ."
            } catch (exc) {
                def helmStatusCMD = "helm get manifest --namespace milvus ${env.HELM_RELEASE_NAME} | kubectl describe -n milvus -f - && \
                                     kubectl logs --namespace milvus -l \"app=milvus,release=${env.HELM_RELEASE_NAME}\" -c milvus && \
                                     helm status -n milvus ${env.HELM_RELEASE_NAME}"
                def helmResult = sh script: helmStatusCMD, returnStatus: true
                if (!helmResult) {
                    sh "helm uninstall -n milvus ${env.HELM_RELEASE_NAME} && sleep 1m"
                }
                throw exc
            }
        }
    }
    dir ("tests/milvus_python_test") {
        sh "pytest . --alluredir=\"test_out/dev/single/sqlite\" --ip ${env.HELM_RELEASE_NAME}.milvus.svc.cluster.local"
    }
}
