timeout(time: 60, unit: 'MINUTES') {
    container('deploy-env') {
        dir ('milvus-helm-chart') {
            sh " helm version && \
                 helm repo add stable https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts && \
                 helm repo add bitnami https://charts.bitnami.com/bitnami && \
                 helm repo add minio https://helm.min.io/ && \
                 helm repo update"

            def milvusHelmURL = "https://github.com/zilliztech/milvus-helm-charts.git"
            checkout([$class: 'GitSCM', branches: [[name: "${env.HELM_BRANCH}"]], userRemoteConfigs: [[url: "${milvusHelmURL}"]]])

            dir ('charts/milvus-ha') {
                sh script: "kubectl create namespace ${env.HELM_RELEASE_NAMESPACE}", returnStatus: true

                def helmCMD = ""
                if ("${REGRESSION_SERVICE_TYPE}" == "distributed") {
                    helmCMD = "helm install --wait --timeout 300s \
                                   --set standalone.enabled=false \
                                   --set image.all.repository=${env.TARGET_REPO}/milvus-distributed \
                                   --set image.all.tag=${env.TARGET_TAG} \
                                   --set image.all.pullPolicy=Always \
                                   --set logsPersistence.enabled=true \
                                   --set logsPersistence.mountPath=/milvus-distributed/logs \
                                   --namespace ${env.HELM_RELEASE_NAMESPACE} ${env.HELM_RELEASE_NAME} ."
                } else {
                    helmCMD = "helm install --wait --timeout 300s \
                                   --set image.all.repository=${env.TARGET_REPO}/milvus-distributed \
                                   --set image.all.tag=${env.TARGET_TAG} \
                                   --set image.all.pullPolicy=Always \
                                   --set logsPersistence.enabled=true \
                                   --set logsPersistence.mountPath=/milvus-distributed/logs \
                                   --namespace ${env.HELM_RELEASE_NAMESPACE} ${env.HELM_RELEASE_NAME} ."
                }

                try {
                    sh "${helmCMD}"
                } catch (exc) {
                    def helmStatusCMD = "helm get manifest -n ${env.HELM_RELEASE_NAMESPACE} ${env.HELM_RELEASE_NAME} | kubectl describe -n ${env.HELM_RELEASE_NAMESPACE} -f - && \
                                         helm status -n ${env.HELM_RELEASE_NAMESPACE} ${env.HELM_RELEASE_NAME}"
                    sh script: helmStatusCMD, returnStatus: true
                    throw exc
                }
            }
        }
    }

    container('test-env') {
        try {
            dir ('tests/python_test') {
                sh "python3 -m pip install --no-cache-dir -r requirements.txt"
                sh "pytest --tags=0331 --ip ${env.HELM_RELEASE_NAME}-milvus-ha.${env.HELM_RELEASE_NAMESPACE}.svc.cluster.local"
            }
        } catch (exc) {
            echo 'PyTest Regression Failed !'
            throw exc
        } finally {
            container('deploy-env') {
                def labels = ""
                if ("${REGRESSION_SERVICE_TYPE}" == "distributed") {
                    labels = "app.kubernetes.io/instance=${env.HELM_RELEASE_NAME},component=proxyservice"
                } else {
                    labels = "app.kubernetes.io/instance=${env.HELM_RELEASE_NAME},component=standalone"
                }
                sh "mkdir -p ${env.DEV_TEST_ARTIFACTS_PATH}"
                sh "kubectl cp -n ${env.HELM_RELEASE_NAMESPACE} \$(kubectl get pod -n ${env.HELM_RELEASE_NAMESPACE} -l ${labels} -o jsonpath='{range.items[0]}{.metadata.name}'):logs ${env.DEV_TEST_ARTIFACTS_PATH}"
                archiveArtifacts artifacts: "${env.DEV_TEST_ARTIFACTS_PATH}/**", allowEmptyArchive: true
            }
        }
    }
}
