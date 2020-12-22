timeout(time: 150, unit: 'MINUTES') {
    sh "mkdir -p ${env.DEV_TEST_ARTIFACTS}"

    def helmCMD = "helm install --wait --timeout 300s \
                   --set image.repository=registry.zilliz.com/milvus/engine \
                   --set image.tag=${DOCKER_VERSION} \
                   --set image.pullPolicy=Always \
                   --set service.type=ClusterIP \
                   -f ci/filebeat/values.yaml \
                   -f test.yaml \
                   --namespace milvus"

    def helmStatusCMD = "helm get manifest --namespace milvus ${env.CLUSTER_HELM_RELEASE_NAME} | kubectl describe -n milvus -f - && \
                         kubectl logs --namespace milvus -l \"app.kubernetes.io/name=milvus,app.kubernetes.io/instance=${env.CLUSTER_HELM_RELEASE_NAME},component=writable\" -c milvus && \
                         kubectl logs --namespace milvus -l \"app.kubernetes.io/name=milvus,app.kubernetes.io/instance=${env.CLUSTER_HELM_RELEASE_NAME},component=nginx\" && \
                         helm status -n milvus ${env.CLUSTER_HELM_RELEASE_NAME}"

    def isTimeTriggeredBuild = currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause').size() != 0

    dir ('milvus-helm') {
        sh "helm version"
        sh "helm repo add stable https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts"
        sh "helm repo update"

        def MILVUS_HELM_URL = "https://github.com/milvus-io/milvus-helm.git"
        def REF_SPEC = "+refs/heads/${env.HELM_BRANCH}:refs/remotes/origin/${env.HELM_BRANCH}"
        checkout([$class: 'GitSCM', branches: [[name: "${env.HELM_BRANCH}"]], userRemoteConfigs: [[url: "${MILVUS_HELM_URL}", name: 'origin', refspec: "${REF_SPEC}"]]])

        retry(3) {
            // test cluster without read nodes
            try {
                dir ('charts/milvus') {
                    writeFile file: 'test.yaml', text: "extraConfiguration:\n  engine:\n    build_index_threshold: 1000\n    max_partition_num: 256"
                    def helmCMD_mysql = "${helmCMD}" + " --set cluster.enabled=true --set readonly.replicas=1 --set nginx.image.repository=threaddao/nginx-delay --set nginx.image.tag=v1 --set nginx.delay.enabled=true --set nginx.delay.time=1000ms --set persistence.enabled=true -f ci/db_backend/mysql_${BINARY_VERSION}_values.yaml ${env.CLUSTER_HELM_RELEASE_NAME} ."
                    sh "${helmCMD_mysql}"
                }
            } catch (exc) {
                sh script: helmStatusCMD, returnStatus: true
                sh script: "helm uninstall -n milvus ${env.CLUSTER_HELM_RELEASE_NAME} && sleep 1m", returnStatus: true
                throw exc
            }
        }
    }

    dir ("tests/milvus_python_test") {
        sh 'python3 -m pip install -r requirements.txt'
        def TESTCASE_LEVEL = 1
        if (isTimeTriggeredBuild) {
            TESTCASE_LEVEL = 2
        }
        def pytestCMD_mysql = "pytest . \
                               --level=${TESTCASE_LEVEL} \
                               -n 4 \
                               --alluredir=\"test_out/dev/cluster/mysql\" \
                               --ip ${env.CLUSTER_HELM_RELEASE_NAME}.milvus.svc.cluster.local \
                               --service ${env.CLUSTER_HELM_RELEASE_NAME} >> \
                               ${WORKSPACE}/${env.DEV_TEST_ARTIFACTS}/milvus_${BINARY_VERSION}_mysql_cluster_dev_test.log"
        sh "${pytestCMD_mysql}"
    }
}
