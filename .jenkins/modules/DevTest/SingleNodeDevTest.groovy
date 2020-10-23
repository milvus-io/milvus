timeout(time: 150, unit: 'MINUTES') {
    sh "mkdir -p ${env.DEV_TEST_ARTIFACTS}"

    def helmCMD = "helm install --wait --timeout 300s \
                   --set image.repository=registry.zilliz.com/milvus/engine \
                   --set image.tag=${DOCKER_VERSION} \
                   --set image.pullPolicy=Always \
                   --set service.type=ClusterIP \
                   --set image.resources.requests.memory=8Gi \
                   --set image.resources.requests.cpu=4.0 \
                   --set image.resources.limits.memory=14Gi \
                   --set image.resources.limits.cpu=6.0 \
                   -f ci/filebeat/values.yaml \
                   -f test.yaml \
                   --namespace milvus"

    def helmStatusCMD = "helm get manifest --namespace milvus ${env.HELM_RELEASE_NAME} | kubectl describe -n milvus -f - && \
                         kubectl logs --namespace milvus -l \"app.kubernetes.io/name=milvus,app.kubernetes.io/instance=${env.HELM_RELEASE_NAME}\" -c milvus && \
                         helm status -n milvus ${env.HELM_RELEASE_NAME}"

    def isTimeTriggeredBuild = currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause').size() != 0

    dir ('milvus-helm') {
        sh "helm version"
        sh "helm repo add stable https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts"
        sh "helm repo update"

        def MILVUS_HELM_URL = "https://github.com/milvus-io/milvus-helm.git"
        def REF_SPEC = "+refs/heads/${env.HELM_BRANCH}:refs/remotes/origin/${env.HELM_BRANCH}"
        checkout([$class: 'GitSCM', branches: [[name: "${env.HELM_BRANCH}"]], userRemoteConfigs: [[url: "${MILVUS_HELM_URL}", name: 'origin', refspec: "${REF_SPEC}"]]])

        retry(3) {
            try {
                dir ('charts/milvus') {
                    writeFile file: 'test.yaml', text: "extraConfiguration:\n  engine:\n    build_index_threshold: 1000\n    max_partition_num: 256"
                    def helmCMD_mysql = "${helmCMD}" + " -f ci/db_backend/mysql_${BINARY_VERSION}_values.yaml ${env.HELM_RELEASE_NAME} ."
                    sh "${helmCMD_mysql}"
                }
            } catch (exc) {
                sh script: helmStatusCMD, returnStatus: true
                sh script: "helm uninstall -n milvus ${env.HELM_RELEASE_NAME} && sleep 1m", returnStatus: true
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
                               --alluredir=\"test_out/dev/single/mysql\" \
                               --ip ${env.HELM_RELEASE_NAME}.milvus.svc.cluster.local \
                               --service ${env.HELM_RELEASE_NAME} >> \
                               ${WORKSPACE}/${env.DEV_TEST_ARTIFACTS}/milvus_${BINARY_VERSION}_mysql_dev_test.log"
        sh "${pytestCMD_mysql}"
    }

    if (isTimeTriggeredBuild) {
        // sqlite database backend test
        MPLModule('Cleanup Single Node DevTest')

        retry(3) {
            try {
                dir ("milvus-helm/charts/milvus") {
                    writeFile file: 'test.yaml', text: "extraConfiguration:\n  engine:\n    build_index_threshold: 1000\n    max_partition_num: 256"
                    def helmCMD_sqlite = "${helmCMD}" + " -f ci/db_backend/sqlite_${BINARY_VERSION}_values.yaml ${env.HELM_RELEASE_NAME} ."
                    sh "${helmCMD_sqlite}"
                }
            } catch (exc) {
                def helmResult = sh script: helmStatusCMD, returnStatus: true
                sh script: "helm uninstall -n milvus ${env.HELM_RELEASE_NAME} && sleep 1m", returnStatus: true
                throw exc
            }
        }
        dir ("tests/milvus_python_test") {
            def pytestCMD_sqlite = "pytest . \
                                    --level=2 \
                                    --alluredir=\"test_out/dev/single/sqlite\" \
                                    --ip ${env.HELM_RELEASE_NAME}.milvus.svc.cluster.local \
                                    --service ${env.HELM_RELEASE_NAME} >> \
                                    ${WORKSPACE}/${env.DEV_TEST_ARTIFACTS}/milvus_${BINARY_VERSION}_sqlite_dev_test.log"
            sh "${pytestCMD_sqlite}"
        }
    }
}
