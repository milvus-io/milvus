timeout(time: 30, unit: 'MINUTES') {
    try {
        dir ("milvus") {
            sh 'helm init --client-only --skip-refresh --stable-repo-url https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts'
            sh 'helm repo update'
            checkout([$class: 'GitSCM', branches: [[name: "${HELM_BRANCH}"]], userRemoteConfigs: [[url: "${HELM_URL}", name: 'origin', refspec: "+refs/heads/${HELM_BRANCH}:refs/remotes/origin/${HELM_BRANCH}"]]])
            dir ("milvus") {
                sh "helm install --wait --timeout 300 --set engine.image.tag=${IMAGE_TAG} --set expose.type=clusterIP --name acc-test-${env.JOB_NAME}-${env.BUILD_NUMBER} -f ci/db_backend/sqlite_${params.IMAGE_TYPE}_values.yaml -f ci/filebeat/values.yaml --namespace milvus --version ${HELM_BRANCH} ."
            }
        }
        // dir ("milvus") {
        //     checkout([$class: 'GitSCM', branches: [[name: "${env.SERVER_BRANCH}"]], userRemoteConfigs: [[url: "${env.SERVER_URL}", name: 'origin', refspec: "+refs/heads/${env.SERVER_BRANCH}:refs/remotes/origin/${env.SERVER_BRANCH}"]]])
        //     dir ("milvus") {
        //         load "ci/jenkins/step/deploySingle2Dev.groovy"
        //     }
        // }
    } catch (exc) {
        echo 'Deploy Milvus Server Failed !'
        throw exc
    }
}

