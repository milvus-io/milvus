timeout(time: 12, unit: 'HOURS') {
    try {
        dir ("milvus-helm") {
            // sh 'helm init --client-only --skip-refresh --stable-repo-url https://kubernetes.oss-cn-hangzhou.aliyuncs.com/charts'
            // sh 'helm repo update'
            checkout([$class: 'GitSCM', branches: [[name: "${HELM_BRANCH}"]], userRemoteConfigs: [[url: "${HELM_URL}", name: 'origin', refspec: "+refs/heads/${HELM_BRANCH}:refs/remotes/origin/${HELM_BRANCH}"]]])
        }
        dir ("milvus_benchmark") {
            print "Git clone url: ${TEST_URL}:${TEST_BRANCH}"
            checkout([$class: 'GitSCM', branches: [[name: "${TEST_BRANCH}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_USER}", url: "${TEST_URL}", name: 'origin', refspec: "+refs/heads/${TEST_BRANCH}:refs/remotes/origin/${TEST_BRANCH}"]]])
            print "Install requirements"
            // sh "python3 -m pip install -r requirements.txt -i http://pypi.douban.com/simple --trusted-host pypi.douban.com"
            sh "python3 -m pip install -r requirements.txt"
            sh "python3 -m pip install git+${TEST_LIB_URL}"
            sh "python3 main.py --image-version=${params.IMAGE_VERSION} --schedule-conf=scheduler/${params.SHARDS_CONFIG_FILE} --deploy-mode=${params.DEPLOY_MODE}"
        }
    } catch (exc) {
        echo 'Deploy SHARDS Test Failed !'
        throw exc
    }
}
