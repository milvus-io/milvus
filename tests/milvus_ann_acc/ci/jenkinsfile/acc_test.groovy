timeout(time: 7200, unit: 'MINUTES') {
    try {
        dir ("milvu_ann_acc") {
            print "Git clone url: ${TEST_URL}:${TEST_BRANCH}"
            checkout([$class: 'GitSCM', branches: [[name: "${TEST_BRANCH}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_USER}", url: "${TEST_URL}", name: 'origin', refspec: "+refs/heads/${TEST_BRANCH}:refs/remotes/origin/${TEST_BRANCH}"]]])
            print "Install requirements"
            sh 'python3 -m pip install -r requirements.txt -i http://pypi.douban.com/simple --trusted-host pypi.douban.com'
            // sleep(120000)
            sh "python3 main.py --suite=${params.SUITE} --host=acc-test-${env.JOB_NAME}-${env.BUILD_NUMBER}-engine.milvus.svc.cluster.local --port=19530"
        }
    } catch (exc) {
        echo 'Milvus Ann Accuracy Test Failed !'
        throw exc
    }
}

