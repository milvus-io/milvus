container('milvus-testframework') {
    timeout(time: 10, unit: 'MINUTES') {
        gitlabCommitStatus(name: 'Dev Test') {
            try {
                dir ("${PROJECT_NAME}_test") {
                    checkout([$class: 'GitSCM', branches: [[name: "${SEMVER}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_USER}", url: "git@192.168.1.105:Test/milvus_test.git", name: 'origin', refspec: "+refs/heads/${SEMVER}:refs/remotes/origin/${SEMVER}"]]])
                    sh 'python3 -m pip install -r requirements.txt'
                    sh "pytest . --alluredir=cluster_test_out --ip ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster-milvus-cluster-proxy.milvus-cluster.svc.cluster.local"
                }
            } catch (exc) {
                sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster"
                updateGitlabCommitStatus name: 'Dev Test', state: 'failed'
                currentBuild.result = 'FAILURE'
                echo 'Milvus Test Failed !'
            }
        }
    }
}

