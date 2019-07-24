timeout(time: 10, unit: 'MINUTES') {
    try {
        dir ("${PROJECT_NAME}_test") {
            checkout([$class: 'GitSCM', branches: [[name: "${SEMVER}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_USER}", url: "git@192.168.1.105:Test/milvus_test.git", name: 'origin', refspec: "+refs/heads/${SEMVER}:refs/remotes/origin/${SEMVER}"]]])
            sh 'python3 -m pip install -r requirements_cluster.txt'
            sh "pytest . --alluredir=cluster_test_out --ip ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster-milvus-cluster-proxy.milvus-cluster.svc.cluster.local"
        }
    } catch (exc) {
        echo 'Milvus Cluster Test Failed !'
        throw exc
    }
}
