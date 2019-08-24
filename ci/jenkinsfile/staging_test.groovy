timeout(time: 40, unit: 'MINUTES') {
    try {
        dir ("${PROJECT_NAME}_test") {
            checkout([$class: 'GitSCM', branches: [[name: "${SEMVER}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_USER}", url: "git@192.168.1.105:Test/milvus_test.git", name: 'origin', refspec: "+refs/heads/${SEMVER}:refs/remotes/origin/${SEMVER}"]]])
            sh 'python3 -m pip install -r requirements.txt'
            def service_ip = sh (script: "kubectl get svc --namespace milvus-1 ${env.JOB_NAME}-${env.BUILD_NUMBER}-milvus-gpu-engine --template \"{{range .status.loadBalancer.ingress}}{{.ip}}{{end}}\"",returnStdout: true).trim()
            sh "pytest . --alluredir=\"test_out/staging/single/sqlite\" --ip ${service_ip}"
        }

        // mysql database backend test
        load "${env.WORKSPACE}/ci/jenkinsfile/cleanup_staging.groovy"

        if (!fileExists('milvus-helm')) {
            dir ("milvus-helm") {
                checkout([$class: 'GitSCM', branches: [[name: "${SEMVER}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_USER}", url: "git@192.168.1.105:megasearch/milvus-helm.git", name: 'origin', refspec: "+refs/heads/${SEMVER}:refs/remotes/origin/${SEMVER}"]]])
            }
        }
        dir ("milvus-helm") {
            dir ("milvus/milvus-gpu") {
                sh "helm install --wait --timeout 300 --set engine.image.repository=\"zilliz.azurecr.cn/milvus/engine\" --set engine.image.tag=${DOCKER_VERSION} --set expose.type=loadBalancer --name ${env.JOB_NAME}-${env.BUILD_NUMBER} -f ci/db_backend/mysql_values.yaml --namespace milvus-2 --version 0.4.0 ."
            }
        }
        dir ("${PROJECT_NAME}_test") {
            def service_ip = sh (script: "kubectl get svc --namespace milvus-2 ${env.JOB_NAME}-${env.BUILD_NUMBER}-milvus-gpu-engine --template \"{{range .status.loadBalancer.ingress}}{{.ip}}{{end}}\"",returnStdout: true).trim()
            sh "pytest . --alluredir=\"test_out/staging/single/mysql\" --ip ${service_ip}"
        }
    } catch (exc) {
        echo 'Milvus Test Failed !'
        throw exc
    }
}
