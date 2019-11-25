timeout(time: 5, unit: 'MINUTES') {
    dir ("ci/jenkins/scripts") {
        sh "pip3 install -r requirements.txt"
        sh "./yaml_processor.py merge -f /opt/milvus/conf/server_config.yaml -m ../yaml/update_server_config.yaml -i && rm /opt/milvus/conf/server_config.yaml.bak"
        sh "sed -i 's/\\/tmp\\/milvus/\\/opt\\/milvus/g' /opt/milvus/conf/log_config.conf"
    }
    sh "tar -zcvf ./${PROJECT_NAME}-${PACKAGE_VERSION}.tar.gz -C /opt/ milvus"
    withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'JFROG_USERNAME', passwordVariable: 'JFROG_PASSWORD')]) {
        def uploadStatus = sh(returnStatus: true, script: "curl -u${JFROG_USERNAME}:${JFROG_PASSWORD} -T ./${PROJECT_NAME}-${PACKAGE_VERSION}.tar.gz ${params.JFROG_ARTFACTORY_URL}/milvus/package/${PROJECT_NAME}-${PACKAGE_VERSION}.tar.gz")
        if (uploadStatus != 0) {
            error("\" ${PROJECT_NAME}-${PACKAGE_VERSION}.tar.gz \" upload to \" ${params.JFROG_ARTFACTORY_URL}/milvus/package/${PROJECT_NAME}-${PACKAGE_VERSION}.tar.gz \" failed!")
        }
    }
}
