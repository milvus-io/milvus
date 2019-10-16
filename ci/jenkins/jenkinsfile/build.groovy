timeout(time: 60, unit: 'MINUTES') {
    dir ("core") {
        sh "git config --global user.email \"test@zilliz.com\""
        sh "git config --global user.name \"test\""
        // sh "./build.sh -l"
        withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
            sh "export JFROG_ARTFACTORY_URL='${params.JFROG_ARTFACTORY_URL}' && export JFROG_USER_NAME='${USERNAME}' && export JFROG_PASSWORD='${PASSWORD}' && ./build.sh -t ${params.BUILD_TYPE} -d /opt/milvus -j -u"
        }
    }
}

