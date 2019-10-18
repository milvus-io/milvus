timeout(time: 60, unit: 'MINUTES') {
    dir ("core") {
        sh "./build.sh -l"
        withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
            sh "export JFROG_ARTFACTORY_URL='${params.JFROG_ARTFACTORY_URL}' && export JFROG_USER_NAME='${USERNAME}' && export JFROG_PASSWORD='${PASSWORD}' && ./build.sh -t ${params.BUILD_TYPE} -o /opt/milvus -d /opt/milvus -j -u -c"
        }
    }
}

