timeout(time: 60, unit: 'MINUTES') {
    dir ("ci/scripts") {
        if ("${env.BINRARY_VERSION}" == "gpu") {
            if ("${params.IS_ORIGIN_FAISS}" == "False") {
                sh "./build.sh -o ${env.FAISS_ROOT_PATH} -i -g"
            } else {
                sh "wget https://github.com/facebookresearch/faiss/archive/v${env.NATIVE_FAISS_VERSION}.tar.gz && \
                        tar zxvf v${env.NATIVE_FAISS_VERSION}.tar.gz"
                sh "./build.sh -o ${env.FAISS_ROOT_PATH} -s ./faiss-${env.NATIVE_FAISS_VERSION} -i -g"
            }
        } else {
            sh "wget https://github.com/facebookresearch/faiss/archive/v${env.NATIVE_FAISS_VERSION}.tar.gz && \
                    tar zxvf v${env.NATIVE_FAISS_VERSION}.tar.gz"
            sh "./build.sh -o ${env.FAISS_ROOT_PATH} -s ./faiss-${env.NATIVE_FAISS_VERSION} -i"
        }
    }

    dir ("milvus") {
        dir ("ci/scripts") {
            withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
                def checkResult = sh(script: "./check_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache", returnStatus: true)
                if ("${env.BINRARY_VERSION}" == "gpu") {
                    if ("${params.IS_ORIGIN_FAISS}" == "False") {
                        sh ". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -o /opt/milvus -f ${env.FAISS_ROOT_PATH} -l -m -g -x -u -c"
                    } else {
                        sh ". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -o /opt/milvus -f ${env.FAISS_ROOT_PATH} -l -m -g -u -c"
                    }
                } else {
                    sh ". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -o /opt/milvus -f ${env.FAISS_ROOT_PATH} -l -m -u -c"
                }
                sh "./update_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache -u ${USERNAME} -p ${PASSWORD}"
            }
        }
    }
}
