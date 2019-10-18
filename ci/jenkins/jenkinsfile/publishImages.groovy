container('publish-images') {
    timeout(time: 15, unit: 'MINUTES') {
        dir ("docker/deploy/${OS_NAME}") {
            def binaryPackage = "${PROJECT_NAME}-${PACKAGE_VERSION}.tar.gz"
            def dockerRegistryURL = "registry.zilliz.com"

            withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'JFROG_USERNAME', passwordVariable: 'JFROG_PASSWORD')]) {
                def downloadStatus = sh(returnStatus: true, script: "curl -u${JFROG_USERNAME}:${JFROG_PASSWORD} -O ${params.JFROG_ARTFACTORY_URL}/milvus/package/${binaryPackage}")

                if (downloadStatus != 0) {
                    error("\" Download \" ${params.JFROG_ARTFACTORY_URL}/milvus/package/${binaryPackage} \" failed!")
                }
            }
            sh "tar zxvf ${binaryPackage}"
            def imageName = "${PROJECT_NAME}/engine:${DOCKER_VERSION}"

            try {
                def isExistImage = sh(returnStatus: true, script: "docker inspect --type=image ${imageName} 2>&1 > /dev/null")
                if (isExistImage == 0) {
                    sh "docker rmi ${imageName}"
                }

                def customImage = docker.build("${imageName}")
                docker.withRegistry("https://${dockerRegistryURL}", "${params.DOCKER_CREDENTIALS_ID}") {
                    customImage.push()
                }
            } catch (exc) {
                throw exc
            } finally {
                def isExistImage = sh(returnStatus: true, script: "docker inspect --type=image ${imageName} 2>&1 > /dev/null")
                if (isExistImage == 0) {
                    sh "docker rmi ${imageName}"
                }
            }
        } 
    }
}
