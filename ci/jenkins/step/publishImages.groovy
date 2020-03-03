timeout(time: 15, unit: 'MINUTES') {
    dir ("docker/deploy/${BINARY_VERSION}/${OS_NAME}") {
        def binaryPackage = "${PROJECT_NAME}-${PACKAGE_VERSION}.tar.gz"

        withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'JFROG_USERNAME', passwordVariable: 'JFROG_PASSWORD')]) {
            def downloadStatus = sh(returnStatus: true, script: "curl -u${JFROG_USERNAME}:${JFROG_PASSWORD} -O ${params.JFROG_ARTFACTORY_URL}/milvus/package/${binaryPackage}")

            if (downloadStatus != 0) {
                error("\" Download \" ${params.JFROG_ARTFACTORY_URL}/milvus/package/${binaryPackage} \" failed!")
            }
        }
        sh "tar zxvf ${binaryPackage}"
        def imageName = "${PROJECT_NAME}/engine:${DOCKER_VERSION}"

        try {
            deleteImages("${imageName}", true)

            def customImage = docker.build("${imageName}")

            deleteImages("${params.DOKCER_REGISTRY_URL}/${imageName}", true)

            docker.withRegistry("https://${params.DOKCER_REGISTRY_URL}", "${params.DOCKER_CREDENTIALS_ID}") {
                customImage.push()
            }
        } catch (exc) {
            throw exc
        } finally {
            deleteImages("${imageName}", true)
            deleteImages("${params.DOKCER_REGISTRY_URL}/${imageName}", true)
        }
    }
}

boolean deleteImages(String imageName, boolean force) {
    def imageNameStr = imageName.trim()
    def isExistImage = sh(returnStatus: true, script: "docker inspect --type=image ${imageNameStr} 2>&1 > /dev/null")
    if (isExistImage == 0) {
        def deleteImageStatus = 0
        if (force) {
            def imageID = sh(returnStdout: true, script: "docker inspect --type=image --format \"{{.ID}}\" ${imageNameStr}")
            deleteImageStatus = sh(returnStatus: true, script: "docker rmi -f ${imageID}")
        } else {
            deleteImageStatus = sh(returnStatus: true, script: "docker rmi ${imageNameStr}")
        }

        if (deleteImageStatus != 0) {
            return false
        }
    }
    return true
}

