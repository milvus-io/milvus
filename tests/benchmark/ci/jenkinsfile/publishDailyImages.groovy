timeout(time: 30, unit: 'MINUTES') {
    def imageName = "milvus/engine:${DOCKER_VERSION}"
    def remoteImageName = "milvusdb/daily-build:${REMOTE_DOCKER_VERSION}"
    def localDockerRegistryImage = "${params.LOCAL_DOKCER_REGISTRY_URL}/${imageName}"
    def remoteDockerRegistryImage = "${params.REMOTE_DOKCER_REGISTRY_URL}/${remoteImageName}"
    try {
        deleteImages("${localDockerRegistryImage}", true)

        def pullSourceImageStatus = sh(returnStatus: true, script: "docker pull ${localDockerRegistryImage}")

        if (pullSourceImageStatus == 0) {
            def renameImageStatus = sh(returnStatus: true, script: "docker tag ${localDockerRegistryImage} ${remoteImageName} && docker rmi ${localDockerRegistryImage}")
            def sourceImage = docker.image("${remoteImageName}")
            docker.withRegistry("https://${params.REMOTE_DOKCER_REGISTRY_URL}", "${params.REMOTE_DOCKER_CREDENTIALS_ID}") {
                sourceImage.push()
                sourceImage.push("${REMOTE_DOCKER_LATEST_VERSION}")
            }
        } else {
            echo "\"${localDockerRegistryImage}\" image does not exist !"
        }
    } catch (exc) {
        throw exc
    } finally {
        deleteImages("${localDockerRegistryImage}", true)
        deleteImages("${remoteDockerRegistryImage}", true)
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
