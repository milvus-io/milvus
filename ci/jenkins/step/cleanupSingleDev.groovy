try {
    def helmResult = sh script: "helm status ${env.HELM_RELEASE_NAME}", returnStatus: true
    if (!helmResult) {
        sh "helm del --purge ${env.HELM_RELEASE_NAME}"
    }
} catch (exc) {
    def helmResult = sh script: "helm status ${env.HELM_RELEASE_NAME}", returnStatus: true
    if (!helmResult) {
        sh "helm del --purge ${env.HELM_RELEASE_NAME}"
    }
    throw exc
}
