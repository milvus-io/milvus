try {
    def helmResult = sh script: "helm status -n milvus ${env.HELM_RELEASE_NAME}", returnStatus: true
    if (!helmResult) {
        sh "helm uninstall -n milvus ${env.HELM_RELEASE_NAME}"
    }
} catch (exc) {
    def helmResult = sh script: "helm status -n milvus ${env.HELM_RELEASE_NAME}", returnStatus: true
    if (!helmResult) {
        sh "helm uninstall -n milvus ${env.HELM_RELEASE_NAME}"
    }
    throw exc
}
