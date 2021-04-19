try {
    def result = sh script: "helm status -n milvus ${env.HELM_SHARDS_RELEASE_NAME}", returnStatus: true
    if (!result) {
        sh "helm uninstall -n milvus ${env.HELM_SHARDS_RELEASE_NAME}"
    }
} catch (exc) {
    def result = sh script: "helm status -n milvus ${env.HELM_SHARDS_RELEASE_NAME}", returnStatus: true
    if (!result) {
        sh "helm uninstall -n milvus ${env.HELM_SHARDS_RELEASE_NAME}"
    }
    throw exc
}

