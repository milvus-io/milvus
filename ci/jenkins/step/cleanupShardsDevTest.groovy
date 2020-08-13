retry(3) {
    def helmResult = sh script: "helm status -n milvus ${env.SHARDS_HELM_RELEASE_NAME}", returnStatus: true
    if (!helmResult) {
        sh "helm uninstall -n milvus ${env.SHARDS_HELM_RELEASE_NAME}"
    }
}
