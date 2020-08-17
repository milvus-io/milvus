retry(3) {
    def helmResult = sh script: "helm status -n milvus ${env.HELM_RELEASE_NAME}", returnStatus: true
    if (!helmResult) {
        sh "helm uninstall -n milvus ${env.HELM_RELEASE_NAME} && sleep 1m"
    }
}
