container('deploy-env') {
    def helmStatus = sh script: "helm status -n ${env.HELM_RELEASE_NAMESPACE} ${env.HELM_RELEASE_NAME}", returnStatus: true
    if (!helmStatus) {
        sh "helm uninstall -n ${env.HELM_RELEASE_NAMESPACE} ${env.HELM_RELEASE_NAME}"
        def labels = "app.kubernetes.io/instance=${env.HELM_RELEASE_NAME}"
        sh "kubectl delete pvc -n ${env.HELM_RELEASE_NAMESPACE} \$(kubectl get pvc -n ${env.HELM_RELEASE_NAMESPACE} -l ${labels} -o jsonpath='{range.items[*]}{.metadata.name} ')"
    }
}
