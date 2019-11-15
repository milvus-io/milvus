try {
    def helmResult = sh script: "helm status ${env.PIPELINE_NAME}-${env.BUILD_NUMBER}-single-${env.BINRARY_VERSION}", returnStatus: true
    if (!helmResult) {
        sh "helm del --purge ${env.PIPELINE_NAME}-${env.BUILD_NUMBER}-single-${env.BINRARY_VERSION}"
    }
} catch (exc) {
    def helmResult = sh script: "helm status ${env.PIPELINE_NAME}-${env.BUILD_NUMBER}-single-${env.BINRARY_VERSION}", returnStatus: true
    if (!helmResult) {
        sh "helm del --purge ${env.PIPELINE_NAME}-${env.BUILD_NUMBER}-single-${env.BINRARY_VERSION}"
    }
    throw exc
}
