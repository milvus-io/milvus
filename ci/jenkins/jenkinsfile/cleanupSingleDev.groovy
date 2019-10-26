try {
    def helmResult = sh script: "helm status ${env.PIPELINE_NAME}-${env.BUILD_NUMBER}-single-gpu", returnStatus: true
    if (!helmResult) {
        sh "helm del --purge ${env.PIPELINE_NAME}-${env.BUILD_NUMBER}-single-gpu"
    }
} catch (exc) {
    def helmResult = sh script: "helm status ${env.PIPELINE_NAME}-${env.BUILD_NUMBER}-single-gpu", returnStatus: true
    if (!helmResult) {
        sh "helm del --purge ${env.PIPELINE_NAME}-${env.BUILD_NUMBER}-single-gpu"
    }
    throw exc
}
