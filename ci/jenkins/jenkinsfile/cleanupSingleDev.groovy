try {
    def jobNames = env.JOB_NAME.split('/')
    sh "helm del --purge ${env.PIPELIEN_NAME}-${env.BUILD_NUMBER}-single-gpu"
} catch (exc) {
    def helmResult = sh script: "helm status ${env.PIPELIEN_NAME}-${env.BUILD_NUMBER}-single-gpu", returnStatus: true
    if (!helmResult) {
        sh "helm del --purge ${env.PIPELIEN_NAME}-${env.BUILD_NUMBER}-single-gpu"
    }
    throw exc
}
