try {
    def result = sh script: "helm status ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster", returnStatus: true
    if (!result) {
        sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster"
    }
} catch (exc) {
    def result = sh script: "helm status ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster", returnStatus: true
    if (!result) {
        sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster"
    }
    throw exc
}

