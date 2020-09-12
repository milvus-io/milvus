try {
    def result = sh script: "helm status ${env.JOB_NAME}-${env.BUILD_NUMBER}", returnStatus: true
    if (!result) {
        sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}"
    }
} catch (exc) {
    def result = sh script: "helm status ${env.JOB_NAME}-${env.BUILD_NUMBER}", returnStatus: true
    if (!result) {
        sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}"
    }
    throw exc
}

