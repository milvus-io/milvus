try {
    def result = sh script: "helm status benchmark-test-${env.JOB_NAME}-${env.BUILD_NUMBER}", returnStatus: true
    if (!result) {
        sh "helm del --purge benchmark-test-${env.JOB_NAME}-${env.BUILD_NUMBER}"
    }
} catch (exc) {
    def result = sh script: "helm status benchmark-test-${env.JOB_NAME}-${env.BUILD_NUMBER}", returnStatus: true
    if (!result) {
        sh "helm del --purge benchmark-test-${env.JOB_NAME}-${env.BUILD_NUMBER}"
    }
    throw exc
}

