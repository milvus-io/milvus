try {
    sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster"
} catch (exc) {
    sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster"
    updateGitlabCommitStatus name: 'Cleanup Dev', state: 'failed'
    throw exc
}

