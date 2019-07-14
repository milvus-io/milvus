try {
    sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}"
} catch (exc) {
    sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}"
    updateGitlabCommitStatus name: 'Cleanup Dev', state: 'failed'
    throw exc
}

