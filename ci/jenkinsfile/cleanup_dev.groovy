try {
    sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}"

    if (currentBuild.result == 'ABORTED') {
        throw new hudson.AbortException("Dev Test Aborted !")
    } else if (currentBuild.result == 'FAILURE') {
        error("Dev Test Failure !")
    }
} catch (exc) {
    sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}"
    updateGitlabCommitStatus name: 'Cleanup Dev', state: 'failed'
    throw exc
}

