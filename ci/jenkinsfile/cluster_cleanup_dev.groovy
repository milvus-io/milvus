try {
    sh "helm del --purge ${env.JOB_NAME}-${env.BUILD_NUMBER}-cluster"

    if (currentBuild.result == 'ABORTED') {
        throw new hudson.AbortException("Cluster Dev Test Aborted !")
    } else if (currentBuild.result == 'FAILURE') {
        error("Dev Test Failure !")
    }
} catch (exc) {
    updateGitlabCommitStatus name: 'Cleanup Dev', state: 'failed'
    throw exc
}

