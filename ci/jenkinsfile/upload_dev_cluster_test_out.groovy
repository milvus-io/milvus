container('milvus-testframework') {
    timeout(time: 5, unit: 'MINUTES') {
        dir ("${PROJECT_NAME}_test") {
            gitlabCommitStatus(name: 'Upload Dev Test Out') {
                if (fileExists('cluster_test_out')) {
                    try {
                        def fileTransfer = load "${env.WORKSPACE}/ci/function/file_transfer.groovy"
                        fileTransfer.FileTransfer("cluster_test_out/", "${PROJECT_NAME}/test/${JOB_NAME}-${BUILD_ID}", 'nas storage')
                        if (currentBuild.resultIsBetterOrEqualTo('SUCCESS')) {
                            echo "Milvus Dev Test Out Viewer \"ftp://192.168.1.126/data/${PROJECT_NAME}/test/${JOB_NAME}-${BUILD_ID}\""
                        }
                    } catch (hudson.AbortException ae) {
                        updateGitlabCommitStatus name: 'Upload Dev Test Out', state: 'canceled'
                        currentBuild.result = 'ABORTED'
                    } catch (exc) {
                        updateGitlabCommitStatus name: 'Upload Dev Test Out', state: 'failed'
                        currentBuild.result = 'FAILURE'
                    }
                } else {
                    updateGitlabCommitStatus name: 'Upload Dev Test Out', state: 'failed'
                    echo "Milvus Dev Test Out directory don't exists!"
                }
            }
        }
    }
}
