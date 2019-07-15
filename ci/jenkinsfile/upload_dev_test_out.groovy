timeout(time: 5, unit: 'MINUTES') {
    dir ("${PROJECT_NAME}_test") {
        if (fileExists('test_out')) {
            def fileTransfer = load "${env.WORKSPACE}/ci/function/file_transfer.groovy"
            fileTransfer.FileTransfer("test_out/", "${PROJECT_NAME}/test/${JOB_NAME}-${BUILD_ID}", 'nas storage')
            if (currentBuild.resultIsBetterOrEqualTo('SUCCESS')) {
                echo "Milvus Dev Test Out Viewer \"ftp://192.168.1.126/data/${PROJECT_NAME}/test/${JOB_NAME}-${BUILD_ID}\""
            }
        } else {
            error("Milvus Dev Test Out directory don't exists!")
        }
    }
}
