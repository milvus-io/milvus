def FileTransfer (sourceFiles, remoteDirectory, remoteIP, protocol = "ftp", makeEmptyDirs = true) {
    if (protocol == "ftp") {
        ftpPublisher masterNodeName: '', paramPublish: [parameterName: ''], alwaysPublishFromMaster: false, continueOnError: false, failOnError: true, publishers: [
            [configName: "${remoteIP}", transfers: [
                [asciiMode: false, cleanRemote: false, excludes: '', flatten: false, makeEmptyDirs: "${makeEmptyDirs}", noDefaultExcludes: false, patternSeparator: '[, ]+', remoteDirectory: "${remoteDirectory}", remoteDirectorySDF: false, removePrefix: '', sourceFiles: "${sourceFiles}"]], usePromotionTimestamp: true, useWorkspaceInPromotion: false, verbose: true
                ]
            ]
    }
}
return this
