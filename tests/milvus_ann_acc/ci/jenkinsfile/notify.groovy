def notify() {
    if (!currentBuild.resultIsBetterOrEqualTo('SUCCESS')) {
        // Send an email only if the build status has changed from green/unstable to red
        emailext subject: '$DEFAULT_SUBJECT',
        body: '$DEFAULT_CONTENT',
        recipientProviders: [
            [$class: 'DevelopersRecipientProvider'],
            [$class: 'RequesterRecipientProvider']
        ], 
        replyTo: '$DEFAULT_REPLYTO',
        to: '$DEFAULT_RECIPIENTS'
    }
}
return this

