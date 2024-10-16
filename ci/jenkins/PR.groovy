@Library('jenkins-shared-library@v0.61.0') _

def pod = libraryResource 'io/milvus/pod/tekton-4am.yaml'
def milvus_helm_chart_version = '4.2.8'

pipeline {
    options {
        skipDefaultCheckout true
        parallelsAlwaysFailFast()
        buildDiscarder logRotator(artifactDaysToKeepStr: '30')
        preserveStashes(buildCount: 5)
        disableConcurrentBuilds(abortPrevious: true)
    }
    agent {
        kubernetes {
            cloud '4am'
            yaml pod
        }
    }
    stages {
        stage('meta') {
            steps {
                container('jnlp') {
                    script {
                        isPr = env.CHANGE_ID != null
                        gitMode = isPr ? 'merge' : 'fetch'
                        gitBaseRef = isPr ? "$env.CHANGE_TARGET" : "$env.BRANCH_NAME"

                        get_helm_release_name =  tekton.helm_release_name client: 'py',
                                                             changeId: "${env.CHANGE_ID}",
                                                             buildId:"${env.BUILD_ID}"
                    }
                }
            }
        }
        stage('build') {
            steps {
                container('jnlp') {
                    script {
                        echo 'hello'
                        def changeLogSets = currentBuild.changeSets
                        echo "${changeLogSets}"
                        for (int i = 0; i < changeLogSets.size(); i++) {
                            def entries = changeLogSets[i].items
                            for (int j = 0; j < entries.length; j++) {
                                def entry = entries[j]
                                def files = new ArrayList(entry.affectedFiles)
                                for (int k = 0; k < files.size(); k++) {
                                    def file = files[k]
                                    echo "File affected: ${file.path}"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
