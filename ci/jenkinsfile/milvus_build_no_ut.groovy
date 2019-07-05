container('milvus-build-env') {
    timeout(time: 20, unit: 'MINUTES') {
        gitlabCommitStatus(name: 'Build Engine') {
            dir ("milvus_engine") {
                try {
                    checkout([$class: 'GitSCM', branches: [[name: "${SEMVER}"]], doGenerateSubmoduleConfigurations: false, extensions: [[$class: 'SubmoduleOption',disableSubmodules: false,parentCredentials: true,recursiveSubmodules: true,reference: '',trackingSubmodules: false]], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_USER}", url: "git@192.168.1.105:megasearch/milvus.git"]]])
                    dir ("cpp") {
                        sh "./build.sh -t ${params.BUILD_TYPE}"
                    }
                } catch (exc) {
                    updateGitlabCommitStatus name: 'Build Engine', state: 'failed'
                    throw exc
                }
            }
        }
    }
}
