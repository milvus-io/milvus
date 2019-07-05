container('publish-docker') {
    timeout(time: 15, unit: 'MINUTES') {
        gitlabCommitStatus(name: 'Publish Engine Docker') {
            try {
                dir ("${PROJECT_NAME}_build") {
                    checkout([$class: 'GitSCM', branches: [[name: "${SEMVER}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_USER}", url: "git@192.168.1.105:build/milvus_build.git"]]])
                    dir ("docker/deploy/ubuntu16.04/free_version") {
                        sh "curl -O -u anonymous: ftp://192.168.1.126/data/${PROJECT_NAME}/engine/${JOB_NAME}-${BUILD_ID}/${PROJECT_NAME}-engine-${PACKAGE_VERSION}.tar.gz"
                        sh "tar zxvf ${PROJECT_NAME}-engine-${PACKAGE_VERSION}.tar.gz"
                        try {
                            docker.withRegistry('https://registry.zilliz.com', 'a54e38ef-c424-4ea9-9224-b25fc20e3924') {
                                def customImage = docker.build("${PROJECT_NAME}/engine:${DOCKER_VERSION}")
                                customImage.push()
                            }
                            echo "Docker Pull Command: docker pull registry.zilliz.com/${PROJECT_NAME}/engine:${DOCKER_VERSION}"
                        } catch (exc) {
                            updateGitlabCommitStatus name: 'Publish Engine Docker', state: 'canceled'
                            throw exc
                        } finally {
                            sh "docker rmi ${PROJECT_NAME}/engine:${DOCKER_VERSION}"
                        }
                    }
                }
            } catch (exc) {
                updateGitlabCommitStatus name: 'Publish Engine Docker', state: 'failed'
                echo 'Publish docker failed!'
                throw exc
            }
        }
    }
}
