container('publish-docker') {
    timeout(time: 15, unit: 'MINUTES') {
        gitlabCommitStatus(name: 'Publish Engine Docker') {
            try {
                dir ("${PROJECT_NAME}_build") {
                    checkout([$class: 'GitSCM', branches: [[name: "${SEMVER}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${params.GIT_USER}", url: "git@192.168.1.105:build/milvus_build.git", name: 'origin', refspec: "+refs/heads/${SEMVER}:refs/remotes/origin/${SEMVER}"]]])
                    dir ("docker/deploy/ubuntu16.04/free_version") {
                        sh "curl -O -u anonymous: ftp://192.168.1.126/data/${PROJECT_NAME}/engine/${JOB_NAME}-${BUILD_ID}/${PROJECT_NAME}-engine-${PACKAGE_VERSION}.tar.gz"
                        sh "tar zxvf ${PROJECT_NAME}-engine-${PACKAGE_VERSION}.tar.gz"
                        try {
                            def customImage = docker.build("${PROJECT_NAME}/engine:${DOCKER_VERSION}")
                            docker.withRegistry('https://registry.zilliz.com', "${params.DOCKER_PUBLISH_USER}") {
                                customImage.push()
                            }
                            docker.withRegistry('https://zilliz.azurecr.cn', "${params.AZURE_DOCKER_PUBLISH_USER}") {
                                customImage.push()
                            }
                            if (currentBuild.resultIsBetterOrEqualTo('SUCCESS')) {
                                updateGitlabCommitStatus name: 'Publish Engine Docker', state: 'success'
                                echo "Docker Pull Command: docker pull registry.zilliz.com/${PROJECT_NAME}/engine:${DOCKER_VERSION}"
                            }
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

