timeout(time: 60, unit: 'MINUTES') {
	dir ("ci/scripts") {
		def isTimeTriggeredBuild = currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause').size() != 0
		if (!isTimeTriggeredBuild) {
			sh "./check_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache || echo \"ccache files not found!\""
		}

		if ("${BINARY_VERSION}" == "gpu") {
			sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -j4 ${env.MILVUS_INSTALL_PREFIX} --with_fiu --coverage -l -g -u\""
		} else {
			sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -j4 ${env.MILVUS_INSTALL_PREFIX} --with_fiu --coverage -l -u\""
		}

		withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
			sh "./update_ccache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache -u ${USERNAME} -p ${PASSWORD}"
		}
	}
}
