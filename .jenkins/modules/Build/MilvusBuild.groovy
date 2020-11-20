timeout(time: 60, unit: 'MINUTES') {
	dir ("ci/scripts") {
		def isTimeTriggeredBuild = currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause').size() != 0
		if (!isTimeTriggeredBuild) {
			sh ". ./before-install.sh && ./check_cache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache --cache_dir=\${CCACHE_DIR} -f \${CCACHE_COMPRESS_PACKAGE_FILE} || echo \"ccache files not found!\""
			sh ". ./before-install.sh && ./check_cache.sh -l ${params.JFROG_ARTFACTORY_URL}/thirdparty --cache_dir=\${CUSTOM_THIRDPARTY_DOWNLOAD_PATH} -f \${THIRDPARTY_COMPRESS_PACKAGE_FILE} || echo \"thirdparty files not found!\""
		}

		if ("${BINARY_VERSION}" == "gpu") {
			sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -j4 -i ${env.MILVUS_INSTALL_PREFIX} --coverage -g -l -u\""
		} else {
			sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -j4 -i ${env.MILVUS_INSTALL_PREFIX} --coverage -l -u\""
		}

		withCredentials([usernamePassword(credentialsId: "${params.JFROG_CREDENTIALS_ID}", usernameVariable: 'USERNAME', passwordVariable: 'PASSWORD')]) {
			sh '. ./before-install.sh && ./update_cache.sh -l ${params.JFROG_ARTFACTORY_URL}/ccache --cache_dir=\${CCACHE_DIR} -f \${CCACHE_COMPRESS_PACKAGE_FILE} -u $USERNAME -p $PASSWORD'
			if (isTimeTriggeredBuild) {
				sh '. ./before-install.sh && ./update_cache.sh -l ${params.JFROG_ARTFACTORY_URL}/thirdparty --cache_dir=\${CUSTOM_THIRDPARTY_DOWNLOAD_PATH} -f \${THIRDPARTY_COMPRESS_PACKAGE_FILE} -u $USERNAME -p $PASSWORD'
			}
		}
	}
}
