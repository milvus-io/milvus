timeout(time: 60, unit: 'MINUTES') {
	dir ("ci/scripts") {
		if ("${BINARY_VERSION}" == "gpu") {
			sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -j4 -i ${env.MILVUS_INSTALL_PREFIX} -l -g -u -n\""
		} else {
			sh "/bin/bash --login -c \". ./before-install.sh && ./build.sh -t ${params.BUILD_TYPE} -j4 -i ${env.MILVUS_INSTALL_PREFIX} -l -u -n\""
		}
	}
}