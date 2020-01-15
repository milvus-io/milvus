timeout(time: 30, unit: 'MINUTES') {
    dir ("ci/scripts") {
        sh "./coverage.sh -o ${env.MILVUS_INSTALL_PREFIX} -u root -p 123456 -t \$POD_IP"
        if (isNightlyTest) {
	        withCredentials([[$class: 'StringBinding', credentialsId: "milvus-ci-codecov-token", variable: 'CODECOV_TOKEN']]) {
	            sh "curl -s https://codecov.io/bash | bash -s - -f output_new.info -n ${BINARY_VERSION}-version-${OS_NAME}-unittest -F nightly -F ${BINARY_VERSION}-version-${OS_NAME}-unittest || echo \"Codecov did not collect coverage reports\""
	        }
	    } else {
            withCredentials([[$class: 'StringBinding', credentialsId: "milvus-ci-codecov-token", variable: 'CODECOV_TOKEN']]) {
	            sh "curl -s https://codecov.io/bash | bash -s - -f output_new.info -n ${BINARY_VERSION}-version-${OS_NAME}-unittest -F ${BINARY_VERSION}-version-${OS_NAME}-unittest || echo \"Codecov did not collect coverage reports\""
	        }
	    }
    }
}

