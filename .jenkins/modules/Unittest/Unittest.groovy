timeout(time: 30, unit: 'MINUTES') {
    dir ("ci/scripts") {
        sh "./run_unittest.sh -i ${env.MILVUS_INSTALL_PREFIX}"
    }
}
