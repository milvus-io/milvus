timeout(time: 30, unit: 'MINUTES') {
    dir ("ci/scripts") {
        sh "./coverage.sh -o ${env.MILVUS_INSTALL_PREFIX} -u root -p 123456 -t \$POD_IP"
    }
}

