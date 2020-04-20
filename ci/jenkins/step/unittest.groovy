timeout(time: 30, unit: 'MINUTES') {
    dir ("ci/scripts") {
        sh "./run_unittest.sh -i ${env.MILVUS_INSTALL_PREFIX} --mysql_user=root --mysql_password=123456 --mysql_host=\$POD_IP"
    }
}
