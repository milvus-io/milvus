timeout(time: 30, unit: 'MINUTES') {
    dir ("milvus/ci/scripts") {
        sh "./coverage.sh -o /opt/milvus -u root -p 123456 -t \$POD_IP"
    }
}