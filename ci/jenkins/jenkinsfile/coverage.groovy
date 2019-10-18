timeout(time: 60, unit: 'MINUTES') {
    dir ("ci/jenkins/scripts") {
        sh "./coverage.sh -o /opt/milvus -u root -p 123456 -t \$POD_IP"
        // Set some env variables so codecov detection script works correctly
        sh 'bash <(curl -s https://codecov.io/bash) -f output_new.info || echo "Codecov did not collect coverage reports"'
    }
}

