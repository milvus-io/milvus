dir ("tests/milvus_python_test") {
    // sh 'python3 -m pip install -r requirements.txt -i http://pypi.douban.com/simple --trusted-host pypi.douban.com'
    sh 'python3 -m pip install -r requirements.txt'
    sh "pytest . --alluredir=\"test_out/dev/single/sqlite\" --level=1 --ip ${env.HELM_RELEASE_NAME}.milvus.svc.cluster.local"
}
