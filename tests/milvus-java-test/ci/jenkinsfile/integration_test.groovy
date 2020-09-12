timeout(time: 30, unit: 'MINUTES') {
    try {
        dir ("milvus-java-test") {
            sh "mvn clean install"
            sh "java -cp \"target/MilvusSDkJavaTest-1.0-SNAPSHOT.jar:lib/*\" com.MainClass -h ${env.JOB_NAME}-${env.BUILD_NUMBER}-milvus-gpu-engine.milvus-sdk-test.svc.cluster.local"
        }

    } catch (exc) {
        echo 'Milvus-SDK-Java Integration Test Failed !'
        throw exc
    }
}

