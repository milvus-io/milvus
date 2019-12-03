#!/usr/bin/env groovy

pipeline {
    agent none
    
    options {
        timestamps()
    }

    parameters{
        choice choices: ['Release', 'Debug'], description: 'Build Type', name: 'BUILD_TYPE'
        string defaultValue: 'registry.zilliz.com', description: 'DOCKER REGISTRY URL', name: 'DOKCER_REGISTRY_URL', trim: true
        string defaultValue: 'a54e38ef-c424-4ea9-9224-b25fc20e3924', description: 'DOCKER CREDENTIALS ID', name: 'DOCKER_CREDENTIALS_ID', trim: true
        string defaultValue: 'http://192.168.1.201/artifactory/milvus', description: 'JFROG ARTFACTORY URL', name: 'JFROG_ARTFACTORY_URL', trim: true
        string defaultValue: '76fd48ab-2b8e-4eed-834d-2eefd23bb3a6', description: 'JFROG CREDENTIALS ID', name: 'JFROG_CREDENTIALS_ID', trim: true
    }

    environment {
        PROJECT_NAME = "milvus"
        MILVUS_ROOT_PATH="/var/lib"
        MILVUS_INSTALL_PREFIX="${env.MILVUS_ROOT_PATH}/${env.PROJECT_NAME}"
        LOWER_BUILD_TYPE = params.BUILD_TYPE.toLowerCase()
        SEMVER = "${BRANCH_NAME.contains('/') ? BRANCH_NAME.substring(BRANCH_NAME.lastIndexOf('/') + 1) : BRANCH_NAME}"
        PIPELINE_NAME = "${env.JOB_NAME.contains('/') ? env.JOB_NAME.getAt(0..(env.JOB_NAME.indexOf('/') - 1)) : env.JOB_NAME}"
    }

    stages {
        stage("Ubuntu 18.04 x86_64") {
            environment {
                OS_NAME = "ubuntu18.04"
                CPU_ARCH = "amd64"
            }

            parallel {
                stage ("GPU Version") {
                    environment {
                        BINRARY_VERSION = "gpu"
                        PACKAGE_VERSION = VersionNumber([
                            versionNumberString : '${SEMVER}-gpu-${OS_NAME}-${CPU_ARCH}-${LOWER_BUILD_TYPE}-${BUILD_DATE_FORMATTED, "yyyyMMdd"}-${BUILDS_TODAY}'
                        ]);
                        DOCKER_VERSION = "${SEMVER}-gpu-${OS_NAME}-${LOWER_BUILD_TYPE}"
                    }

                    stages {
                        stage("Run Build") {
                            agent {
                                kubernetes {
                                    label "${env.BINRARY_VERSION}-build"
                                    defaultContainer 'jnlp'
                                    yaml """
apiVersion: v1
kind: Pod
metadata:
  name: milvus-gpu-build-env
  labels:
    app: milvus
    componet: gpu-build-env
spec:
  containers:
  - name: milvus-gpu-build-env
    image: registry.zilliz.com/milvus/milvus-gpu-build-env:v0.6.0-ubuntu18.04
    env:
    - name: POD_IP
      valueFrom:
        fieldRef:
          fieldPath: status.podIP
    - name: BUILD_ENV_IMAGE_ID
      value: "da9023b0f858f072672f86483a869aa87e90a5140864f89e5a012ec766d96dea"
    command:
    - cat
    tty: true
    resources:
      limits:
        memory: "24Gi"
        cpu: "8.0"
        nvidia.com/gpu: 1
      requests:
        memory: "16Gi"
        cpu: "4.0"
  - name: milvus-mysql
    image: mysql:5.6
    env:
    - name: MYSQL_ROOT_PASSWORD
      value: 123456
    ports:
    - containerPort: 3306
      name: mysql
                                    """
                                }
                            }

                            stages {
                                stage('Build') {
                                    steps {
                                        container("milvus-${env.BINRARY_VERSION}-build-env") {
                                            script {
                                                load "${env.WORKSPACE}/ci/jenkins/step/build.groovy"
                                            }
                                        }
                                    }
                                }
                                stage('Code Coverage') {
                                    steps {
                                        container("milvus-${env.BINRARY_VERSION}-build-env") {
                                            script {
                                                load "${env.WORKSPACE}/ci/jenkins/step/internalCoverage.groovy"
                                            }
                                        }
                                    }
                                }
                                stage('Upload Package') {
                                    steps {
                                        container("milvus-${env.BINRARY_VERSION}-build-env") {
                                            script {
                                                load "${env.WORKSPACE}/ci/jenkins/step/package.groovy"
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        stage("Publish docker images") {
                            agent {
                                kubernetes {
                                    label "${env.BINRARY_VERSION}-publish"
                                    defaultContainer 'jnlp'
                                    yaml """
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: publish
    componet: docker
spec:
  containers:
  - name: publish-images
    image: registry.zilliz.com/library/docker:v1.0.0
    securityContext:
      privileged: true
    command:
    - cat
    tty: true
    volumeMounts:
    - name: docker-sock
      mountPath: /var/run/docker.sock
  volumes:
  - name: docker-sock
    hostPath:
      path: /var/run/docker.sock
"""
                                }
                            }

                            stages {
                                stage('Publish') {
                                    steps {
                                        container('publish-images') {
                                            script {
                                                load "${env.WORKSPACE}/ci/jenkins/step/publishImages.groovy"
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        stage("Deploy to Development") {
                            environment {
                                FROMAT_SEMVER = "${env.SEMVER}".replaceAll("\\.", "-")
                                HELM_RELEASE_NAME = "${env.PIPELINE_NAME}-${env.FROMAT_SEMVER}-${env.BUILD_NUMBER}-single-${env.BINRARY_VERSION}".toLowerCase()
                            }

                            agent {
                                kubernetes {
                                    label "${env.BINRARY_VERSION}-dev-test"
                                    defaultContainer 'jnlp'
                                    yaml """
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: milvus
    componet: test-env
spec:
  containers:
  - name: milvus-test-env
    image: registry.zilliz.com/milvus/milvus-test-env:v0.1
    command:
    - cat
    tty: true
    volumeMounts:
    - name: kubeconf
      mountPath: /root/.kube/
      readOnly: true
  volumes:
  - name: kubeconf
    secret:
      secretName: test-cluster-config
"""
                                }
                            }

                            stages {
                                stage("Deploy to Dev") {
                                    steps {
                                        container('milvus-test-env') {
                                            script {
                                                load "${env.WORKSPACE}/ci/jenkins/step/deploySingle2Dev.groovy"
                                            }
                                        }
                                    }
                                }

                                stage("Dev Test") {
                                    steps {
                                        container('milvus-test-env') {
                                            script {
                                                boolean isNightlyTest = isTimeTriggeredBuild()
                                                if (isNightlyTest) {
                                                    load "${env.WORKSPACE}/ci/jenkins/step/singleDevNightlyTest.groovy"
                                                } else {
                                                    load "${env.WORKSPACE}/ci/jenkins/step/singleDevTest.groovy"
                                                }
                                            }
                                        }
                                    }
                                }

                                stage ("Cleanup Dev") {
                                    steps {
                                        container('milvus-test-env') {
                                            script {
                                                load "${env.WORKSPACE}/ci/jenkins/step/cleanupSingleDev.groovy"
                                            }
                                        }
                                    }
                                }
                            }
                            post {
                                unsuccessful {
                                    container('milvus-test-env') {
                                        script {
                                            load "${env.WORKSPACE}/ci/jenkins/step/cleanupSingleDev.groovy"
                                        }
                                    }
                                }
                            }
                        }
    				}
                }

                stage ("CPU Version") {
                    environment {
                        BINRARY_VERSION = "cpu"
                        PACKAGE_VERSION = VersionNumber([
                            versionNumberString : '${SEMVER}-cpu-${OS_NAME}-${CPU_ARCH}-${LOWER_BUILD_TYPE}-${BUILD_DATE_FORMATTED, "yyyyMMdd"}-${BUILDS_TODAY}'
                        ]);
                        DOCKER_VERSION = "${SEMVER}-cpu-${OS_NAME}-${LOWER_BUILD_TYPE}"
                    }

                    stages {
                        stage("Run Build") {
                            agent {
                                kubernetes {
                                    label "${env.BINRARY_VERSION}-build"
                                    defaultContainer 'jnlp'
                                    yaml """
apiVersion: v1
kind: Pod
metadata:
  name: milvus-cpu-build-env
  labels:
    app: milvus
    componet: cpu-build-env
spec:
  containers:
  - name: milvus-cpu-build-env
    image: registry.zilliz.com/milvus/milvus-cpu-build-env:v0.6.0-ubuntu18.04
    env:
    - name: POD_IP
      valueFrom:
        fieldRef:
          fieldPath: status.podIP
    - name: BUILD_ENV_IMAGE_ID
      value: "23476391bec80c64f10d44a6370c73c71f011a6b95114b10ff82a60e771e11c7"
    command:
    - cat
    tty: true
    resources:
      limits:
        memory: "24Gi"
        cpu: "8.0"
      requests:
        memory: "16Gi"
        cpu: "4.0"
  - name: milvus-mysql
    image: mysql:5.6
    env:
    - name: MYSQL_ROOT_PASSWORD
      value: 123456
    ports:
    - containerPort: 3306
      name: mysql
                                    """
                                }
                            }

                            stages {
                                stage('Build') {
                                    steps {
                                        container("milvus-${env.BINRARY_VERSION}-build-env") {
                                            script {
                                                load "${env.WORKSPACE}/ci/jenkins/step/build.groovy"
                                            }
                                        }
                                    }
                                }
                                stage('Code Coverage') {
                                    steps {
                                        container("milvus-${env.BINRARY_VERSION}-build-env") {
                                            script {
                                                load "${env.WORKSPACE}/ci/jenkins/step/internalCoverage.groovy"
                                            }
                                        }
                                    }
                                }
                                stage('Upload Package') {
                                    steps {
                                        container("milvus-${env.BINRARY_VERSION}-build-env") {
                                            script {
                                                load "${env.WORKSPACE}/ci/jenkins/step/package.groovy"
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        stage("Publish docker images") {
                            agent {
                                kubernetes {
                                    label "${env.BINRARY_VERSION}-publish"
                                    defaultContainer 'jnlp'
                                    yaml """
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: publish
    componet: docker
spec:
  containers:
  - name: publish-images
    image: registry.zilliz.com/library/docker:v1.0.0
    securityContext:
      privileged: true
    command:
    - cat
    tty: true
    volumeMounts:
    - name: docker-sock
      mountPath: /var/run/docker.sock
  volumes:
  - name: docker-sock
    hostPath:
      path: /var/run/docker.sock
"""
                                }
                            }

                            stages {
                                stage('Publish') {
                                    steps {
                                        container('publish-images'){
                                            script {
                                                load "${env.WORKSPACE}/ci/jenkins/step/publishImages.groovy"
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        stage("Deploy to Development") {
                            environment {
                                FROMAT_SEMVER = "${env.SEMVER}".replaceAll("\\.", "-")
                                HELM_RELEASE_NAME = "${env.PIPELINE_NAME}-${env.FROMAT_SEMVER}-${env.BUILD_NUMBER}-single-${env.BINRARY_VERSION}".toLowerCase()
                            }

                            agent {
                                kubernetes {
                                    label "${env.BINRARY_VERSION}-dev-test"
                                    defaultContainer 'jnlp'
                                    yaml """
apiVersion: v1
kind: Pod
metadata:
  labels:
    app: milvus
    componet: test-env
spec:
  containers:
  - name: milvus-test-env
    image: registry.zilliz.com/milvus/milvus-test-env:v0.1
    command:
    - cat
    tty: true
    volumeMounts:
    - name: kubeconf
      mountPath: /root/.kube/
      readOnly: true
  volumes:
  - name: kubeconf
    secret:
      secretName: test-cluster-config
"""
                                }
                            }

                            stages {
                                stage("Deploy to Dev") {
                                    steps {
                                        container('milvus-test-env') {
                                            script {
                                                load "${env.WORKSPACE}/ci/jenkins/step/deploySingle2Dev.groovy"
                                            }
                                        }
                                    }
                                }

                                stage("Dev Test") {
                                    steps {
                                        container('milvus-test-env') {
                                            script {
                                                boolean isNightlyTest = isTimeTriggeredBuild()
                                                if (isNightlyTest) {
                                                    load "${env.WORKSPACE}/ci/jenkins/step/singleDevNightlyTest.groovy"
                                                } else {
                                                    load "${env.WORKSPACE}/ci/jenkins/step/singleDevTest.groovy"
                                                }
                                            }
                                        }
                                    }
                                }

                                stage ("Cleanup Dev") {
                                    steps {
                                        container('milvus-test-env') {
                                            script {
                                                load "${env.WORKSPACE}/ci/jenkins/step/cleanupSingleDev.groovy"
                                            }
                                        }
                                    }
                                }
                            }
                            post {
                                unsuccessful {
                                    container('milvus-test-env') {
                                        script {
                                            load "${env.WORKSPACE}/ci/jenkins/step/cleanupSingleDev.groovy"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

boolean isTimeTriggeredBuild() {
    if (currentBuild.getBuildCauses('hudson.triggers.TimerTrigger$TimerTriggerCause').size() != 0) {
        return true
    }
    return false
}
