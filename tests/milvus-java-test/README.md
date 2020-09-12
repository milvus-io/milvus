# Requirements

- jdk-1.8
- testng

# How to use this Test Project

1. package and install

```shell
mvn clean install
```

2. start or deploy your milvus server
3. run tests

```shell
java -cp \"target/MilvusSDkJavaTest-1.0-SNAPSHOT.jar:lib/*\" com.MainClass -h 127.0.0.1
```

4. get test report

```shell
firefox test-output/index.html
```

# Contribution getting started

Add test cases under testng framework