package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestConnect {
    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void test_connect(String host, int port) throws ConnectFailedException {
        System.out.println("Host: "+host+", Port: "+port);
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(host)
                .withPort(port)
                .build();
        MilvusClient client = new MilvusGrpcClient(connectParam);
    }

    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void test_connect_repeat(String host, int port) {
        Response res = null;
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(host)
                .withPort(port)
                .build();
        MilvusClient client = new MilvusGrpcClient(connectParam);
        MilvusClient client_1 = new MilvusGrpcClient(connectParam);
    }

    @Test(dataProvider="InvalidConnectArgs")
    public void test_connect_invalid_connect_args(String ip, int port) {
        Response res = null;
        try {
            ConnectParam connectParam = new ConnectParam.Builder()
                    .withHost(ip)
                    .withPort(port)
                    .build();
            MilvusClient client = new MilvusGrpcClient(connectParam);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @DataProvider(name="InvalidConnectArgs")
    public Object[][] generate_invalid_connect_args() {
        int port = 19530;
        return new Object[][]{
                {"1.1.1.1", port},
                {"255.255.0.0", port},
                {"1.2.2", port},
                {"中文", port},
                {"www.baidu.com", 100000},
                {"127.0.0.1", 100000},
                {"www.baidu.com", 80},
        };
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_disconnect(MilvusClient client, String collectionName){
        client.close();
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_disconnect_repeatably(MilvusClient client, String collectionName){
        Response res = null;
        client.close();
    }
}
