package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestConnect {
    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void test_connect(String host, int port) throws ConnectFailedException {
        System.out.println("Host: "+host+", Port: "+port);
        MilvusClient client = new MilvusGrpcClient();
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(host)
                .withPort(port)
                .build();
        Response res = client.connect(connectParam);
        assert(res.ok());
        assert(client.isConnected());
    }

    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void test_connect_repeat(String host, int port) {
        MilvusGrpcClient client = new MilvusGrpcClient();

        Response res = null;
        try {
            ConnectParam connectParam = new ConnectParam.Builder()
                    .withHost(host)
                    .withPort(port)
                    .build();
            res = client.connect(connectParam);
            res = client.connect(connectParam);
        } catch (ConnectFailedException e) {
            e.printStackTrace();
        }
        assert (res.ok());
        assert(client.isConnected());
    }

    @Test(dataProvider="InvalidConnectArgs")
    public void test_connect_invalid_connect_args(String ip, int port) {
        MilvusClient client = new MilvusGrpcClient();
        Response res = null;
        try {
            ConnectParam connectParam = new ConnectParam.Builder()
                    .withHost(ip)
                    .withPort(port)
                    .build();
            res = client.connect(connectParam);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Assert.assertEquals(res, null);
        assert(!client.isConnected());
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
        assert(!client.isConnected());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_disconnect_repeatably(MilvusClient client, String collectionName){
        Response res = null;
        try {
            res = client.disconnect();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assert(!res.ok());
        assert(!client.isConnected());
    }
}
