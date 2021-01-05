package com;

import io.milvus.client.*;
import org.testng.annotations.Test;

public class TestPing {
    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void test_server_status(String host, int port) throws ConnectFailedException {
        System.out.println("Host: "+host+", Port: "+port);
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(host)
                .withPort(port)
                .build();
        MilvusClient client = new MilvusGrpcClient(connectParam);
        Response res = client.getServerStatus();
        assert (res.ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_server_status_without_connected(MilvusClient client, String collectionName) throws ConnectFailedException {
        Response res = client.getServerStatus();
        assert (!res.ok());
    }
}