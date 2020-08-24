package com;

import io.milvus.client.*;
import org.testng.annotations.Test;

public class TestPing {
    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void testServerStatus(String host, int port) throws ConnectFailedException {
        System.out.println("Host: "+host+", Port: "+port);
        MilvusClient client = new MilvusGrpcClient();
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(host)
                .withPort(port)
                .build();
        client.connect(connectParam);
        Response res = client.getServerStatus();
        assert (res.ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void testServerStatusWithoutConnected(MilvusClient client, String collectionName) throws ConnectFailedException {
        Response res = client.getServerStatus();
        assert (!res.ok());
    }
}