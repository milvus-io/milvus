package com;

import io.milvus.client.ConnectParam;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusGrpcClient;
import io.milvus.client.Response;
import org.testng.annotations.Test;

public class TestPing {
    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void test_server_status(String host, String port){
        System.out.println("Host: "+host+", Port: "+port);
        MilvusClient client = new MilvusGrpcClient();
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(host)
                .withPort(port)
                .build();
        client.connect(connectParam);
        Response res = client.serverStatus();
        assert (res.ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_server_status_without_connected(MilvusGrpcClient client, String tableName){
        Response res = client.serverStatus();
        assert (!res.ok());
    }
}