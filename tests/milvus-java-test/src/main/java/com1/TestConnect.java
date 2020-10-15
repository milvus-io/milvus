package com1;

import io.milvus.client.exception.ClientSideMilvusException;
import io.milvus.client.exception.ServerSideMilvusException;
import org.testcontainers.containers.GenericContainer;
import com1.MainClass;
import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

public class TestConnect {
    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void testConnect(String host, int port) throws Exception {
        System.out.println("Host: "+host+", Port: "+port);
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(host)
                .withPort(port)
                .build();
        MilvusClient client = new MilvusGrpcClient(connectParam).withLogging();
    }

    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void testConnectRepeat(String host, int port) {
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(host)
                .withPort(port)
                .build();
        MilvusClient client = new MilvusGrpcClient(connectParam).withLogging();
        MilvusClient client1 = new MilvusGrpcClient(connectParam).withLogging();
    }

    // TODO timeout
    @Test(dataProvider="InvalidConnectArgs", expectedExceptions = {ClientSideMilvusException.class, IllegalArgumentException.class})
    public void testConnectInvalidConnectArgs(String ip, int port) {
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(ip)
                .withPort(port)
                .withKeepAliveTimeout(1, TimeUnit.MILLISECONDS)
                .build();
        MilvusClient client = new MilvusGrpcClient(connectParam).withLogging();
    }

    @DataProvider(name="InvalidConnectArgs")
    public Object[][] generateInvalidConnectArgs() {
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

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void testDisconnect(MilvusClient client, String collectionName){
        client.close();
    }

    // TODO
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class, expectedExceptions = ClientSideMilvusException.class)
    public void testDisconnectRepeatably(MilvusClient client, String collectionName){
        client.close();
    }
}
