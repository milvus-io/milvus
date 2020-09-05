package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.List;

public class TestCollection_v2 {
    int segmentRowCount = 5000;
    int dimension = 128;

    @BeforeClass
    public MilvusClient setUp() throws ConnectFailedException {
        MilvusClient client = new MilvusGrpcClient();
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost("127.0.0.1")
                .withPort(19530)
                .build();
        client.connect(connectParam);
        return client;
    }

    @AfterClass
    public void tearDown() throws ConnectFailedException {
        MilvusClient client = setUp();
        List<String> collectionNames = client.listCollections().getCollectionNames();
        // collectionNames.forEach(collection -> {client.dropCollection(collection);});
        for(String collection: collectionNames){
            System.out.print(collection+" ");
            client.dropCollection(collection);
        }
        System.out.println("After Test");
    }

    // case-01
    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void testCreateCollection(MilvusClient client, String collectionName) {
        CollectionMapping collectionSchema =
                Utils.genDefaultCollectionMapping(collectionName, dimension, segmentRowCount, false);
        Response res = client.createCollection(collectionSchema);
        assert(res.ok());
        Assert.assertEquals(res.ok(), true);
    }

    // case-02
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void testCreateCollectionDisconnect(MilvusClient client, String collectionName) {
        CollectionMapping collectionSchema =
                Utils.genDefaultCollectionMapping(collectionName, dimension, segmentRowCount, false);
        Response res = client.createCollection(collectionSchema);
        assert(!res.ok());
    }

    // case-03
    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void testCreateCollectionRepeatably(MilvusClient client, String collectionName) {
        CollectionMapping collectionSchema =
                Utils.genDefaultCollectionMapping(collectionName, dimension, segmentRowCount, false);
        Response res = client.createCollection(collectionSchema);
        Assert.assertEquals(res.ok(), true);
        Response resNew = client.createCollection(collectionSchema);
        Assert.assertEquals(resNew.ok(), false);
    }

    // case-04
    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void testCreateCollectionWrongParams(MilvusClient client, String collectionName) {
        CollectionMapping collectionSchema =
                Utils.genDefaultCollectionMapping(collectionName, 0, segmentRowCount, false);
        Response res = client.createCollection(collectionSchema);
        System.out.println(res.toString());
        Assert.assertEquals(res.ok(), false);
    }

    // case-05
    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void testShowCollections(MilvusClient client, String collectionName) {
        Integer collectionNum = 10;
        ListCollectionsResponse res = null;
        for (int i = 0; i < collectionNum; ++i) {
            String collectionNameNew = collectionName+"_"+Integer.toString(i);
            CollectionMapping collectionSchema =
                    Utils.genDefaultCollectionMapping(collectionNameNew, dimension, segmentRowCount, false);
            client.createCollection(collectionSchema);
            List<String> collectionNames = client.listCollections().getCollectionNames();
            Assert.assertTrue(collectionNames.contains(collectionNameNew));
        }
    }
}
