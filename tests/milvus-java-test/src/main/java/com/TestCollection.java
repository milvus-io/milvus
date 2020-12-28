package com;


import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class TestCollection {
    int index_file_size = 50;
    int dimension = 128;

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_table(MilvusClient client, String collectionName){
        CollectionMapping tableSchema = new CollectionMapping.Builder(collectionName, dimension)
                .withIndexFileSize(index_file_size)
                .withMetricType(MetricType.L2)
                .build();
        Response res = client.createCollection(tableSchema);
        assert(res.ok());
        Assert.assertEquals(res.ok(), true);
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_table_disconnect(MilvusClient client, String collectionName){
        CollectionMapping tableSchema = new CollectionMapping.Builder(collectionName, dimension)
                .withIndexFileSize(index_file_size)
                .withMetricType(MetricType.L2)
                .build();
        Response res = client.createCollection(tableSchema);
        assert(!res.ok());
    }

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_table_repeatably(MilvusClient client, String collectionName){
        CollectionMapping tableSchema = new CollectionMapping.Builder(collectionName, dimension)
                .withIndexFileSize(index_file_size)
                .withMetricType(MetricType.L2)
                .build();
        Response res = client.createCollection(tableSchema);
        Assert.assertEquals(res.ok(), true);
        Response res_new = client.createCollection(tableSchema);
        Assert.assertEquals(res_new.ok(), false);
    }

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_table_wrong_params(MilvusClient client, String collectionName){
        Integer dimension = 0;
        CollectionMapping tableSchema = new CollectionMapping.Builder(collectionName, dimension)
                .withIndexFileSize(index_file_size)
                .withMetricType(MetricType.L2)
                .build();
        Response res = client.createCollection(tableSchema);
        System.out.println(res.toString());
        Assert.assertEquals(res.ok(), false);
    }

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void test_show_tables(MilvusClient client, String collectionName){
        Integer tableNum = 10;
        ListCollectionsResponse res = null;
        for (int i = 0; i < tableNum; ++i) {
            String collectionNameNew = collectionName+"_"+Integer.toString(i);
            CollectionMapping tableSchema = new CollectionMapping.Builder(collectionNameNew, dimension)
                    .withIndexFileSize(index_file_size)
                    .withMetricType(MetricType.L2)
                    .build();
            client.createCollection(tableSchema);
            List<String> collectionNames = client.listCollections().getCollectionNames();
            Assert.assertTrue(collectionNames.contains(collectionNameNew));
        }
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_show_tables_without_connect(MilvusClient client, String collectionName){
        ListCollectionsResponse res = client.listCollections();
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_drop_table(MilvusClient client, String collectionName) throws InterruptedException {
        Response res = client.dropCollection(collectionName);
        assert(res.ok());
        Thread.currentThread().sleep(1000);
        List<String> collectionNames = client.listCollections().getCollectionNames();
        Assert.assertFalse(collectionNames.contains(collectionName));
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_drop_table_not_existed(MilvusClient client, String collectionName) {
        Response res = client.dropCollection(collectionName+"_");
        assert(!res.ok());
        List<String> collectionNames = client.listCollections().getCollectionNames();
        Assert.assertTrue(collectionNames.contains(collectionName));
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_drop_table_without_connect(MilvusClient client, String collectionName) {
        Response res = client.dropCollection(collectionName);
        assert(!res.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_describe_table(MilvusClient client, String collectionName) {
        GetCollectionInfoResponse res = client.getCollectionInfo(collectionName);
        assert(res.getResponse().ok());
        CollectionMapping tableSchema = res.getCollectionMapping().get();
        Assert.assertEquals(tableSchema.getDimension(), dimension);
        Assert.assertEquals(tableSchema.getCollectionName(), collectionName);
        Assert.assertEquals(tableSchema.getIndexFileSize(), index_file_size);
        Assert.assertEquals(tableSchema.getMetricType().name(), collectionName.substring(0,2));
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_describe_table_without_connect(MilvusClient client, String collectionName) {
        GetCollectionInfoResponse res = client.getCollectionInfo(collectionName);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_has_table_not_existed(MilvusClient client, String collectionName) {
        HasCollectionResponse res = client.hasCollection(collectionName+"_");
        assert(res.getResponse().ok());
        Assert.assertFalse(res.hasCollection());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_has_table_without_connect(MilvusClient client, String collectionName) {
        HasCollectionResponse res = client.hasCollection(collectionName);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_has_table(MilvusClient client, String collectionName) {
        HasCollectionResponse res = client.hasCollection(collectionName);
        assert(res.getResponse().ok());
        Assert.assertTrue(res.hasCollection());
    }


}
