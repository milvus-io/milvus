package com;


import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

public class TestCollection {
    int segment_row_count = 5000;
    int dimension = 128;

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_collection(MilvusClient client, String collectionName){
        CollectionMapping collectionSchema = new CollectionMapping.Builder(collectionName)
                .withFields(Utils.genDefaultFields(dimension,false))
                .withParamsInJson(String.format("{\"segment_row_count\": %s}",segment_row_count))
                .build();
        Response res = client.createCollection(collectionSchema);
        assert(res.ok());
        Assert.assertEquals(res.ok(), true);
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_collection_disconnect(MilvusClient client, String collectionName){
        CollectionMapping collectionSchema = new CollectionMapping.Builder(collectionName)
                .withFields(Utils.genDefaultFields(dimension,false))
                .withParamsInJson(String.format("{\"segment_row_count\": %s}",segment_row_count))
                .build();
        Response res = client.createCollection(collectionSchema);
        assert(!res.ok());
    }

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_collection_repeatably(MilvusClient client, String collectionName){
        CollectionMapping collectionSchema = new CollectionMapping.Builder(collectionName)
                .withFields(Utils.genDefaultFields(dimension,false))
                .withParamsInJson(String.format("{\"segment_row_count\": %s}",segment_row_count))
                .build();
        Response res = client.createCollection(collectionSchema);
        Assert.assertEquals(res.ok(), true);
        Response res_new = client.createCollection(collectionSchema);
        Assert.assertEquals(res_new.ok(), false);
    }

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_collection_wrong_params(MilvusClient client, String collectionName){
        Integer dim = 0;
        CollectionMapping collectionSchema = new CollectionMapping.Builder(collectionName)
                .withFields(Utils.genDefaultFields(dim,false))
                .withParamsInJson(String.format("{\"segment_row_count\": %s}",segment_row_count))
                .build();
        Response res = client.createCollection(collectionSchema);
        System.out.println(res.toString());
        Assert.assertEquals(res.ok(), false);
    }

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void test_show_collections(MilvusClient client, String collectionName){
        Integer collectionNum = 10;
        ListCollectionsResponse res = null;
        for (int i = 0; i < collectionNum; ++i) {
            String collectionNameNew = collectionName+"_"+Integer.toString(i);
            CollectionMapping collectionSchema = new CollectionMapping.Builder(collectionNameNew)
                    .withFields(Utils.genDefaultFields(dimension,false))
                    .withParamsInJson(String.format("{\"segment_row_count\": %s}",segment_row_count))
                    .build();
            client.createCollection(collectionSchema);
            List<String> collectionNames = client.listCollections().getCollectionNames();
            Assert.assertTrue(collectionNames.contains(collectionNameNew));
        }
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_show_collections_without_connect(MilvusClient client, String collectionName){
        ListCollectionsResponse res = client.listCollections();
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_drop_collection(MilvusClient client, String collectionName) throws InterruptedException {
        Response res = client.dropCollection(collectionName);
        assert(res.ok());
        Thread.currentThread().sleep(1000);
        List<String> collectionNames = client.listCollections().getCollectionNames();
        Assert.assertFalse(collectionNames.contains(collectionName));
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_drop_collection_not_existed(MilvusClient client, String collectionName) {
        Response res = client.dropCollection(collectionName+"_");
        assert(!res.ok());
        List<String> collectionNames = client.listCollections().getCollectionNames();
        Assert.assertTrue(collectionNames.contains(collectionName));
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_drop_collection_without_connect(MilvusClient client, String collectionName) {
        Response res = client.dropCollection(collectionName);
        assert(!res.ok());
    }

    // TODO
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_describe_collection(MilvusClient client, String collectionName) {
        GetCollectionInfoResponse res = client.getCollectionInfo(collectionName);
        assert(res.getResponse().ok());
        CollectionMapping collectionSchema = res.getCollectionMapping().get();
        List<Map<String,Object>> fields = (List<Map<String, Object>>) collectionSchema.getFields();
        for(Map<String,Object> field: fields){
            if (field.containsValue("float_vector"))
        }
        String param = (String) fields.get(fields.size()-1).get("params");
        System.out.println("param: "+param);
        String str_segment_row_count = collectionSchema.getParamsInJson();
        System.out.println("segment: "+Utils.getParam(str_segment_row_count,"segment_row_count"));
        Assert.assertEquals(Utils.getParam(param,"dim"), dimension);
        Assert.assertEquals(collectionSchema.getCollectionName(), collectionName);
        Assert.assertEquals(Utils.getParam(str_segment_row_count,"segment_row_count"), segment_row_count);
//        Assert.assertEquals(collectionSchema.getMetricType().name(), collectionName.substring(0,2));
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_describe_collection_without_connect(MilvusClient client, String collectionName) {
        GetCollectionInfoResponse res = client.getCollectionInfo(collectionName);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_has_collection_not_existed(MilvusClient client, String collectionName) {
        HasCollectionResponse res = client.hasCollection(collectionName+"_");
        assert(res.getResponse().ok());
        Assert.assertFalse(res.hasCollection());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_has_collection_without_connect(MilvusClient client, String collectionName) {
        HasCollectionResponse res = client.hasCollection(collectionName);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_has_collection(MilvusClient client, String collectionName) {
        HasCollectionResponse res = client.hasCollection(collectionName);
        assert(res.getResponse().ok());
        Assert.assertTrue(res.hasCollection());
    }


}
