package com;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestIndex {

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndex(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        Response statsResponse = client.getCollectionStats(collectionName);
        if(statsResponse.ok()) {
            JSONArray filesJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "files");
            filesJsonArray.stream().map(item-> (JSONObject)item).filter(item->item.containsKey("index_type")).forEach(file->
                    Assert.assertEquals(file.get("index_type"), Constants.indexType));
        }
    }

    // case-02
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCreateIndexBinary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.binaryFieldName).withParamsInJson(Constants.binaryIndexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        Response statsResponse = client.getCollectionStats(collectionName);
        if(statsResponse.ok()) {
            JSONArray filesJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "files");
            filesJsonArray.stream().map(item-> (JSONObject)item).filter(item->item.containsKey("index_type")).forEach(file->
                    Assert.assertEquals(file.get("index_type"), Constants.defaultBinaryIndexType));
        }
    }

    // case-03
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexRepeatably(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        Response res_create_2 = client.createIndex(index);
        assert(res_create_2.ok());
    }

    // case-04
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexWithNoVector(MilvusClient client, String collectionName) {
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
    }

    // case-05
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexTableNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        Index index = new Index.Builder(collectionNameNew, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    // case-06
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void testCreateIndexWithoutConnect(MilvusClient client, String collectionName) {
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    // case-07
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexInvalidNList(MilvusClient client, String collectionName) {
        int n_list = 0;
        String indexParamNew = Utils.setIndexParam(Constants.indexType, "L2", n_list);
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(indexParamNew).build();
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    // # 3407
    // case-08
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCreateIndexInvalidMetricTypeBinary(MilvusClient client, String collectionName) {
        String metric_type = "L2";
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        client.insert(insertParam);
        String indexParamNew = Utils.setIndexParam(Constants.defaultBinaryIndexType, metric_type, Constants.n_list);
        Index createIndexParam = new Index.Builder(collectionName, Constants.binaryFieldName).withParamsInJson(indexParamNew).build();
        Response res_create = client.createIndex(createIndexParam);
//        JSONArray filesJsonArray = Utils.parseJsonArray(res_create.getMessage(), "files");
        Response statsResponse = client.getCollectionStats(collectionName);
        System.out.println(statsResponse.getMessage());
        assert (!res_create.ok());
    }

    // #3408
    // case-09
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropIndex(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        Response res_drop = client.dropIndex(collectionName, Constants.floatFieldName);
        assert(res_drop.ok());
        Response statsResponse = client.getCollectionStats(collectionName);
        if(statsResponse.ok()) {
            JSONArray filesJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "files");
            filesJsonArray.stream().map(item -> (JSONObject) item).forEach(file->{
                Assert.assertFalse(file.containsKey("index_type"));
            });
        }
    }

    // case-10
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testDropIndexBinary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.binaryFieldName).withParamsInJson(Constants.binaryIndexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        Response res_drop = client.dropIndex(collectionName, Constants.binaryFieldName);
        assert(res_drop.ok());
        Response statsResponse = client.getCollectionStats(collectionName);
        if(statsResponse.ok()) {
            JSONArray filesJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "files");
            filesJsonArray.stream().map(item -> (JSONObject) item).forEach(file->{
                Assert.assertFalse(file.containsKey("index_type"));
            });
        }
    }

    // case-11
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropIndexCollectionNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        Response res_drop = client.dropIndex(collectionNameNew, Constants.floatFieldName);
        assert(!res_drop.ok());
    }

    // case-12
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void testDropIndexWithoutConnect(MilvusClient client, String collectionName) {
        Response res_drop = client.dropIndex(collectionName, Constants.floatFieldName);
        assert(!res_drop.ok());
    }

}
