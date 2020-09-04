package com;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.*;

import java.nio.ByteBuffer;
import java.util.List;

public class TestIndex_v2 {
    int dimension = Constants.dimension;
    int nb = Constants.nb;
    int n_list = Constants.n_list;
    String floatFieldName = Constants.floatFieldName;
    String binaryFieldName = Constants.binaryFieldName;
    String indexType = Constants.indexType;
    String defaultBinaryIndexType = Constants.defaultBinaryIndexType;
    String defaultMetricType = Constants.defaultMetricType;
    List<List<Float>> vectors = Constants.vectors;
    List<ByteBuffer> vectorsBinary = Constants.vectorsBinary;


    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndex(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genDefaultInsertParam(collectionName, dimension, nb, vectors);
        client.insert(insertParam);
        Index index = Utils.genDefaultIndex(collectionName, floatFieldName, indexType, defaultMetricType, n_list);
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        Response statsResponse = client.getCollectionStats(collectionName);
        if(statsResponse.ok()) {
            System.out.println(statsResponse.getMessage());
            JSONArray filesJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "files");
            filesJsonArray.stream().map(item-> (JSONObject)item).filter(item->item.containsKey("index_type")).forEach(file->
                    Assert.assertEquals(file.get("index_type"), indexType));
        }
    }

    // case-02
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCreateIndexBinary(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genDefaultBinaryInsertParam(collectionName, dimension, nb, vectorsBinary);
        client.insert(insertParam);
        Index index = Utils.genDefaultIndex(collectionName, binaryFieldName, defaultBinaryIndexType,
                Constants.defaultBinaryMetricType, n_list);
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        Response statsResponse = client.getCollectionStats(collectionName);
        if(statsResponse.ok()) {
            JSONArray filesJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "files");
            filesJsonArray.stream().map(item-> (JSONObject)item).filter(item->item.containsKey("index_type")).forEach(file->
                    Assert.assertEquals(file.get("index_type"), defaultBinaryIndexType));
        }
    }

    // case-04
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexWithNoVector(MilvusClient client, String collectionName) {
        Index index = Utils.genDefaultIndex(collectionName, floatFieldName, indexType,
                defaultMetricType, n_list);
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
    }

    // case-05
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexTableNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        Index index = Utils.genDefaultIndex(collectionNameNew, floatFieldName, indexType,
                defaultMetricType, n_list);
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    // case-06
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void testCreateIndexWithoutConnect(MilvusClient client, String collectionName) {
        Index index = Utils.genDefaultIndex(collectionName, floatFieldName, indexType,
                defaultMetricType, n_list);
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    // case-07
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexInvalidNList(MilvusClient client, String collectionName) {
        int n_list = 0;
        Index index = Utils.genDefaultIndex(collectionName, floatFieldName, indexType, defaultMetricType, n_list);
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    // # 3407
    // case-08
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCreateIndexInvalidMetricTypeBinary(MilvusClient client, String collectionName) {
        String metric_type = "L2";
        InsertParam insertParam = Utils.genDefaultBinaryInsertParam(collectionName, dimension, nb, vectorsBinary);
        client.insert(insertParam);
        Index createIndexParam = Utils.genDefaultIndex(collectionName, binaryFieldName, defaultBinaryIndexType, metric_type, n_list);
        Response res_create = client.createIndex(createIndexParam);
        Response statsResponse = client.getCollectionStats(collectionName);
        System.out.println(statsResponse.getMessage());
        Assert.assertFalse(res_create.ok());
    }

    // #3408 #3590
    // case-09
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropIndex(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genDefaultInsertParam(collectionName, dimension, nb, vectors);
        client.insert(insertParam);
        Index index = Utils.genDefaultIndex(collectionName, floatFieldName, indexType, defaultMetricType, n_list);
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        Response res_drop = client.dropIndex(collectionName, floatFieldName);
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
        InsertParam insertParam = Utils.genDefaultBinaryInsertParam(collectionName, dimension, nb, vectorsBinary);
        client.insert(insertParam);
        Index index = Utils.genDefaultIndex(collectionName, binaryFieldName, defaultBinaryIndexType, Constants.defaultBinaryMetricType, n_list);
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        Response res_drop = client.dropIndex(collectionName, binaryFieldName);
        assert(res_drop.ok());
        Response statsResponse = client.getCollectionStats(collectionName);
        if(statsResponse.ok()) {
            JSONArray filesJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "files");
            filesJsonArray.stream().map(item -> (JSONObject) item).forEach(file->{
                Assert.assertFalse(file.containsKey("index_type"));
            });
        }
    }

}
