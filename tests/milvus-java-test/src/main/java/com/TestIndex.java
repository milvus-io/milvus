package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class TestIndex {

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndex(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        // TODO: should check getCollectionStats
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCreateIndexBinary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.binaryFieldName).withParamsInJson(Constants.binaryIndexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
    }

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

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexWithNoVector(MilvusClient client, String collectionName) {
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexTableNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        Index index = new Index.Builder(collectionNameNew, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void testCreateIndexWithoutConnect(MilvusClient client, String collectionName) {
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexInvalidNList(MilvusClient client, String collectionName) {
        int n_list = 0;
        String indexParamNew = Utils.setIndexParam(Constants.indexType, "L2", n_list);
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(indexParamNew).build();
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    // # 3407
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCreateIndexInvalidMetricTypeBinary(MilvusClient client, String collectionName) {
        String metric_type = "L2";
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        client.insert(insertParam);
        String indexParamNew = Utils.setIndexParam(Constants.defaultBinaryIndexType, metric_type, Constants.n_list);
        Index createIndexParam = new Index.Builder(collectionName, Constants.binaryFieldName).withParamsInJson(indexParamNew).build();
        Response res_create = client.createIndex(createIndexParam);
        assert (!res_create.ok());
    }

    // #3408
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropIndex(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
//        Response res_drop = client.dropIndex(collectionName, Constants.floatFieldName);
//        assert(res_drop.ok());
        // TODO: getCollectionStats
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testDropIndexBinary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.binaryFieldName).withParamsInJson(Constants.binaryIndexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
//        Response res_drop = client.dropIndex(collectionName, Constants.binaryFieldName);
//        assert(res_drop.ok());
        // TODO: getCollectionStats
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropIndexTableNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
//        Response res_drop = client.dropIndex(collectionNameNew, Constants.floatFieldName);
//        assert(!res_drop.ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void testDropIndexWithoutConnect(MilvusClient client, String collectionName) {
//        Response res_drop = client.dropIndex(collectionNameNew, Constants.floatFieldName);
//        assert(!res_drop.ok());
    }

}
