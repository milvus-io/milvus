package com;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.milvus.client.Index;
import io.milvus.client.IndexType;
import io.milvus.client.JsonBuilder;
import io.milvus.client.MetricType;
import io.milvus.client.MilvusClient;
import io.milvus.client.exception.ClientSideMilvusException;
import io.milvus.client.exception.ServerSideMilvusException;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestIndex {

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndex(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initData(client, collectionName);
        Index index =
                Index.create(collectionName, Constants.floatVectorFieldName)
                        .setIndexType(IndexType.IVF_SQ8)
                        .setMetricType(MetricType.L2)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
        String stats = client.getCollectionStats(collectionName);
        JSONArray filesJsonArray = Utils.parseJsonArray(stats, "files");
        filesJsonArray.stream()
                .map(item -> (JSONObject) item)
                .filter(item -> item.containsKey("index_type"))
                .forEach(
                        file ->
                                Assert.assertEquals(
                                        file.get("index_type"), Constants.indexType.toString()));
    }

    // case-02
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCreateIndexBinary(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initBinaryData(client, collectionName);
        Index index =
                Index.create(collectionName, Constants.binaryVectorFieldName)
                        .setIndexType(Constants.defaultBinaryIndexType)
                        .setMetricType(MetricType.JACCARD)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
        String stats = client.getCollectionStats(collectionName);
        JSONArray filesJsonArray = Utils.parseJsonArray(stats, "files");
        filesJsonArray.stream()
                .map(item -> (JSONObject) item)
                .filter(item -> item.containsKey("index_type"))
                .forEach(
                        file ->
                                Assert.assertEquals(
                                        file.get("index_type"),
                                        Constants.defaultBinaryIndexType.toString()));
    }

    // case-03
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexRepeatably(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initData(client, collectionName);
        Index index =
                Index.create(collectionName, Constants.floatVectorFieldName)
                        .setIndexType(IndexType.IVF_SQ8)
                        .setMetricType(MetricType.L2)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
        client.createIndex(index);
    }

    // case-04
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexWithNoVector(MilvusClient client, String collectionName) {
        Index index =
                Index.create(collectionName, Constants.floatVectorFieldName)
                        .setIndexType(IndexType.IVF_SQ8)
                        .setMetricType(MetricType.L2)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
    }

    // case-05
    @Test(
            dataProvider = "Collection",
            dataProviderClass = MainClass.class,
            expectedExceptions = ServerSideMilvusException.class)
    public void testCreateIndexTableNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        Index index =
                Index.create(collectionNameNew, Constants.floatVectorFieldName)
                        .setIndexType(IndexType.IVF_SQ8)
                        .setMetricType(MetricType.L2)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
    }

    // case-06
    @Test(
            dataProvider = "DisConnectInstance",
            dataProviderClass = MainClass.class,
            expectedExceptions = ClientSideMilvusException.class)
    public void testCreateIndexWithoutConnect(MilvusClient client, String collectionName) {
        Index index =
                Index.create(collectionName, Constants.floatVectorFieldName)
                        .setIndexType(IndexType.IVF_SQ8)
                        .setMetricType(MetricType.L2)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
    }

    // case-07
    @Test(
            dataProvider = "Collection",
            dataProviderClass = MainClass.class,
            expectedExceptions = ServerSideMilvusException.class)
    public void testCreateIndexInvalidNList(MilvusClient client, String collectionName) {
        int n_list = 0;
        Index index =
                Index.create(collectionName, Constants.floatVectorFieldName)
                        .setIndexType(IndexType.IVF_SQ8)
                        .setMetricType(MetricType.L2)
                        .setParamsInJson(new JsonBuilder().param("nlist", n_list).build());
        client.createIndex(index);
    }

    // #3407
    // case-08
    @Test(
            dataProvider = "BinaryCollection",
            dataProviderClass = MainClass.class,
            expectedExceptions = ServerSideMilvusException.class)
    public void testCreateIndexInvalidMetricTypeBinary(MilvusClient client, String collectionName) {
        MetricType metric_type = MetricType.L2;
        List<Long> ids = Utils.initBinaryData(client, collectionName);
        Index index =
                Index.create(collectionName, Constants.binaryVectorFieldName)
                        .setIndexType(IndexType.BIN_IVF_FLAT)
                        .setMetricType(metric_type)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
    }

    // #3408
    // case-09
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropIndex(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initData(client, collectionName);
        Index index =
                Index.create(collectionName, Constants.floatVectorFieldName)
                        .setIndexType(Constants.indexType)
                        .setMetricType(Constants.defaultMetricType)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
        client.dropIndex(collectionName, Constants.floatVectorFieldName);
        String stats = client.getCollectionStats(collectionName);
        JSONArray filesJsonArray = Utils.parseJsonArray(stats, "files");
        for (Object item : filesJsonArray) {
            JSONObject file = (JSONObject) item;
            Assert.assertFalse(file.containsKey("index_type"));
        }
    }

    // case-10
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testDropIndexBinary(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initBinaryData(client, collectionName);
        Index index =
                Index.create(collectionName, Constants.binaryVectorFieldName)
                        .setIndexType(Constants.defaultBinaryIndexType)
                        .setMetricType(Constants.defaultBinaryMetricType)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
        client.dropIndex(collectionName, Constants.binaryVectorFieldName);
        String stats = client.getCollectionStats(collectionName);
        JSONArray filesJsonArray = Utils.parseJsonArray(stats, "files");
        for (Object item : filesJsonArray) {
            JSONObject file = (JSONObject) item;
            Assert.assertFalse(file.containsKey("index_type"));
        }
    }

    // case-11
    @Test(
            dataProvider = "Collection",
            dataProviderClass = MainClass.class,
            expectedExceptions = ServerSideMilvusException.class)
    public void testDropIndexCollectionNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        client.dropIndex(collectionNameNew, Constants.floatVectorFieldName);
    }

    // case-12
    @Test(
            dataProvider = "DisConnectInstance",
            dataProviderClass = MainClass.class,
            expectedExceptions = ClientSideMilvusException.class)
    public void testDropIndexWithoutConnect(MilvusClient client, String collectionName) {
        client.dropIndex(collectionName, Constants.floatVectorFieldName);
    }
    //
    //    // case-13
    //    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    //    public void testAsyncIndex(MilvusClient client, String collectionName) {
    //        Index index = new Index.Builder(collectionName,
    // Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
    //        ListenableFuture<Response> createIndexResFuture = client.createIndexAsync(index);
    //        Futures.addCallback(
    //                createIndexResFuture, new FutureCallback<Response>() {
    //                    @Override
    //                    public void onSuccess(Response createIndexResponse) {
    //                        Assert.assertNotNull(createIndexResponse);
    //                        Assert.assertTrue(createIndexResponse.ok());
    //                        Response statsResponse = client.getCollectionStats(collectionName);
    //                        if(statsResponse.ok()) {
    //                            JSONArray filesJsonArray =
    // Utils.parseJsonArray(statsResponse.getMessage(), "files");
    //                            filesJsonArray.stream().map(item->
    // (JSONObject)item).filter(item->item.containsKey("index_type")).forEach(file->
    //                                    Assert.assertEquals(file.get("index_type"),
    // Constants.indexType));
    //                        }
    //                    }
    //                    @Override
    //                    public void onFailure(Throwable t) {
    //                        System.out.println(t.getMessage());
    //                    }
    //                }, MoreExecutors.directExecutor()
    //        );
    //    }

}
