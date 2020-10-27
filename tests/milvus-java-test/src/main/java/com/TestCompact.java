package com;

import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;
import io.milvus.client.exception.ServerSideMilvusException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class TestCompact {
    int nb = Constants.nb;

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactAfterDelete(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initData(client, collectionName);
        client.deleteEntityByID(collectionName, ids);
        client.flush(collectionName);
        client.compact(CompactParam.create(collectionName));
        String statsResponse = client.getCollectionStats(collectionName);
        JSONObject jsonObject = JSONObject.parseObject(statsResponse);
        Assert.assertEquals(jsonObject.getIntValue("data_size"), 0);
        Assert.assertEquals(client.countEntities(collectionName), 0);
    }

    // case-02
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCompactAfterDeleteBinary(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initBinaryData(client, collectionName);
        client.deleteEntityByID(collectionName, ids);
        client.flush(collectionName);
        client.compact(CompactParam.create(collectionName));
        String stats = client.getCollectionStats(collectionName);
        JSONObject jsonObject = JSONObject.parseObject(stats);
        Assert.assertEquals(jsonObject.getIntValue("data_size"), 0);
        Assert.assertEquals(client.countEntities(collectionName), 0);
    }

    // case-03
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testCompactNoCollection(MilvusClient client, String collectionName) {
        String name = "";
        client.compact(CompactParam.create(name));
    }

    // case-04
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactEmptyCollection(MilvusClient client, String collectionName) {
        String stats = client.getCollectionStats(collectionName);
        JSONObject jsonObject = JSONObject.parseObject(stats);
        client.compact(CompactParam.create(collectionName));
        int data_size = jsonObject.getIntValue("data_size");
        Assert.assertEquals(0, data_size);
    }

    // case-05
    // TODO delete not correct
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactThresholdLessThanDeleted(MilvusClient client, String collectionName) {
        int segmentRowLimit = nb + 1000;
        String collectionNameNew = collectionName + "_";
        CollectionMapping cm = CollectionMapping.create(collectionNameNew)
                .addField(Constants.intFieldName, DataType.INT64)
                .addField(Constants.floatFieldName, DataType.FLOAT)
                .addVectorField(Constants.floatVectorFieldName, DataType.VECTOR_FLOAT, Constants.dimension)
                .setParamsInJson(new JsonBuilder()
                        .param("segment_row_limit", segmentRowLimit)
                        .param("auto_id", true)
                        .build());
        client.createCollection(cm);
        List<Long> ids = Utils.initData(client, collectionNameNew);
        client.deleteEntityByID(collectionNameNew, ids.subList(0, nb / 4));
        client.flush(collectionNameNew);
        Assert.assertEquals(client.countEntities(collectionNameNew), nb - (nb / 4));
        // before compact
        String stats = client.getCollectionStats(collectionNameNew);
        JSONObject segmentsBefore = (JSONObject) Utils.parseJsonArray(stats, "segments").get(0);
        client.compact(CompactParam.create(collectionNameNew).setThreshold(0.9));
        // after compact
        String statsAfter = client.getCollectionStats(collectionNameNew);
        JSONObject segmentsAfter = (JSONObject) Utils.parseJsonArray(statsAfter, "segments").get(0);
        Assert.assertEquals(segmentsAfter.get("data_size"), segmentsBefore.get("data_size"));
    }

    // case-06
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testCompactInvalidThreshold(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initData(client, collectionName);
        client.deleteEntityByID(collectionName, ids);
        client.flush(collectionName);
        client.compact(CompactParam.create(collectionName).setThreshold(-1.0));
    }

//    // case-07, test CompactAsync callback
//    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
//    public void testCompactAsyncAfterDelete(MilvusClient client, String collectionName) {
//        // define callback
//        FutureCallback<Response> futureCallback = new FutureCallback<Response>() {
//            @Override
//            public void onSuccess(Response compactResponse) {
//                assert(compactResponse != null);
//                assert(compactResponse.ok());
//
//                Response statsResponse = client.getCollectionStats(collectionName);
//                assert(statsResponse.ok());
//                JSONObject jsonObject = JSONObject.parseObject(statsResponse.getMessage());
//                Assert.assertEquals(jsonObject.getIntValue("data_size"), 0);
//                Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
//            }
//
//            @Override
//            public void onFailure(Throwable t) {
//                System.out.println(t.getMessage());
//                Assert.assertTrue(false);
//            }
//        };
//
//        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
//        InsertResponse res = client.insert(insertParam);
//        assert(res.getResponse().ok());
//        List<Long> ids = res.getEntityIds();
//        client.flush(collectionName);
//        Response res_delete = client.deleteEntityByID(collectionName, ids);
//        assert(res_delete.ok());
//        client.flush(collectionName);
//        CompactParam compactParam = new CompactParam.Builder(collectionName).build();
//
//        // call compactAsync
//        ListenableFuture<Response> compactResponseFuture = client.compactAsync(compactParam);
//        Futures.addCallback(compactResponseFuture, futureCallback, MoreExecutors.directExecutor());
//
//        // execute before callback
//        Response statsResponse = client.getCollectionStats(collectionName);
//        assert(statsResponse.ok());
//        JSONObject jsonObject = JSONObject.parseObject(statsResponse.getMessage());
//        Assert.assertTrue(jsonObject.getIntValue("data_size") > 0);
//        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
//    }
//
//    // case-08, test CompactAsync callback with invalid collection name
//    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
//    public void testCompactAsyncNoCollection(MilvusClient client, String collectionName) {
//        // define callback
//        FutureCallback<Response> futureCallback = new FutureCallback<Response>() {
//            @Override
//            public void onSuccess(Response compactResponse) {
//                assert(compactResponse != null);
//                assert(!compactResponse.ok());
//            }
//
//            @Override
//            public void onFailure(Throwable t) {
//                System.out.println(t.getMessage());
//                Assert.assertTrue(false);
//            }
//        };
//
//        String name = "";
//        CompactParam compactParam = new CompactParam.Builder(name).build();
//
//        // call compactAsync
//        ListenableFuture<Response> compactResponseFuture = client.compactAsync(compactParam);
//        Futures.addCallback(compactResponseFuture, futureCallback, MoreExecutors.directExecutor());
//    }
}
