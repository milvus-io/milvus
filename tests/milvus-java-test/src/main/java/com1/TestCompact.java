package com1;

import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;
import io.milvus.client.exception.ServerSideMilvusException;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import com1.Utils;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.MoreExecutors;

public class TestCompact {
    int nb = Constants.nb;

    private List<Long> initData(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        List<Long> ids = client.insert(insertParam);
        System.out.println(ids.get(0));
        client.flush(collectionName);
        return ids;
    }

    private List<Long> initBinaryData(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genBinaryInsertParam(collectionName);
        List<Long> ids = client.insert(insertParam);
        client.flush(collectionName);
        return ids;
    }

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactAfterDelete(MilvusClient client, String collectionName) {
        List<Long> ids = initData(client, collectionName);
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
        List<Long> ids = initData(client, collectionName);
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
        List<Long> ids = initData(client, collectionName);
        String statss = client.getCollectionStats(collectionName);
        JSONObject segments = (JSONObject)Utils.parseJsonArray(statss, "segments").get(0);
        System.out.println(segments);
        client.deleteEntityByID(collectionName, ids.subList(0, nb / 4));
        client.flush(collectionName);
        Assert.assertEquals(client.countEntities(collectionName), nb - (nb / 4));
        // before compact
        String stats = client.getCollectionStats(collectionName);
        System.out.println(stats);
        JSONObject segmentsBefore = (JSONObject)Utils.parseJsonArray(stats, "segments").get(0);
        client.compact(CompactParam.create(collectionName).setThreshold(0.5));
        // after compact
        String statsAfter = client.getCollectionStats(collectionName);
        System.out.println(statsAfter);
        JSONObject segmentsAfter = (JSONObject)Utils.parseJsonArray(statsAfter, "segments").get(0);
        Assert.assertEquals(segmentsAfter.get("data_size"), segmentsBefore.get("data_size"));
    }

    // case-06
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testCompactInvalidThreshold(MilvusClient client, String collectionName) {
        List<Long> ids = initData(client, collectionName);
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
