package com;

import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.MoreExecutors;
import javax.annotation.Nullable;

public class TestCompact {
    int nb = Constants.nb;

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactAfterDelete(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, ids);
        assert(res_delete.ok());
        client.flush(collectionName);
        CompactParam compactParam = new CompactParam.Builder(collectionName).build();
        Response res_compact = client.compact(compactParam);
        assert(res_compact.ok());
        Response statsResponse = client.getCollectionStats(collectionName);
        assert(statsResponse.ok());
        JSONObject jsonObject = JSONObject.parseObject(statsResponse.getMessage());
        Assert.assertEquals(jsonObject.getIntValue("data_size"), 0);
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
    }

    // case-02
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCompactAfterDeleteBinary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, ids);
        assert(res_delete.ok());
        client.flush(collectionName);
        CompactParam compactParam = new CompactParam.Builder(collectionName).build();
        Response res_compact = client.compact(compactParam);
        assert(res_compact.ok());
        Response statsResponse = client.getCollectionStats(collectionName);
        assert(statsResponse.ok());
        JSONObject jsonObject = JSONObject.parseObject(statsResponse.getMessage());
        Assert.assertEquals(jsonObject.getIntValue("data_size"), 0);
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
    }

    // case-03
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactNoCollection(MilvusClient client, String collectionName) {
        String name = "";
        CompactParam compactParam = new CompactParam.Builder(name).build();
        Response res_compact = client.compact(compactParam);
        assert(!res_compact.ok());
    }

    // case-04
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactEmptyCollection(MilvusClient client, String collectionName) {
        CompactParam compactParam = new CompactParam.Builder(collectionName).build();
        Response res_compact = client.compact(compactParam);
        assert(res_compact.ok());
    }

    // case-05
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactThresholdLessThanDeleted(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        client.flush(collectionName);
        Response deleteRes = client.deleteEntityByID(collectionName, res.getEntityIds().subList(0, nb/4));
        assert(deleteRes.ok());
        client.flush(collectionName);
        Response resBefore = client.getCollectionStats(collectionName);
        JSONObject segmentsBefore = (JSONObject)Utils.parseJsonArray(resBefore.getMessage(), "segments").get(0);
        CompactParam compactParam = new CompactParam.Builder(collectionName).withThreshold(0.3).build();
        Response resCompact = client.compact(compactParam);
        assert(resCompact.ok());
        Response resAfter = client.getCollectionStats(collectionName);
        JSONObject segmentsAfter = (JSONObject)Utils.parseJsonArray(resAfter.getMessage(), "segments").get(0);
        Assert.assertEquals(segmentsBefore.get("data_size"), segmentsAfter.get("data_size"));
    }

    // case-06
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactInvalidThreshold(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        client.flush(collectionName);
        Response deleteRes = client.deleteEntityByID(collectionName, res.getEntityIds());
        assert(deleteRes.ok());
        client.flush(collectionName);
        CompactParam compactParam = new CompactParam.Builder(collectionName).withThreshold(-1.0).build();
        Response resCompact = client.compact(compactParam);
        Assert.assertFalse(resCompact.ok());
    }

    // case-07, test CompactAsync callback
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactAsyncAfterDelete(MilvusClient client, String collectionName) {
        // define callback
        FutureCallback<Response> futureCallback = new FutureCallback<Response>() {
            @Override
            public void onSuccess(@Nullable Response compactResponse) {
                assert(compactResponse != null);
                assert(compactResponse.ok());

                Response statsResponse = client.getCollectionStats(collectionName);
                assert(statsResponse.ok());
                JSONObject jsonObject = JSONObject.parseObject(statsResponse.getMessage());
                Assert.assertEquals(jsonObject.getIntValue("data_size"), 0);
                Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
            }

            @Override
            public void onFailure(Throwable t) {
                System.out.println(t.getMessage());
                Assert.assertTrue(false);
            }
        };

        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, ids);
        assert(res_delete.ok());
        client.flush(collectionName);
        CompactParam compactParam = new CompactParam.Builder(collectionName).build();

        // call compactAsync
        ListenableFuture<Response> compactResponseFuture = client.compactAsync(compactParam);
        Futures.addCallback(compactResponseFuture, futureCallback, MoreExecutors.directExecutor());

        // execute before callback
        Response statsResponse = client.getCollectionStats(collectionName);
        assert(statsResponse.ok());
        JSONObject jsonObject = JSONObject.parseObject(statsResponse.getMessage());
        Assert.assertTrue(jsonObject.getIntValue("data_size") > 0);
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
    }

    // case-08, test CompactAsync callback with invalid collection name
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactAsyncNoCollection(MilvusClient client, String collectionName) {
        // define callback
        FutureCallback<Response> futureCallback = new FutureCallback<Response>() {
            @Override
            public void onSuccess(@Nullable Response compactResponse) {
                assert(compactResponse != null);
                assert(!compactResponse.ok());
            }

            @Override
            public void onFailure(Throwable t) {
                System.out.println(t.getMessage());
                Assert.assertTrue(false);
            }
        };

        String name = "";
        CompactParam compactParam = new CompactParam.Builder(name).build();

        // call compactAsync
        ListenableFuture<Response> compactResponseFuture = client.compactAsync(compactParam);
        Futures.addCallback(compactResponseFuture, futureCallback, MoreExecutors.directExecutor());
    }
}
