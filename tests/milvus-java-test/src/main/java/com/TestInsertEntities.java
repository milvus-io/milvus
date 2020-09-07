package com;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TestInsertEntities {
    int dimension = Constants.dimension;
    String tag = "tag";
    int nb = Constants.nb;

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntitiesCollectionNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = collectionName + "_";
        InsertParam insertParam = new InsertParam.Builder(collectionNameNew)
                .withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    // case-02
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void testInsertEntitiesWithoutConnect(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    // case-03
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntities(MilvusClient client, String collectionName)  {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    // case-04
    @Test(dataProvider = "IdCollection", dataProviderClass = MainClass.class)
    public void testInsertEntityWithIds(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> entityIds = LongStream.range(0, nb).boxed().collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName)
                .withFields(Constants.defaultEntities)
                .withEntityIds(entityIds)
                .build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert ids and collection row count
        Assert.assertEquals(res.getEntityIds(), entityIds);
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    // case-05
    @Test(dataProvider = "IdCollection", dataProviderClass = MainClass.class)
    public void testInsertEntityWithInvalidIds(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> entityIds = LongStream.range(0, nb+1).boxed().collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).withEntityIds(entityIds).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    // case-06
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntityWithInvalidDimension(MilvusClient client, String collectionName) {
        List<List<Float>> vectors = Utils.genVectors(nb, dimension+1, true);
        List<Map<String,Object>> entities = Utils.genDefaultEntities(dimension+1,nb,vectors);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(entities).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    // case-07
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntityWithInvalidVectors(MilvusClient client, String collectionName) {
        List<Map<String,Object>> invalidEntities = Utils.genDefaultEntities(dimension,nb,new ArrayList<>());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(invalidEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    // ----------------------------- partition cases in Insert ---------------------------------
    // case-08: Add vectors into collection with given tag
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntityPartition(MilvusClient client, String collectionName) {
        Response createpResponse = client.createPartition(collectionName, tag);
        assert(createpResponse.ok());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).withPartitionTag(tag).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Response statsResponse = client.getCollectionStats(collectionName);
        if(statsResponse.ok()) {
            JSONArray partitionsJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "partitions");
            partitionsJsonArray.stream().map(item -> (JSONObject) item).filter(item->item.containsValue(tag)).forEach(obj -> {
                Assert.assertEquals(obj.get("row_count"), nb);
                Assert.assertEquals(obj.get("tag"), tag);
            });
        }
    }

    // case-09: Add vectors into collection, which tag not existed
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntityPartitionTagNotExisted(MilvusClient client, String collectionName) {
        Response createpResponse = client.createPartition(collectionName, tag);
        assert(createpResponse.ok());
        String tag = RandomStringUtils.randomAlphabetic(10);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).withPartitionTag(tag).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    // case-10: Binary tests
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testInsertEntityPartitionABinary(MilvusClient client, String collectionName) {
        Response createpResponse = client.createPartition(collectionName, tag);
        assert (createpResponse.ok());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).withPartitionTag(tag).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Response statsResponse = client.getCollectionStats(collectionName);
        if(statsResponse.ok()) {
            JSONArray partitionsJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "partitions");
            partitionsJsonArray.stream().map(item -> (JSONObject) item).filter(item->item.containsValue(tag)).forEach(obj -> {
                Assert.assertEquals(obj.get("tag"), tag);
                Assert.assertEquals(obj.get("row_count"), nb);
            });
        }
    }

    // case-11
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testInsertEntityBinary(MilvusClient client, String collectionName)  {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    // case-12
    @Test(dataProvider = "BinaryIdCollection", dataProviderClass = MainClass.class)
    public void testInsertBinaryEntityWithIds(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> entityIds = LongStream.range(0, nb).boxed().collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).withEntityIds(entityIds).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(entityIds, res.getEntityIds());
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    // case-13
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testInsertBinaryEntityWithInvalidIds(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> invalidEntityIds = LongStream.range(0, nb+1).boxed().collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).withEntityIds(invalidEntityIds).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    // case-14
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testInsertBinaryEntityWithInvalidDimension(MilvusClient client, String collectionName) {
        List<List<Byte>> vectorsBinary = Utils.genBinaryVectors(nb, dimension-1);
        List<Map<String,Object>> binaryEntities = Utils.genDefaultBinaryEntities(dimension-1,nb,vectorsBinary);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(binaryEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }
}
