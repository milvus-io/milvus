package com;

import io.milvus.client.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TestInsertEntities {
    int dimension = 128;
    String tag = "tag";
    int nb = 8000;
    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);
    List<Map<String,Object>> defaultEntities = Utils.genDefaultEntities(dimension,nb,vectors);
    List<Map<String,Object>> defaultBinaryEntities = Utils.genDefaultBinaryEntities(dimension,nb,vectorsBinary);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntitiesCollectionNotExisted(MilvusClient client, String collectionName) throws InterruptedException {
        String collectionNameNew = collectionName + "_";
        InsertParam insertParam = new InsertParam.Builder(collectionNameNew)
                .withFields(defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void testInsertEntitiesWithoutConnect(MilvusClient client, String collectionName) throws InterruptedException {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntities(MilvusClient client, String collectionName)  {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    @Test(dataProvider = "IdCollection", dataProviderClass = MainClass.class)
    public void testInsertEntityWithIds(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> entityIds = LongStream.range(0, nb).boxed().collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName)
                .withFields(defaultEntities)
                .withEntityIds(entityIds)
                .build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    @Test(dataProvider = "IdCollection", dataProviderClass = MainClass.class)
    public void testInsertEntityWithInvalidIds(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> entityIds = LongStream.range(0, nb+1).boxed().collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultEntities).withEntityIds(entityIds).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

//    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
//    public void testInsertEntityWithInvalidDimension(MilvusClient client, String collectionName) {
////        vectors.get(0).add((float) 0);
//        List<Map<String,Object>> entities = Utils.genDefaultEntities(dimension+1,nb,vectors);
//        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(entities).build();
//        InsertResponse res = client.insert(insertParam);
//        assert(!res.getResponse().ok());
//    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntityWithInvalidVectors(MilvusClient client, String collectionName) {
//        vectors.set(0, new ArrayList<>());
        List<Map<String,Object>> invalidEntities = Utils.genDefaultEntities(dimension,nb,new ArrayList<>());
        invalidEntities.forEach(entity ->{
            if("float_vector".equals(entity.get("field"))){
                entity.put("values",new ArrayList<>());
            }
        });
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(invalidEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    // ----------------------------- partition cases in Insert ---------------------------------
    // Add vectors into collection with given tag
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntityPartition(MilvusClient client, String collectionName) {
        Response createpResponse = client.createPartition(collectionName, tag);
        assert(createpResponse.ok());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultEntities).withPartitionTag(tag).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    // Add vectors into collection, which tag not existed
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntityPartitionTagNotExisted(MilvusClient client, String collectionName) {
        Response createpResponse = client.createPartition(collectionName, tag);
        assert(createpResponse.ok());
        String tag = RandomStringUtils.randomAlphabetic(10);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultEntities).withPartitionTag(tag).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    // Binary tests
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testInsertEntityPartitionABinary(MilvusClient client, String collectionName) {
        Response createpResponse = client.createPartition(collectionName, tag);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultBinaryEntities).withPartitionTag(tag).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testInsertEntityBinary(MilvusClient client, String collectionName)  {
        System.out.println(collectionName);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultBinaryEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    @Test(dataProvider = "BinaryIdCollection", dataProviderClass = MainClass.class)
    public void testInsertBinaryEntityWithIds(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> entityIds = LongStream.range(0, nb).boxed().collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultBinaryEntities).withEntityIds(entityIds).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testInsertBinaryEntityWithInvalidIds(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> invalidEntityIds = LongStream.range(0, nb+1).boxed().collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultBinaryEntities).withEntityIds(invalidEntityIds).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testInsertBinaryEntityWithInvalidDimension(MilvusClient client, String collectionName) {
        List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension-1);
        List<Map<String,Object>> binaryEntities = Utils.genDefaultBinaryEntities(dimension-1,nb,vectorsBinary);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(binaryEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }
}
