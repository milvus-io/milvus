package com;

import io.milvus.client.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestAddVectors {
    int dimension = 128;
    String tag = "tag";
    int nb = 8000;
    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_vectors_collection_not_existed(MilvusClient client, String collectionName) throws InterruptedException {
        String collectionNameNew = collectionName + "_";
        InsertParam insertParam = new InsertParam.Builder(collectionNameNew).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_add_vectors_without_connect(MilvusClient client, String collectionName) throws InterruptedException {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_vectors(MilvusClient client, String collectionName)  {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_vectors_with_ids(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> vectorIds;
        vectorIds = Stream.iterate(0L, n -> n)
                .limit(nb)
                .collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_vectors_with_invalid_ids(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> vectorIds;
        vectorIds = Stream.iterate(0L, n -> n)
                .limit(nb+1)
                .collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_vectors_with_invalid_dimension(MilvusClient client, String collectionName) {
        vectors.get(0).add((float) 0);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_vectors_with_invalid_vectors(MilvusClient client, String collectionName) {
        vectors.set(0, new ArrayList<>());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    // ----------------------------- partition cases in Insert ---------------------------------
    // Add vectors into collection with given tag
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_vectors_partition(MilvusClient client, String collectionName) {
        Response createpResponse = client.createPartition(collectionName, tag);
        assert(createpResponse.ok());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).withPartitionTag(tag).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    // Add vectors into collection, which tag not existed
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_vectors_partition_tag_not_existed(MilvusClient client, String collectionName) {
        Response createpResponse = client.createPartition(collectionName, tag);
        assert(createpResponse.ok());
        String tag = RandomStringUtils.randomAlphabetic(10);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).withPartitionTag(tag).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    // Binary tests
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_add_vectors_partition_A_binary(MilvusClient client, String collectionName) {
        Response createpResponse = client.createPartition(collectionName, tag);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).withPartitionTag(tag).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_add_vectors_binary(MilvusClient client, String collectionName)  {
        System.out.println(collectionName);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_add_vectors_with_ids_binary(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> vectorIds;
        vectorIds = Stream.iterate(0L, n -> n)
                .limit(nb)
                .collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Response res_flush = client.flush(collectionName);
        assert(res_flush.ok());
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_add_vectors_with_invalid_ids_binary(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> vectorIds;
        vectorIds = Stream.iterate(0L, n -> n)
                .limit(nb+1)
                .collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).withVectorIds(vectorIds).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_add_vectors_with_invalid_dimension_binary(MilvusClient client, String collectionName) {
        List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension-1);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }
}
