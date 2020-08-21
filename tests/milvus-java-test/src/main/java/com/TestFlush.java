package com;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import io.milvus.client.*;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TestFlush {
    int segmentRowCount = 50;
    int dimension = 128;
    int nb = 8000;

    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);
    List<Map<String,Object>> defaultFields = Utils.genDefaultFields(dimension,false);
    List<Map<String,Object>> defaultEntities = Utils.genDefaultEntities(dimension,nb,vectors);
    List<Map<String,Object>> defaultBinaryEntities = Utils.genDefaultBinaryEntities(dimension,nb,vectorsBinary);



    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testFlushCollectionNotExisted(MilvusClient client, String collectionName) {
        String newCollection = "not_existed";
        Response res = client.flush(newCollection);
        assert(!res.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testFlushEmptyCollection(MilvusClient client, String collectionName) {
        Response res = client.flush(collectionName);
        assert(res.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testAddCollectionsFlush(MilvusClient client, String collectionName) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            names.add(RandomStringUtils.randomAlphabetic(10));
            CollectionMapping collectionSchema = new CollectionMapping.Builder(names.get(i))
                    .withFields(defaultFields)
                    .withParamsInJson(String.format("{\"segment_row_count\": %s}",segmentRowCount))
                    .build();
            client.createCollection(collectionSchema);
            InsertParam insertParam = new InsertParam.Builder(names.get(i)).withFields(defaultEntities).build();
            client.insert(insertParam);
            System.out.println("Table " + names.get(i) + " created.");
        }
        Response res = client.flush(names);
        assert(res.ok());
        for (int i = 0; i < 10; i++) {
            // check row count
            Assert.assertEquals(client.countEntities(names.get(i)).getCollectionEntityCount(), nb);
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testAddCollectionsFlushAsync(MilvusClient client, String collectionName) throws ExecutionException, InterruptedException {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            names.add(RandomStringUtils.randomAlphabetic(10));
            CollectionMapping collectionSchema = new CollectionMapping.Builder(names.get(i))
                    .withFields(defaultFields)
                    .withParamsInJson(String.format("{\"segment_row_count\": %s}",segmentRowCount))
                    .build();
            client.createCollection(collectionSchema);
            InsertParam insertParam = new InsertParam.Builder(names.get(i)).withFields(defaultEntities).build();
            client.insert(insertParam);
            System.out.println("Collection " + names.get(i) + " created.");
        }
        ListenableFuture<Response> flushResponseFuture = client.flushAsync(names);
        flushResponseFuture.get();
        for (int i = 0; i < 100; i++) {
            // check row count
            Assert.assertEquals(client.countEntities(names.get(i)).getCollectionEntityCount(), nb);
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testAddFlushMultipleTimes(MilvusClient client, String collectionName) {
        for (int i = 0; i < 10; i++) {
            InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultEntities).build();
            client.insert(insertParam);
            Response res = client.flush(collectionName);
            assert(res.ok());
            Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb * (i+1));
        }
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testAddFlushMultipleTimesBinary(MilvusClient client, String collectionName) {
        for (int i = 0; i < 10; i++) {
            InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultBinaryEntities).build();
            client.insert(insertParam);
            Response res = client.flush(collectionName);
            assert(res.ok());
            Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb * (i+1));
        }
    }
}