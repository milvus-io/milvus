package com;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.ExecutionException;
import io.milvus.client.*;

import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TestFlush {
    int index_file_size = 50;
    int dimension = 128;
    int nb = 8000;

    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);    
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);


    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_flush_collection_not_existed(MilvusClient client, String collectionName) {
        String newCollection = "not_existed";
        Response res = client.flush(newCollection);
        assert(!res.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_flush_empty_collection(MilvusClient client, String collectionName) {
        Response res = client.flush(collectionName);
        assert(res.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_collections_flush(MilvusClient client, String collectionName) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            names.add(RandomStringUtils.randomAlphabetic(10));
            CollectionMapping tableSchema = new CollectionMapping.Builder(names.get(i), dimension)
                                                    .withIndexFileSize(index_file_size)
                                                    .withMetricType(MetricType.IP)
                                                    .build();
            client.createCollection(tableSchema);
            InsertParam insertParam = new InsertParam.Builder(names.get(i)).withFloatVectors(vectors).build();
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
    public void test_add_collections_flush_async(MilvusClient client, String collectionName) throws ExecutionException, InterruptedException {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            names.add(RandomStringUtils.randomAlphabetic(10));
            CollectionMapping tableSchema = new CollectionMapping.Builder(names.get(i), dimension)
                    .withIndexFileSize(index_file_size)
                    .withMetricType(MetricType.IP)
                    .build();
            client.createCollection(tableSchema);
            InsertParam insertParam = new InsertParam.Builder(names.get(i)).withFloatVectors(vectors).build();
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
    public void test_add_flush_multiple_times(MilvusClient client, String collectionName) {
        for (int i = 0; i < 10; i++) {
            InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
            client.insert(insertParam);
            Response res = client.flush(collectionName);
            assert(res.ok());
            Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb * (i+1));
        }
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_add_flush_multiple_times_binary(MilvusClient client, String collectionName) {
        for (int i = 0; i < 10; i++) {
            InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
            client.insert(insertParam);
            Response res = client.flush(collectionName);
            assert(res.ok());
            Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb * (i+1));
        }
    }
}