package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class TestCollectionCount {
    int index_file_size = 50;
    int dimension = 128;
    int nb = 10000;
    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_collection_count_no_vectors(MilvusClient client, String collectionName) {
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_collection_count_collection_not_existed(MilvusClient client, String collectionName) {
        CountEntitiesResponse res = client.countEntities(collectionName+"_");
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_collection_count_without_connect(MilvusClient client, String collectionName) {
        CountEntitiesResponse res = client.countEntities(collectionName+"_");
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_collection_count(MilvusClient client, String collectionName) throws InterruptedException {
        // Add vectors
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        client.flush(collectionName);
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_collection_count_binary(MilvusClient client, String collectionName) throws InterruptedException {
        // Add vectors
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        client.insert(insertParam);
        client.flush(collectionName);
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_collection_count_multi_collections(MilvusClient client, String collectionName) throws InterruptedException {
        Integer collectionNum = 10;
        CountEntitiesResponse res;
        for (int i = 0; i < collectionNum; ++i) {
            String collectionNameNew = collectionName + "_" + i;
            CollectionMapping collectionSchema = new CollectionMapping.Builder(collectionNameNew, dimension)
                    .withIndexFileSize(index_file_size)
                    .withMetricType(MetricType.L2)
                    .build();
            client.createCollection(collectionSchema);
            // Add vectors
            InsertParam insertParam = new InsertParam.Builder(collectionNameNew).withFloatVectors(vectors).build();
            client.insert(insertParam);
            client.flush(collectionNameNew);
        }
        for (int i = 0; i < collectionNum; ++i) {
            String collectionNameNew = collectionName + "_" + i;
            res = client.countEntities(collectionNameNew);
            Assert.assertEquals(res.getCollectionEntityCount(), nb);
        }
    }

}


