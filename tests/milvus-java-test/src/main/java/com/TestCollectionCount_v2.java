package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;
import java.nio.ByteBuffer;

public class TestCollectionCount_v2 {
    int segmentRowCount = 5000;
    int dimension = Constants.dimension;
    int nb = Constants.nb;
    List<List<Float>> vectors = Constants.vectors;
    List<ByteBuffer> vectorsBinary = Constants.vectorsBinary;

    // case-04
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCollectionCount(MilvusClient client, String collectionName) throws InterruptedException {

        InsertParam insertParam = Utils.genDefaultInsertParam(collectionName, dimension, nb, vectors);
        InsertResponse insertResponse = client.insert(insertParam);
        // Insert returns a list of entity ids that you will be using (if you did not supply the yourself) to reference the entities you just inserted
        List<Long> vectorIds = insertResponse.getEntityIds();
        // Add vectors
        Response flushResponse = client.flush(collectionName);
        Assert.assertTrue(flushResponse.ok());
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    // case-05
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCollectionCountBinary(MilvusClient client, String collectionName) throws InterruptedException {
        // Add vectors
        InsertParam insertParam = Utils.genDefaultBinaryInsertParam(collectionName, dimension, nb, vectorsBinary);
        client.insert(insertParam);
        client.flush(collectionName);
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

    // case-06
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCollectionCountMultiCollections(MilvusClient client, String collectionName) throws InterruptedException {
        Integer collectionNum = 10;
        CountEntitiesResponse res;
        for (int i = 0; i < collectionNum; ++i) {
            String collectionNameNew = collectionName + "_" + i;
            CollectionMapping collectionSchema =
                    Utils.genDefaultCollectionMapping(collectionNameNew, dimension, segmentRowCount, false);
            Response createRes = client.createCollection(collectionSchema);
            Assert.assertEquals(createRes.ok(), true);
            // Add vectors
            InsertParam insertParam = Utils.genDefaultInsertParam(collectionNameNew, dimension, nb, vectors);
            InsertResponse insertRes = client.insert(insertParam);
            Assert.assertEquals(insertRes.ok(), true);
            Response flushRes = client.flush(collectionNameNew);
            Assert.assertEquals(flushRes.ok(), true);
        }
        for (int i = 0; i < collectionNum; ++i) {
            String collectionNameNew = collectionName + "_" + i;
            res = client.countEntities(collectionNameNew);
            Assert.assertEquals(res.getCollectionEntityCount(), nb);
        }
    }
}


