package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class TestCollectionCount {
    int segmentRowCount = 5000;
    int dimension = Constants.dimension;
    int nb = Constants.nb;

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCollectionCountNoVectors(MilvusClient client, String collectionName) {
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
    }

    // case-02
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCollectionCountCollectionNotExisted(MilvusClient client, String collectionName) {
        CountEntitiesResponse res = client.countEntities(collectionName+"_");
        assert(!res.getResponse().ok());
    }

    // case-03
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void testCollectionCountWithoutConnect(MilvusClient client, String collectionName) {
        CountEntitiesResponse res = client.countEntities(collectionName+"_");
        assert(!res.getResponse().ok());
    }

    // case-04
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCollectionCount(MilvusClient client, String collectionName) throws InterruptedException {

        InsertParam insertParam =
                new InsertParam.Builder(collectionName)
                        .withFields(Constants.defaultEntities)
                        .build();
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
        InsertParam insertParam = new InsertParam.Builder(collectionName)
                .withFields(Constants.defaultBinaryEntities)
                .build();
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
            CollectionMapping collectionSchema = new CollectionMapping.Builder(collectionNameNew)
                    .withFields(Utils.genDefaultFields(dimension,false))
                    .withParamsInJson(String.format("{\"segment_row_count\": %s}",segmentRowCount))
                    .build();
            Response createRes = client.createCollection(collectionSchema);
            Assert.assertEquals(createRes.ok(), true);
            // Add vectors
            InsertParam insertParam = new InsertParam.Builder(collectionNameNew)
                    .withFields(Constants.defaultEntities)
                    .build();
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


