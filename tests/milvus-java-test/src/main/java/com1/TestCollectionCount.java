package com1;

import io.grpc.StatusRuntimeException;
import io.milvus.client.*;
import io.milvus.client.exception.ClientSideMilvusException;
import io.milvus.client.exception.ServerSideMilvusException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class TestCollectionCount {
    int nb = Constants.nb;

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCollectionCountNoVectors(MilvusClient client, String collectionName) {
        Assert.assertEquals(client.countEntities(collectionName), 0);
    }

    // case-02
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testCollectionCountCollectionNotExisted(MilvusClient client, String collectionName) {
        client.countEntities(collectionName+"_");
    }

    // case-03
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class, expectedExceptions = ClientSideMilvusException.class)
    public void testCollectionCountWithoutConnect(MilvusClient client, String collectionName) {
        client.countEntities(collectionName);
    }

    // case-04
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCollectionCount(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        List<Long> ids = client.insert(insertParam);
        client.flush(collectionName);
        // Insert returns a list of entity ids that you will be using (if you did not supply the yourself) to reference the entities you just inserted
        Assert.assertEquals(client.countEntities(collectionName), nb);
    }

    // case-05
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCollectionCountBinary(MilvusClient client, String collectionName) {
        // Add vectors
        InsertParam insertParam = Utils.genBinaryInsertParam(collectionName);
        List<Long> ids = client.insert(insertParam);
        client.flush(collectionName);
        Assert.assertEquals(client.countEntities(collectionName), nb);
    }

    // case-06
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCollectionCountMultiCollections(MilvusClient client, String collectionName) {
        Integer collectionNum = 10;
        for (int i = 0; i < collectionNum; ++i) {
            String collectionNameNew = collectionName + "_" + i;
            CollectionMapping cm = Utils.genCreateCollectionMapping(collectionNameNew, true, false);
            client.createCollection(cm);
            // Add vectors
            InsertParam insertParam = Utils.genInsertParam(collectionNameNew);
            List<Long> ids = client.insert(insertParam);
            client.flush(collectionNameNew);
        }
        for (int i = 0; i < collectionNum; ++i) {
            String collectionNameNew = collectionName + "_" + i;
            Assert.assertEquals(client.countEntities(collectionNameNew), nb);
        }
    }
}


