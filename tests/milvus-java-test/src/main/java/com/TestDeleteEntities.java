package com;

import io.milvus.client.*;
import io.milvus.client.exception.ServerSideMilvusException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TestDeleteEntities {

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDeleteEntities(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        List<Long> ids = client.insert(insertParam);
        client.flush(collectionName);
        client.deleteEntityByID(collectionName, ids);
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName), 0);
    }

    // case-02
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDeleteSingleEntity(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initData(client, collectionName);
        client.deleteEntityByID(collectionName, Collections.singletonList(ids.get(0)));
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName), Constants.nb - 1);
        // Assert getEntityByID
        Map<Long, Map<String, Object>> resEntity = client.getEntityByID(collectionName, Collections.singletonList(ids.get(0)));
        Assert.assertEquals(resEntity.size(), 0);
    }

    // case-03
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testDeleteEntitiesCollectionNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        client.deleteEntityByID(collectionNameNew, new ArrayList<Long>());
    }

    // case-04
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testDeleteEntitiesEmptyCollection(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        List<Long> entityIds = LongStream.range(0, Constants.nb).boxed().collect(Collectors.toList());
        client.deleteEntityByID(collectionNameNew, entityIds);
    }

    // case-05
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDeleteEntityIdNotExisted(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initData(client, collectionName);
        List<Long> delIds = new ArrayList<Long>();
        delIds.add(123456L);
        delIds.add(1234561L);
        client.deleteEntityByID(collectionName, delIds);
        client.flush(collectionName);
//         Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName), Constants.nb);
    }

    // case-06
    // Below tests binary vectors
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testDeleteEntitiesBinary(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initBinaryData(client, collectionName);
        client.deleteEntityByID(collectionName, ids);
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName), 0);
    }
}
