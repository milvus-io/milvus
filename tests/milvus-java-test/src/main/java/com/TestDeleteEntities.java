package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TestDeleteEntities {

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDeleteEntities(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, ids);
        assert(res_delete.ok());
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
    }

    // case-02
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDeleteSingleEntity(MilvusClient client, String collectionName) {
        List<List<Float>> del_vector = new ArrayList<>();
        del_vector.add(Constants.vectors.get(0));
        List<Long> del_ids = new ArrayList<>();
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        del_ids.add(ids.get(0));
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, Collections.singletonList(ids.get(0)));
        assert(res_delete.ok());
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), Constants.nb - 1);
        // Assert getEntityByID
        GetEntityByIDResponse res_get = client.getEntityByID(collectionName, del_ids);
        assert(res_get.getResponse().ok());
        Assert.assertEquals(res_get.getFieldsMap().size(), 1);
    }

    // case-03
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDeleteEntitiesCollectionNotExisted(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        client.flush(collectionName);
        List<Long> ids = res.getEntityIds();
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        Response res_delete = client.deleteEntityByID(collectionNameNew, ids);
        assert(!res_delete.ok());
    }

    // case-04
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDeleteEntitiesEmptyCollection(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        List<Long> entityIds = LongStream.range(0, Constants.nb).boxed().collect(Collectors.toList());
        Response res_delete = client.deleteEntityByID(collectionNameNew, entityIds);
        assert(!res_delete.ok());
    }

    // case-05
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDeleteEntityIdNotExisted(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = new ArrayList<Long>();
        ids.add((long)123456);
        ids.add((long)1234561);
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, ids);
        assert(res_delete.ok());
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), Constants.nb);
    }

    // case-06
    // Below tests binary vectors
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testDeleteEntitiesBinary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, ids);
        assert(res_delete.ok());
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
    }
}
