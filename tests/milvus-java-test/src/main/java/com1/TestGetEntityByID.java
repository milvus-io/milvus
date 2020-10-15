package com1;

import io.milvus.client.*;
import io.milvus.client.exception.ServerSideMilvusException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestGetEntityByID {
    public List<Long> get_ids = Utils.toListIds(1111);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testGetEntitiesByIdValid(MilvusClient client, String collectionName) {
        int get_length = 100;
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        List<Long> ids = client.insert(insertParam);
        client.flush(collectionName);
        Map<Long, Map<String, Object>> resEntities = client.getEntityByID(collectionName, ids.subList(0, get_length));
        for (int i = 0; i < get_length; i++) {
            Map<String,Object> fieldsMap = resEntities.get(ids.get(i));
            assert (fieldsMap.get("float_vector").equals(Constants.vectors.get(i)));
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testGetEntityByIdAfterDelete(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initData(client, collectionName);
        client.deleteEntityByID(collectionName, Collections.singletonList(ids.get(1)));
        client.flush(collectionName);
        List<Long> getIds = ids.subList(0,2);
        Map<Long, Map<String, Object>> resEntities = client.getEntityByID(collectionName, getIds);
        Assert.assertEquals(resEntities.size(), getIds.size()-1);
        Assert.assertEquals(resEntities.get(getIds.get(0)).get(Constants.floatVectorFieldName), Constants.vectors.get(0));
    }

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testGetEntityByIdCollectionNameNotExisted(MilvusClient client, String collectionName) {
        String newCollection = "not_existed";
        Map<Long, Map<String, Object>> resEntities = client.getEntityByID(newCollection, get_ids);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testGetVectorIdNotExisted(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initData(client, collectionName);
        Map<Long, Map<String, Object>> resEntities =  client.getEntityByID(collectionName, get_ids);
        Assert.assertEquals(resEntities.size(), 0);
    }

    // Binary tests
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testGetEntityByIdValidBinary(MilvusClient client, String collectionName) {
        int get_length = 20;
        List<Long> ids = Utils.initBinaryData(client, collectionName);
        Map<Long, Map<String, Object>> resEntities = client.getEntityByID(collectionName, ids.subList(0, get_length));
        for (int i = 0; i < get_length; i++) {
            assert (resEntities.get(ids.get(i)).get(Constants.binaryVectorFieldName).equals(Constants.vectorsBinary.get(i)));
        }
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testGetEntityByIdAfterDeleteBinary(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initBinaryData(client, collectionName);
        client.deleteEntityByID(collectionName, Collections.singletonList(ids.get(0)));
        client.flush(collectionName);
        Map<Long, Map<String, Object>> resEntities = client.getEntityByID(collectionName, ids.subList(0, 1));
        Assert.assertEquals(resEntities.size(), 0);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testGetEntityIdNotExistedBinary(MilvusClient client, String collectionName) {
        List<Long> ids = Utils.initBinaryData(client, collectionName);
        Map<Long, Map<String, Object>> resEntities = client.getEntityByID(collectionName, get_ids);
        Assert.assertEquals(resEntities.size(), 0);
    }
}