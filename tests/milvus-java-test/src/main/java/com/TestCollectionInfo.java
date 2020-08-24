package com;

import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

public class TestCollectionInfo {
    int nb = Constants.nb;

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testGetEntityIdsAfterDeleteEntities(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName)
                .withFields(Constants.defaultEntities)
                .build();
        InsertResponse resInsert = client.insert(insertParam);
        client.flush(collectionName);
        List<Long> idsBefore = resInsert.getEntityIds();
        client.deleteEntityByID(collectionName, Collections.singletonList(idsBefore.get(0)));
        client.flush(collectionName);
        Response res = client.getCollectionStats(collectionName);
        System.out.println(res.getMessage());
        JSONObject collectionInfo = Utils.getCollectionInfo(res.getMessage());
        int rowCount = collectionInfo.getIntValue("row_count");
        assert(rowCount == nb-1);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testGetEntityIdsAterDeleteEntitiesIndexed(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName)
                .withFields(Constants.defaultEntities)
                .build();
        InsertResponse resInsert = client.insert(insertParam);
        client.flush(collectionName);
        Index index = new Index.Builder(collectionName, "float_vector")
                .withParamsInJson(Constants.indexParam).build();
        Response createIndexResponse = client.createIndex(index);
        assert(createIndexResponse.ok());
        List<Long> idsBefore = resInsert.getEntityIds();
        client.deleteEntityByID(collectionName, Collections.singletonList(idsBefore.get(0)));
        client.flush(collectionName);
        Response res = client.getCollectionStats(collectionName);
        System.out.println(res.getMessage());
        JSONObject collectionInfo = Utils.getCollectionInfo(res.getMessage());
        int rowCount = collectionInfo.getIntValue("row_count");
        assert(rowCount == nb-1);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testGetEntityIdsAfterDeleteEntitiesBinary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName)
                .withFields(Constants.defaultBinaryEntities)
                .build();
        InsertResponse resInsert = client.insert(insertParam);
        client.flush(collectionName);
        List<Long> idsBefore = resInsert.getEntityIds();
        client.deleteEntityByID(collectionName, Collections.singletonList(idsBefore.get(0)));
        client.flush(collectionName);
        Response res = client.getCollectionStats(collectionName);
        System.out.println(res.getMessage());
        JSONObject collectionInfo = Utils.getCollectionInfo(res.getMessage());
        int rowCount = collectionInfo.getIntValue("row_count");
        assert(rowCount == nb-1);
    }

}