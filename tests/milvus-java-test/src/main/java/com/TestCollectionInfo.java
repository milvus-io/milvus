package com;

import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;
import org.testng.annotations.Test;

<<<<<<< HEAD
=======
import java.nio.ByteBuffer;
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
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
<<<<<<< HEAD
        List<Long> idsBefore = resInsert.getEntityIds();
=======
        List<Long> idsBefore = resInsert.getVectorIds();
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
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
<<<<<<< HEAD
        Index index = new Index.Builder(collectionName, "float_vector")
                .withParamsInJson(Constants.indexParam).build();
        Response createIndexResponse = client.createIndex(index);
        assert(createIndexResponse.ok());
        List<Long> idsBefore = resInsert.getEntityIds();
=======
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        client.createIndex(index);
        List<Long> idsBefore = resInsert.getVectorIds();
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
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
<<<<<<< HEAD
        List<Long> idsBefore = resInsert.getEntityIds();
=======
        List<Long> idsBefore = resInsert.getVectorIds();
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
        client.deleteEntityByID(collectionName, Collections.singletonList(idsBefore.get(0)));
        client.flush(collectionName);
        Response res = client.getCollectionStats(collectionName);
        System.out.println(res.getMessage());
        JSONObject collectionInfo = Utils.getCollectionInfo(res.getMessage());
        int rowCount = collectionInfo.getIntValue("row_count");
        assert(rowCount == nb-1);
    }

}