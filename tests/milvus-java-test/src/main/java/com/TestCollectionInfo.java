
package com;

import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;

import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TestCollectionInfo {
    int dimension = 128;
    int nb = 8000;
    int nList = 1024;
    int defaultNList = 16384;
    String indexType = "IVF_SQ8";
    String defaultIndexType = "FLAT";
    String metricType = "L2";
    String indexParam = Utils.setIndexParam(indexType,metricType,nList);
    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);
    List<Map<String,Object>> defaultEntities = Utils.genDefaultEntities(dimension,nb,vectors);
    List<Map<String,Object>> defaultBinaryEntities = Utils.genDefaultBinaryEntities(dimension,nb,vectorsBinary);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testGetEntityIdsAfterDeleteEntities(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName)
                .withFields(defaultEntities)
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
                .withFields(defaultEntities)
                .build();
        InsertResponse resInsert = client.insert(insertParam);
        client.flush(collectionName);
        Index index = new Index.Builder(collectionName, "float_vector")
                .withParamsInJson(indexParam).build();
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
                .withFields(defaultBinaryEntities)
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