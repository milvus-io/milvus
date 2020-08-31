package com;

import com.alibaba.fastjson.JSONObject;
import com.sun.xml.internal.bind.v2.runtime.reflect.opt.Const;
import io.milvus.client.*;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.nio.ByteBuffer;

public class TestCollectionInfo_v2 {
    int nb = Constants.nb;
    int dimension = Constants.dimension;
    int n_list = Constants.n_list;
    List<List<Float>> vectors = Constants.vectors;
    List<ByteBuffer> vectorsBinary = Constants.vectorsBinary;
    String indexType = Constants.indexType;
    String metricType = Constants.defaultMetricType;

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testGetEntityIdsAfterDeleteEntities(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genDefaultInsertParam(collectionName, dimension, nb, vectors);
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

    // case-02
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testGetEntityIdsAterDeleteEntitiesIndexed(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genDefaultInsertParam(collectionName, dimension, nb, vectors);
        InsertResponse resInsert = client.insert(insertParam);
        client.flush(collectionName);
        Index index = Utils.genDefaultIndex(collectionName, indexType, metricType, n_list);
        Response createIndexResponse = client.createIndex(index);
        assert(createIndexResponse.ok());
        List<Long> idsBefore = resInsert.getEntityIds();
        client.deleteEntityByID(collectionName, Collections.singletonList(idsBefore.get(0)));
        client.flush(collectionName);
        Response res = client.getCollectionStats(collectionName);
        System.out.println(res.getMessage());
        JSONObject collectionInfo = Utils.getCollectionInfo(res.getMessage());
        int rowCount = collectionInfo.getIntValue("row_count");
        assert(rowCount == nb - 1);
    }

    // case-03
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testGetEntityIdsAfterDeleteEntitiesBinary(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genDefaultBinaryInsertParam(collectionName, dimension, nb, vectorsBinary);
        InsertResponse resInsert = client.insert(insertParam);
        client.flush(collectionName);
        List<Long> idsBefore = resInsert.getEntityIds();
        client.deleteEntityByID(collectionName, Collections.singletonList(idsBefore.get(0)));
        client.flush(collectionName);
        Response res = client.getCollectionStats(collectionName);
        System.out.println(res.getMessage());
        JSONObject collectionInfo = Utils.getCollectionInfo(res.getMessage());
        int rowCount = collectionInfo.getIntValue("row_count");
        assert(rowCount == nb - 1);
    }

}