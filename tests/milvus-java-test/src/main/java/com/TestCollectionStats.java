package com;

import com.alibaba.fastjson.JSONObject;
import io.milvus.client.Index;
import io.milvus.client.IndexType;
import io.milvus.client.InsertParam;
import io.milvus.client.JsonBuilder;
import io.milvus.client.MetricType;
import io.milvus.client.MilvusClient;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.Test;

public class TestCollectionStats {
    int nb = Constants.nb;

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testGetEntityIdsAfterDeleteEntities(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        List<Long> idsBefore = client.insert(insertParam);
        client.flush(collectionName);
        client.deleteEntityByID(collectionName, Collections.singletonList(idsBefore.get(0)));
        client.flush(collectionName);
        String stats = client.getCollectionStats(collectionName);
        JSONObject collectionStats = JSONObject.parseObject(stats);
        int rowCount = collectionStats.getIntValue("row_count");
        assert (rowCount == nb - 1);
    }

    // case-02
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testGetEntityIdsAfterDeleteEntitiesIndexed(
            MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        List<Long> idsBefore = client.insert(insertParam);
        client.flush(collectionName);
        Index index =
                Index.create(collectionName, Constants.floatVectorFieldName)
                        .setIndexType(IndexType.IVF_SQ8)
                        .setMetricType(MetricType.L2)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
        client.deleteEntityByID(collectionName, Collections.singletonList(idsBefore.get(0)));
        client.flush(collectionName);
        String stats = client.getCollectionStats(collectionName);
        JSONObject collectionStats = JSONObject.parseObject(stats);
        int rowCount = collectionStats.getIntValue("row_count");
        assert (rowCount == nb - 1);
    }

    // case-03
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testGetEntityIdsAfterDeleteEntitiesBinary(
            MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genBinaryInsertParam(collectionName);
        List<Long> idsBefore = client.insert(insertParam);
        client.flush(collectionName);
        client.deleteEntityByID(collectionName, Collections.singletonList(idsBefore.get(0)));
        client.flush(collectionName);
        String stats = client.getCollectionStats(collectionName);
        System.out.println(stats);
        JSONObject collectionStats = JSONObject.parseObject(stats);
        int rowCount = collectionStats.getIntValue("row_count");
        assert (rowCount == nb - 1);
    }
}
