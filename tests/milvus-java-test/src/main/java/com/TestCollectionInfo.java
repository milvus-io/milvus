
package com;

import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;

import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class TestCollectionInfo {
    int dimension = 128;
    int nb = 8000;
    int n_list = 1024;
    int default_n_list = 16384;
    IndexType indexType = IndexType.IVF_SQ8;
    IndexType defaultIndexType = IndexType.FLAT;
    String indexParam = Utils.setIndexParam(n_list);
    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_get_vector_ids_after_delete_vectors(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse resInsert = client.insert(insertParam);
        client.flush(collectionName);
        List<Long> idsBefore = resInsert.getVectorIds();
        client.deleteEntityByID(collectionName, Collections.singletonList(idsBefore.get(0)));
        client.flush(collectionName);
        Response res = client.getCollectionStats(collectionName);
        System.out.println(res.getMessage());
        JSONObject collectionInfo = Utils.getCollectionInfo(res.getMessage());
        int row_count = collectionInfo.getIntValue("row_count");
        assert(row_count == nb-1);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_get_vector_ids_after_delete_vectors_indexed(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse resInsert = client.insert(insertParam);
        client.flush(collectionName);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        client.createIndex(index);
        List<Long> idsBefore = resInsert.getVectorIds();
        client.deleteEntityByID(collectionName, Collections.singletonList(idsBefore.get(0)));
        client.flush(collectionName);
        Response res = client.getCollectionStats(collectionName);
        System.out.println(res.getMessage());
        JSONObject collectionInfo = Utils.getCollectionInfo(res.getMessage());
        int row_count = collectionInfo.getIntValue("row_count");
        assert(row_count == nb-1);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_get_vector_ids_after_delete_vectors_binary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse resInsert = client.insert(insertParam);
        client.flush(collectionName);
        List<Long> idsBefore = resInsert.getVectorIds();
        client.deleteEntityByID(collectionName, Collections.singletonList(idsBefore.get(0)));
        client.flush(collectionName);
        Response res = client.getCollectionStats(collectionName);
        System.out.println(res.getMessage());
        JSONObject collectionInfo = Utils.getCollectionInfo(res.getMessage());
        int row_count = collectionInfo.getIntValue("row_count");
        assert(row_count == nb-1);
    }

}