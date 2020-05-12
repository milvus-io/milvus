package com;

import io.milvus.client.*;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestGetVectorByID {
    int dimension = 128;
    int nb = 8000;
    public List<Long> get_ids = Utils.toListIds(1111);
    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_get_vector_by_id_valid(MilvusClient client, String collectionName) {
        int get_length = 100;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse resInsert = client.insert(insertParam);
        List<Long> ids = resInsert.getVectorIds();
        client.flush(collectionName);
        GetEntityByIDResponse res = client.getEntityByID(collectionName, ids.subList(0, get_length));
        assert (res.getResponse().ok());
        for (int i = 0; i < get_length; i++) {
            assert (res.getFloatVectors().get(i).equals(vectors.get(i)));
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_get_vector_by_id_after_delete(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse resInsert = client.insert(insertParam);
        List<Long> ids = resInsert.getVectorIds();
        Response res_delete = client.deleteEntityByID(collectionName, Collections.singletonList(ids.get(0)));
        assert(res_delete.ok());
        client.flush(collectionName);
        GetEntityByIDResponse res = client.getEntityByID(collectionName, ids.subList(0, 1));
        assert (res.getResponse().ok());
        assert (res.getFloatVectors().get(0).size() == 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_get_vector_by_id_collection_name_not_existed(MilvusClient client, String collectionName) {
        String newCollection = "not_existed";
        GetEntityByIDResponse res = client.getEntityByID(newCollection, get_ids);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_get_vector_id_not_existed(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        client.flush(collectionName);
        GetEntityByIDResponse res = client.getEntityByID(collectionName, get_ids);
        assert (res.getFloatVectors().get(0).size() == 0);
    }

    // Binary tests
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_get_vector_by_id_valid_binary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse resInsert = client.insert(insertParam);
        List<Long> ids = resInsert.getVectorIds();
        client.flush(collectionName);
        GetEntityByIDResponse res = client.getEntityByID(collectionName, ids.subList(0, 1));
        assert res.getBinaryVectors().get(0).equals(vectorsBinary.get(0).rewind());
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_get_vector_by_id_after_delete_binary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse resInsert = client.insert(insertParam);
        List<Long> ids = resInsert.getVectorIds();
        Response res_delete = client.deleteEntityByID(collectionName, Collections.singletonList(ids.get(0)));
        assert(res_delete.ok());
        client.flush(collectionName);
        GetEntityByIDResponse res = client.getEntityByID(collectionName, ids.subList(0, 1));
        assert (res.getFloatVectors().get(0).size() == 0);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_get_vector_id_not_existed_binary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        client.insert(insertParam);
        client.flush(collectionName);
        GetEntityByIDResponse res = client.getEntityByID(collectionName, get_ids);
        assert (res.getFloatVectors().get(0).size() == 0);
    }
}