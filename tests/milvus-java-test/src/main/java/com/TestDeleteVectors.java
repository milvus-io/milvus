package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestDeleteVectors {
    int dimension = 128;
    int nb = 8000;

    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_delete_vectors(MilvusClient client, String collectionName) {
        // Add vectors
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getVectorIds();
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, ids);
        assert(res_delete.ok());
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_delete_single_vector(MilvusClient client, String collectionName) {
        List<List<Float>> del_vector = new ArrayList<>();
        del_vector.add(vectors.get(0));
        List<Long> del_ids = new ArrayList<>();
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getVectorIds();
        del_ids.add(ids.get(0));
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, Collections.singletonList(ids.get(0)));
        assert(res_delete.ok());
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb - 1);
        GetEntityByIDResponse res_get = client.getEntityByID(collectionName, del_ids);
        assert(res_get.getResponse().ok());
        assert(res_get.getFloatVectors().get(0).size() == 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_delete_vectors_collection_not_existed(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        client.flush(collectionName);
        List<Long> ids = res.getVectorIds();
        Response res_delete = client.deleteEntityByID(collectionName + "_not_existed", ids);
        assert(!res_delete.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_delete_vector_id_not_existed(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
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
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }


    // Below tests binary vectors
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_delete_vectors_binary(MilvusClient client, String collectionName) {
        // Add vectors
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getVectorIds();
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, ids);
        assert(res_delete.ok());
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_delete_single_vector_binary(MilvusClient client, String collectionName) {
        List<ByteBuffer> del_vector = new ArrayList<>();
        del_vector.add(vectorsBinary.get(0));
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getVectorIds();
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, Collections.singletonList(ids.get(0)));
        assert(res_delete.ok());
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb - 1);
        // Cannot search for the vector
        SearchParam searchParam = new SearchParam.Builder(collectionName)
            .withBinaryVectors(del_vector)
            .withTopK(1)
            .withParamsInJson("{\"nprobe\": 20}")
            .build();
        SearchResponse res_search = client.search(searchParam);
        assert(res_search.getResultIdsList().size() == 1);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_delete_vectors_collection_not_existed_binary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getVectorIds();
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName + "_not_existed", ids);
        assert(!res_delete.ok());
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_delete_vector_id_not_existed_binary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
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
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
    }

}
