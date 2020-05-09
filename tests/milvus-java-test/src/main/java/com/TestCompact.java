package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;

public class TestCompact {

    int dimension = 128;
    int nb = 8000;
    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_compact_after_delete(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getVectorIds();
        client.flush(collectionName);
        Response res_delete = client.deleteByIds(collectionName, ids);
        assert(res_delete.ok());
        client.flush(collectionName);
        Response res_compact = client.compact(collectionName);
        assert(res_compact.ok());
        Assert.assertEquals(client.getCollectionRowCount(collectionName).getCollectionRowCount(), 0);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_compact_after_delete_binary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getVectorIds();
        client.flush(collectionName);
        Response res_delete = client.deleteByIds(collectionName, ids);
        assert(res_delete.ok());
        client.flush(collectionName);
        Response res_compact = client.compact(collectionName);
        assert(res_compact.ok());
        Assert.assertEquals(client.getCollectionRowCount(collectionName).getCollectionRowCount(), 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_compact_no_table(MilvusClient client, String collectionName) {
        String name = "";
        Response res_compact = client.compact(name);
        assert(!res_compact.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_compact_empty_table(MilvusClient client, String collectionName) {
        Response res_compact = client.compact(collectionName);
        assert(res_compact.ok());
    }

}
