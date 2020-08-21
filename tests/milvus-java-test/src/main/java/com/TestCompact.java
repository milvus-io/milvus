package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TestCompact {

    int dimension = 128;
    int nb = 8000;
    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);
    List<Map<String,Object>> defaultEntities = Utils.genDefaultEntities(dimension,nb,vectors);
    List<Map<String,Object>> defaultBinaryEntities = Utils.genDefaultBinaryEntities(dimension,nb,vectorsBinary);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactAfterDelete(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, ids);
        assert(res_delete.ok());
        client.flush(collectionName);
        CompactParam compactParam = new CompactParam.Builder(collectionName)
                .withThreshold(0.3)
                .build();
        Response res_compact = client.compact(compactParam);
        assert(res_compact.ok());
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCompactAfterDeleteBinary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(defaultBinaryEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        Response res_delete = client.deleteEntityByID(collectionName, ids);
        assert(res_delete.ok());
        client.flush(collectionName);
        CompactParam compactParam = new CompactParam.Builder(collectionName)
                .withThreshold(0.3)
                .build();
        Response res_compact = client.compact(compactParam);
        assert(res_compact.ok());
        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactNoCollection(MilvusClient client, String collectionName) {
        String name = "";
        CompactParam compactParam = new CompactParam.Builder(name)
                .withThreshold(0.3)
                .build();
        Response res_compact = client.compact(compactParam);
        assert(!res_compact.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCompactEmptyCollection(MilvusClient client, String collectionName) {
        CompactParam compactParam = new CompactParam.Builder(collectionName)
                .withThreshold(0.3)
                .build();
        Response res_compact = client.compact(compactParam);
        assert(res_compact.ok());
    }

}
