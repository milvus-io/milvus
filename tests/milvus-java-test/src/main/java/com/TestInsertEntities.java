package com;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.milvus.client.DataType;
import io.milvus.client.InsertParam;
import io.milvus.client.MilvusClient;
import io.milvus.client.exception.ClientSideMilvusException;
import io.milvus.client.exception.ServerSideMilvusException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;


public class TestInsertEntities {
    int dimension = Constants.dimension;
    String tag = "tag";
    int nb = Constants.nb;
    Map<String, List> entities = Constants.defaultEntities;
    Map<String, List> binaryEntities = Constants.defaultBinaryEntities;

    // case-01
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testInsertEntitiesCollectionNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = collectionName + "_";
        InsertParam insertParam = Utils.genInsertParam(collectionNameNew);
        client.insert(insertParam);
    }

    // case-02
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class, expectedExceptions = ClientSideMilvusException.class)
    public void testInsertEntitiesWithoutConnect(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        client.insert(insertParam);
    }

    // case-03
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntities(MilvusClient client, String collectionName) {
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, entities.get(Constants.intFieldName))
                .addField(Constants.floatFieldName, DataType.FLOAT, entities.get(Constants.floatFieldName))
                .addVectorField(Constants.floatVectorFieldName, DataType.VECTOR_FLOAT, entities.get(Constants.floatVectorFieldName));
        List<Long> vectorIds = client.insert(insertParam);
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(vectorIds.size(), nb);
        Assert.assertEquals(client.countEntities(collectionName), nb);
    }

    // case-04
    @Test(dataProvider = "IdCollection", dataProviderClass = MainClass.class)
    public void testInsertEntityWithIds(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> entityIds = LongStream.range(0, nb).boxed().collect(Collectors.toList());
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, entities.get(Constants.intFieldName))
                .addField(Constants.floatFieldName, DataType.FLOAT, entities.get(Constants.floatFieldName))
                .addVectorField(Constants.floatVectorFieldName, DataType.VECTOR_FLOAT, entities.get(Constants.floatVectorFieldName))
                .setEntityIds(entityIds);
        List<Long> vectorIds = client.insert(insertParam);
        client.flush(collectionName);
        // Assert ids and collection row count
        Assert.assertEquals(vectorIds, entityIds);
        Assert.assertEquals(client.countEntities(collectionName), nb);
    }

    // case-05
    @Test(dataProvider = "IdCollection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testInsertEntityWithInvalidIds(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> entityIds = LongStream.range(0, nb + 1).boxed().collect(Collectors.toList());
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, entities.get(Constants.intFieldName))
                .addField(Constants.floatFieldName, DataType.FLOAT, entities.get(Constants.floatFieldName))
                .addVectorField(Constants.floatVectorFieldName, DataType.VECTOR_FLOAT, entities.get(Constants.floatVectorFieldName))
                .setEntityIds(entityIds);
        client.insert(insertParam);
    }

    // case-06
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testInsertEntityWithInvalidDimension(MilvusClient client, String collectionName) {
        List<List<Float>> vectors = Utils.genVectors(nb, dimension + 1, true);
        Map<String, List> entities = Utils.genDefaultEntities(nb, vectors);
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, entities.get(Constants.intFieldName))
                .addField(Constants.floatFieldName, DataType.FLOAT, entities.get(Constants.floatFieldName))
                .addVectorField(Constants.floatVectorFieldName, DataType.VECTOR_FLOAT, entities.get(Constants.floatVectorFieldName));
        client.insert(insertParam);
    }

    // case-07
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testInsertEntityWithInvalidVectors(MilvusClient client, String collectionName) {
        Map<String, List> entities = Utils.genDefaultEntities(nb, new ArrayList<>());
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, entities.get(Constants.intFieldName))
                .addField(Constants.floatFieldName, DataType.FLOAT, entities.get(Constants.floatFieldName))
                .addVectorField(Constants.floatVectorFieldName, DataType.VECTOR_FLOAT, entities.get(Constants.floatVectorFieldName));
        client.insert(insertParam);
    }

    // ----------------------------- partition cases in Insert ---------------------------------
    // case-08: Add vectors into collection with given tag
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testInsertEntityPartition(MilvusClient client, String collectionName) {
        client.createPartition(collectionName, tag);
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, entities.get(Constants.intFieldName))
                .addField(Constants.floatFieldName, DataType.FLOAT, entities.get(Constants.floatFieldName))
                .addVectorField(Constants.floatVectorFieldName, DataType.VECTOR_FLOAT, entities.get(Constants.floatVectorFieldName))
                .setPartitionTag(tag);
        client.insert(insertParam);
        client.flush(collectionName);
        // Assert collection row count
        String stats = client.getCollectionStats(collectionName);
        JSONArray partitionsJsonArray = Utils.parseJsonArray(stats, "partitions");
        partitionsJsonArray.stream().map(item -> (JSONObject) item).filter(item -> item.containsValue(tag)).forEach(obj -> {
            Assert.assertEquals(obj.get("row_count"), nb);
            Assert.assertEquals(obj.get("tag"), tag);
        });
    }

    // case-09: Add vectors into collection, which tag not existed
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testInsertEntityPartitionTagNotExisted(MilvusClient client, String collectionName) {
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, entities.get(Constants.intFieldName))
                .addField(Constants.floatFieldName, DataType.FLOAT, entities.get(Constants.floatFieldName))
                .addVectorField(Constants.floatVectorFieldName, DataType.VECTOR_FLOAT, entities.get(Constants.floatVectorFieldName))
                .setPartitionTag(tag);
        client.insert(insertParam);
    }

    // case-10: Binary tests
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testInsertEntityPartitionABinary(MilvusClient client, String collectionName) {
        client.createPartition(collectionName, tag);
        List<Long> intValues = new ArrayList<>(Constants.nb);
        List<Float> floatValues = new ArrayList<>(Constants.nb);
        List<ByteBuffer> vectors = Utils.genBinaryVectors(Constants.nb, Constants.dimension);
        for (int i = 0; i < Constants.nb; ++i) {
            intValues.add((long) i);
            floatValues.add((float) i);
        }
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, intValues)
                .addField(Constants.floatFieldName, DataType.FLOAT, floatValues)
                .addVectorField(Constants.binaryVectorFieldName, DataType.VECTOR_BINARY, vectors)
                .setPartitionTag(tag);
        client.insert(insertParam);
        client.flush(collectionName);
        // Assert collection row count
        String stats = client.getCollectionStats(collectionName);
        JSONArray partitionsJsonArray = Utils.parseJsonArray(stats, "partitions");
        partitionsJsonArray.stream().map(item -> (JSONObject) item).filter(item -> item.containsValue(tag)).forEach(obj -> {
            Assert.assertEquals(obj.get("tag"), tag);
            Assert.assertEquals(obj.get("row_count"), nb);
        });
    }

    // case-11
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testInsertEntityBinary(MilvusClient client, String collectionName) {
        List<Long> intValues = new ArrayList<>(Constants.nb);
        List<Float> floatValues = new ArrayList<>(Constants.nb);
        List<ByteBuffer> vectors = Utils.genBinaryVectors(Constants.nb, Constants.dimension);
        for (int i = 0; i < Constants.nb; ++i) {
            intValues.add((long) i);
            floatValues.add((float) i);
        }
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, intValues)
                .addField(Constants.floatFieldName, DataType.FLOAT, floatValues)
                .addVectorField(Constants.binaryVectorFieldName, DataType.VECTOR_BINARY, vectors);
        List<Long> vectorIds = client.insert(insertParam);
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(vectorIds.size(), nb);
        Assert.assertEquals(client.countEntities(collectionName), nb);
    }

    // case-12
    @Test(dataProvider = "BinaryIdCollection", dataProviderClass = MainClass.class)
    public void testInsertBinaryEntityWithIds(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> entityIds = LongStream.range(0, nb).boxed().collect(Collectors.toList());
        List<Long> intValues = new ArrayList<>(Constants.nb);
        List<Float> floatValues = new ArrayList<>(Constants.nb);
        List<ByteBuffer> vectors = Utils.genBinaryVectors(Constants.nb, Constants.dimension);
        for (int i = 0; i < Constants.nb; ++i) {
            intValues.add((long) i);
            floatValues.add((float) i);
        }
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, intValues)
                .addField(Constants.floatFieldName, DataType.FLOAT, floatValues)
                .addVectorField(Constants.binaryVectorFieldName, DataType.VECTOR_BINARY, vectors)
                .setEntityIds(entityIds);
        List<Long> vectorIds = client.insert(insertParam);
        client.flush(collectionName);
        // Assert collection row count
        Assert.assertEquals(entityIds, vectorIds);
        Assert.assertEquals(client.countEntities(collectionName), nb);
    }

    // case-13
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testInsertBinaryEntityWithInvalidIds(MilvusClient client, String collectionName) {
        // Add vectors with ids
        List<Long> invalidEntityIds = LongStream.range(0, nb + 1).boxed().collect(Collectors.toList());
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, binaryEntities.get(Constants.intFieldName))
                .addField(Constants.floatFieldName, DataType.FLOAT, binaryEntities.get(Constants.floatFieldName))
                .addVectorField(Constants.binaryVectorFieldName, DataType.VECTOR_BINARY, binaryEntities.get(Constants.binaryVectorFieldName))
                .setEntityIds(invalidEntityIds);
        client.insert(insertParam);
    }

    // case-14
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testInsertBinaryEntityWithInvalidDimension(MilvusClient client, String collectionName) {
        List<ByteBuffer> vectors = Utils.genBinaryVectors(nb, dimension - 1);
        Map<String, List> entities = Utils.genDefaultBinaryEntities(nb, vectors);
        InsertParam insertParam = InsertParam
                .create(collectionName)
                .addField(Constants.intFieldName, DataType.INT64, entities.get(Constants.intFieldName))
                .addField(Constants.floatFieldName, DataType.FLOAT, entities.get(Constants.floatFieldName))
                .addVectorField(Constants.binaryVectorFieldName, DataType.VECTOR_BINARY, entities.get(Constants.binaryVectorFieldName));
        client.insert(insertParam);
    }

//    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
//    public void testAsyncInsert(MilvusClient client, String collectionName) {
//        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
//        ListenableFuture<InsertResponse> insertResFuture = client.insertAsync(insertParam);
//        Futures.addCallback(
//                insertResFuture, new FutureCallback<InsertResponse>() {
//                    @Override
//                    public void onSuccess(InsertResponse insertResponse) {
//                        Assert.assertNotNull(insertResponse);
//                        Assert.assertTrue(insertResponse.ok());
//                        Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb);
//                    }
//                    @Override
//                    public void onFailure(Throwable t) {
//                        System.out.println(t.getMessage());
//                    }
//                }, MoreExecutors.directExecutor()
//        );
//    }
}
