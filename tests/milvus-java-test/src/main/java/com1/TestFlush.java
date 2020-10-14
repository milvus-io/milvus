package com1;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.concurrent.ExecutionException;
import io.milvus.client.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

public class TestFlush {
    int nb = Constants.nb;

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void testFlushCollectionNotExisted(MilvusClient client, String collectionName) {
        String newCollection = "not_existed";
        try {
            client.flush(newCollection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testFlushEmptyCollection(MilvusClient client, String collectionName) {
        client.flush(collectionName);
    }

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void testAddCollectionsFlush(MilvusClient client, String collectionName) {
        List<String> names = new ArrayList<>();
        int collectionNum = 10;
        for (int i = 0; i < collectionNum; i++) {
            names.add(RandomStringUtils.randomAlphabetic(10));
            CollectionMapping cm = CollectionMapping
                    .create(names.get(i))
                    .addField("int64", DataType.INT64)
                    .addField("float", DataType.FLOAT)
                    .addVectorField("float_vector", DataType.VECTOR_FLOAT, Constants.dimension)
                    .setParamsInJson(new JsonBuilder()
                            .param("segment_row_limit", Constants.segmentRowLimit)
                            .param("auto_id", true)
                            .build());
            client.createCollection(cm);
            InsertParam insertParam = Utils.genInsertParam(names.get(i));
            client.insert(insertParam);
            System.out.println("Table " + names.get(i) + " created.");
        }
        client.flush(names);
        for (int i = 0; i < collectionNum; i++) {
            // check row count
            Assert.assertEquals(client.countEntities(names.get(i)), nb);
        }
    }

//    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
//    public void testAddCollectionsFlushAsync(MilvusClient client, String collectionName) throws ExecutionException, InterruptedException {
//        List<String> names = new ArrayList<>();
//        for (int i = 0; i < 100; i++) {
//            names.add(RandomStringUtils.randomAlphabetic(10));
//            CollectionMapping collectionSchema = new CollectionMapping.Builder(names.get(i))
//                    .withFields(Constants.defaultFields)
//                    .withParamsInJson(String.format("{\"segment_row_count\": %s}",segmentRowCount))
//                    .build();
//            client.createCollection(collectionSchema);
//            InsertParam insertParam = new InsertParam.Builder(names.get(i)).withFields(Constants.defaultEntities).build();
//            client.insert(insertParam);
//            System.out.println("Collection " + names.get(i) + " created.");
//        }
//        ListenableFuture<Response> flushResponseFuture = client.flushAsync(names);
//        flushResponseFuture.get();
//        for (int i = 0; i < 100; i++) {
//            // check row count
//            Assert.assertEquals(client.countEntities(names.get(i)).getCollectionEntityCount(), nb);
//        }
//    }
//
//    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
//    public void testAddFlushMultipleTimes(MilvusClient client, String collectionName) {
//        for (int i = 0; i < 10; i++) {
//            InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
//            client.insert(insertParam);
//            Response res = client.flush(collectionName);
//            assert(res.ok());
//            Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb * (i+1));
//        }
//    }
//
//    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
//    public void testAddFlushMultipleTimesBinary(MilvusClient client, String collectionName) {
//        for (int i = 0; i < 10; i++) {
//            InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
//            client.insert(insertParam);
//            Response res = client.flush(collectionName);
//            assert(res.ok());
//            Assert.assertEquals(client.countEntities(collectionName).getCollectionEntityCount(), nb * (i+1));
//        }
//    }
}