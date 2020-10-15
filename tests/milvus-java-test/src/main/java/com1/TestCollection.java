package com1;

import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;
import io.milvus.client.exception.ClientSideMilvusException;
import io.milvus.client.exception.ServerSideMilvusException;
import org.testng.Assert;
import org.testng.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com1.Utils;

public class TestCollection {
    String intFieldName = Constants.intFieldName;
    String floatFieldName = Constants.floatFieldName;
    String floatVectorFieldName = Constants.floatVectorFieldName;
    String binaryVectorFieldName = Constants.binaryVectorFieldName;
    Boolean autoId = true;

    // case-01
    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void testCreateCollection(MilvusClient client, String collectionName) {
        // Generate connection instance
        CollectionMapping cm = Utils.genCreateCollectionMapping(collectionName, true,false);
        client.createCollection(cm);
        Assert.assertEquals(client.hasCollection(collectionName), true);
    }

    // case-02
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class, expectedExceptions = ClientSideMilvusException.class)
    public void testCreateCollectionDisconnect(MilvusClient client, String collectionName) {
        CollectionMapping cm = Utils.genCreateCollectionMapping(collectionName, true, false);
        client.createCollection(cm);

    }

    // case-03
    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testCreateCollectionRepeatably(MilvusClient client, String collectionName) {
        CollectionMapping cm = Utils.genCreateCollectionMapping(collectionName, true, false);
        client.createCollection(cm);
        Assert.assertEquals(client.hasCollection(collectionName), true);
        client.createCollection(cm);
    }

    // case-04
    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testCreateCollectionWrongParams(MilvusClient client, String collectionName) {
        Integer dim = 0;
        CollectionMapping cm = CollectionMapping.create(collectionName)
                .addField(intFieldName, DataType.INT64)
                .addField(floatFieldName, DataType.FLOAT)
                .addVectorField(floatVectorFieldName, DataType.VECTOR_FLOAT, dim)
                .setParamsInJson(new JsonBuilder()
                        .param("segment_row_limit", Constants.segmentRowLimit)
                        .param("auto_id", autoId)
                        .build());
        client.createCollection(cm);
    }

    // case-05
    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void testShowCollections(MilvusClient client, String collectionName) {
        Integer collectionNum = 10;
        List<String> originCollections = new ArrayList<>();
        for (int i = 0; i < collectionNum; ++i) {
            String collectionNameNew = collectionName+"_"+Integer.toString(i);
            originCollections.add(collectionNameNew);
            CollectionMapping cm = Utils.genCreateCollectionMapping(collectionNameNew, true, false);
            client.createCollection(cm);
        }
        List<String> listCollections = client.listCollections();
        originCollections.stream().forEach(listCollections::contains);
    }

    // case-06
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class, expectedExceptions = ClientSideMilvusException.class)
    public void testShowCollectionsWithoutConnect(MilvusClient client, String collectionName) {
        client.listCollections();
    }

    // case-07
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropCollection(MilvusClient client, String collectionName) {
        client.dropCollection(collectionName);
        Assert.assertEquals(client.hasCollection(collectionName), false);
//        Thread.currentThread().sleep(1000);
        List<String> collectionNames = client.listCollections();
        Assert.assertFalse(collectionNames.contains(collectionName));
    }

    // case-08
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testDropCollectionNotExisted(MilvusClient client, String collectionName) {
        client.dropCollection(collectionName+"_");
        List<String> collectionNames = client.listCollections();
        Assert.assertTrue(collectionNames.contains(collectionName));
    }

    // case-09
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class, expectedExceptions = ClientSideMilvusException.class)
    public void testDropCollectionWithoutConnect(MilvusClient client, String collectionName) {
        client.dropCollection(collectionName);
    }

    // case-10
    // TODO
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDescribeCollection(MilvusClient client, String collectionName) {
        CollectionMapping info = client.getCollectionInfo(collectionName);
        List<Map<String,Object>> fields = info.getFields();
        int dim = 0;
        for(Map<String,Object> field: fields){
            if (floatVectorFieldName.equals(field.get("field"))) {
                JSONObject jsonObject = JSONObject.parseObject(field.get("params").toString());
                dim = jsonObject.getIntValue("dim");
            }
            continue;
        }
        JSONObject params = JSONObject.parseObject(info.getParamsInJson().toString());
        Assert.assertEquals(dim, Constants.dimension);
        Assert.assertEquals(info.getCollectionName(), collectionName);
        Assert.assertEquals(params.getIntValue("segment_row_limit"), Constants.segmentRowLimit);
    }

    // case-11
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class, expectedExceptions = ClientSideMilvusException.class)
    public void testDescribeCollectionWithoutConnect(MilvusClient client, String collectionName) {
        client.getCollectionInfo(collectionName);
    }

    // case-12
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testHasCollectionNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = collectionName+"_";
        Assert.assertFalse(client.hasCollection(collectionNameNew));
    }

    // case-13
    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class, expectedExceptions = ClientSideMilvusException.class)
    public void testHasCollectionWithoutConnect(MilvusClient client, String collectionName) {
        client.hasCollection(collectionName);
    }

    // case-14
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testHasCollection(MilvusClient client, String collectionName) {
        Assert.assertTrue(client.hasCollection(collectionName));
    }
}
