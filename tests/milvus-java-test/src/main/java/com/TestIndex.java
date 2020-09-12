package com;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.logging.Logger;

public class TestIndex {

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndex(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
<<<<<<< HEAD
        Response statsResponse = client.getCollectionStats(collectionName);
        // TODO: should check getCollectionStats
        if(statsResponse.ok()) {
            JSONArray filesJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "files");
            filesJsonArray.stream().map(item-> (JSONObject)item).filter(item->item.containsKey("index_type")).forEach(file->
                    Assert.assertEquals(file.get("index_type"), Constants.indexType));
        }
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCreateIndexBinary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
=======
        GetIndexInfoResponse res = client.getIndexInfo(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(Utils.getIndexParamValue(index1.getParamsInJson(), "nlist"), n_list);
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void  test_create_index_FLAT(MilvusClient client, String collectionName) {
        IndexType indexType = IndexType.FLAT;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        Logger.getLogger("a").info("start insert");
        client.insert(insertParam);
        Logger.getLogger("a").info("end insert");
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        Logger.getLogger("a").info("end create");
        GetIndexInfoResponse res = client.getIndexInfo(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_create_index_IVFLAT(MilvusClient client, String collectionName) {
        IndexType indexType = IndexType.IVFLAT;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        GetIndexInfoResponse res = client.getIndexInfo(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_create_index_IVFSQ8(MilvusClient client, String collectionName) {
        IndexType indexType = IndexType.IVF_SQ8;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.binaryFieldName).withParamsInJson(Constants.binaryIndexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
<<<<<<< HEAD
        Response statsResponse = client.getCollectionStats(collectionName);
        // TODO: should check getCollectionStats
        if(statsResponse.ok()) {
            JSONArray filesJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "files");
            filesJsonArray.stream().map(item-> (JSONObject)item).filter(item->item.containsKey("index_type")).forEach(file->
                    Assert.assertEquals(file.get("index_type"), Constants.defaultBinaryIndexType));
        }
=======
        GetIndexInfoResponse res = client.getIndexInfo(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), indexType);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexRepeatably(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
<<<<<<< HEAD
        Response res_create_2 = client.createIndex(index);
        assert(res_create_2.ok());
=======
        GetIndexInfoResponse res = client.getIndexInfo(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), indexType);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexWithNoVector(MilvusClient client, String collectionName) {
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexTableNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        Index index = new Index.Builder(collectionNameNew, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void testCreateIndexWithoutConnect(MilvusClient client, String collectionName) {
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreateIndexInvalidNList(MilvusClient client, String collectionName) {
        int n_list = 0;
        String indexParamNew = Utils.setIndexParam(Constants.indexType, "L2", n_list);
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(indexParamNew).build();
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    // # 3407
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testCreateIndexInvalidMetricTypeBinary(MilvusClient client, String collectionName) {
        String metric_type = "L2";
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        client.insert(insertParam);
        String indexParamNew = Utils.setIndexParam(Constants.defaultBinaryIndexType, metric_type, Constants.n_list);
        Index createIndexParam = new Index.Builder(collectionName, Constants.binaryFieldName).withParamsInJson(indexParamNew).build();
        Response res_create = client.createIndex(createIndexParam);
        assert (!res_create.ok());
    }

    // #3408
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropIndex(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
<<<<<<< HEAD
        Response res_drop = client.dropIndex(collectionName, Constants.floatFieldName);
        assert(res_drop.ok());
        Response statsResponse = client.getCollectionStats(collectionName);
        // TODO: should check getCollectionStats
        if(statsResponse.ok()) {
            JSONArray filesJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "files");
            filesJsonArray.stream().map(item -> (JSONObject) item).forEach(file->{
                assert (!file.containsKey("index_type"));
            });
        }
=======
        GetIndexInfoResponse res = client.getIndexInfo(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(Utils.getIndexParamValue(index1.getParamsInJson(), "nlist"), n_list);
        Assert.assertEquals(index1.getIndexType(), indexType);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testDropIndexBinary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, Constants.binaryFieldName).withParamsInJson(Constants.binaryIndexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
<<<<<<< HEAD
        Response res_drop = client.dropIndex(collectionName, Constants.binaryFieldName);
        assert(res_drop.ok());
        Response statsResponse = client.getCollectionStats(collectionName);
        // TODO: should check getCollectionStats
        if(statsResponse.ok()) {
            JSONArray filesJsonArray = Utils.parseJsonArray(statsResponse.getMessage(), "files");
            filesJsonArray.stream().map(item -> (JSONObject) item).forEach(file->{
                assert (!file.containsKey("index_type"));
            });
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropIndexTableNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        Response res_drop = client.dropIndex(collectionNameNew, Constants.floatFieldName);
=======
        // Create another index
        IndexType indexTypeNew = IndexType.IVFLAT;
        String indexParam = Utils.setIndexParam(n_list);
        Index indexNew = new Index.Builder(collectionName, indexTypeNew).withParamsInJson(indexParam).build();
        Response resNew = client.createIndex(indexNew);
        assert(resNew.ok());
        GetIndexInfoResponse res = client.getIndexInfo(collectionName);
        assert(res_create.ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(Utils.getIndexParamValue(index1.getParamsInJson(), "nlist"), n_list);
        Assert.assertEquals(index1.getIndexType(), indexTypeNew);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_describe_index_table_not_existed(MilvusClient client, String collectionName) {
        String collectionNameNew = collectionName + "_";
        GetIndexInfoResponse res = client.getIndexInfo(collectionNameNew);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_describe_index_without_connect(MilvusClient client, String collectionName) {
        GetIndexInfoResponse res = client.getIndexInfo(collectionName);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_drop_index(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Index indexNew = new Index.Builder(collectionName, defaultIndexType).withParamsInJson(indexParam).build();
        Response res = client.createIndex(indexNew);
        assert(res.ok());
        Response res_drop = client.dropIndex(collectionName);
        assert(res_drop.ok());
        GetIndexInfoResponse res2 = client.getIndexInfo(collectionName);
        assert(res2.getResponse().ok());
        Index index1 = res2.getIndex().get();
        Assert.assertEquals(Utils.getIndexParamValue(index1.getParamsInJson(), "nlist"), 0);
        Assert.assertEquals(index1.getIndexType(), defaultIndexType);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_drop_index_binary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        client.insert(insertParam);
        Index indexNew = new Index.Builder(collectionName, defaultIndexType).withParamsInJson(indexParam).build();
        Response res = client.createIndex(indexNew);
        assert(res.ok());
        Response res_drop = client.dropIndex(collectionName);
        assert(res_drop.ok());
        GetIndexInfoResponse res2 = client.getIndexInfo(collectionName);
        assert(res2.getResponse().ok());
        Index index1 = res2.getIndex().get();
        Assert.assertEquals(Utils.getIndexParamValue(index1.getParamsInJson(), "nlist"), 0);
        Assert.assertEquals(index1.getIndexType(), defaultIndexType);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_drop_index_repeatably(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, defaultIndexType).withParamsInJson(indexParam).build();
        Response res = client.createIndex(index);
        assert(res.ok());
        Response res_drop = client.dropIndex(collectionName);
        assert(res_drop.ok());
        GetIndexInfoResponse res_desc = client.getIndexInfo(collectionName);
        assert(res_desc.getResponse().ok());
        Index index1 = res_desc.getIndex().get();
        Assert.assertEquals(Utils.getIndexParamValue(index1.getParamsInJson(), "nlist"), 0);
        Assert.assertEquals(index1.getIndexType(), defaultIndexType);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_drop_index_table_not_existed(MilvusClient client, String collectionName) {
        String collectionNameNew = collectionName + "_";
        Response res_drop = client.dropIndex(collectionNameNew);
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
        assert(!res_drop.ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void testDropIndexWithoutConnect(MilvusClient client, String collectionName) {
        Response res_drop = client.dropIndex(collectionName, Constants.floatFieldName);
        assert(!res_drop.ok());
    }

<<<<<<< HEAD
=======
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_drop_index_no_index_created(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Response res_drop = client.dropIndex(collectionName);
        assert(res_drop.ok());
        GetIndexInfoResponse res = client.getIndexInfo(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), defaultIndexType);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_drop_index_no_index_created_binary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        client.insert(insertParam);
        Response res_drop = client.dropIndex(collectionName);
        assert(res_drop.ok());
        GetIndexInfoResponse res = client.getIndexInfo(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), defaultIndexType);
    }
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
}
