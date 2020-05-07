package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.logging.Logger;

public class TestIndex {
    int dimension = 128;
    int n_list = 1024;
    int default_n_list = 16384;
    int nb = 100000;
    IndexType indexType = IndexType.IVF_SQ8;
    IndexType defaultIndexType = IndexType.FLAT;
    String indexParam = Utils.setIndexParam(n_list);

    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_create_index(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_create_index_binary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_create_index_repeatably(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        DescribeIndexResponse res = client.describeIndex(collectionName);
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
        DescribeIndexResponse res = client.describeIndex(collectionName);
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
        DescribeIndexResponse res = client.describeIndex(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_create_index_IVFSQ8(MilvusClient client, String collectionName) {
        IndexType indexType = IndexType.IVF_SQ8;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        DescribeIndexResponse res = client.describeIndex(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_create_index_IVFSQ8H(MilvusClient client, String collectionName) {
        IndexType indexType = IndexType.IVF_SQ8_H;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        DescribeIndexResponse res = client.describeIndex(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_create_index_with_no_vector(MilvusClient client, String collectionName) {
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_create_index_table_not_existed(MilvusClient client, String collectionName) {
        String collectionNameNew = collectionName + "_";
        Index index = new Index.Builder(collectionNameNew, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_index_without_connect(MilvusClient client, String collectionName) {
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_create_index_invalid_n_list(MilvusClient client, String collectionName) {
        int n_list = 0;
        String indexParamNew = Utils.setIndexParam(n_list);
        Index createIndexParam = new Index.Builder(collectionName, indexType).withParamsInJson(indexParamNew).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(!res_create.ok());
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_create_index_invalid_n_list_binary(MilvusClient client, String collectionName) {
        int n_list = 0;
        String indexParamNew = Utils.setIndexParam(n_list);
        Index createIndexParam = new Index.Builder(collectionName, indexType).withParamsInJson(indexParamNew).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(!res_create.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_describe_index(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        DescribeIndexResponse res = client.describeIndex(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(Utils.getIndexParamValue(index1.getParamsInJson(), "nlist"), n_list);
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_describe_index_binary(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(!res_create.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_after_index(MilvusClient client, String collectionName) {
        int n_list = 1025;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        Response res_create = client.createIndex(index);
        assert(res_create.ok());
        // Create another index
        IndexType indexTypeNew = IndexType.IVFLAT;
        String indexParam = Utils.setIndexParam(n_list);
        Index indexNew = new Index.Builder(collectionName, indexTypeNew).withParamsInJson(indexParam).build();
        Response resNew = client.createIndex(indexNew);
        assert(resNew.ok());
        DescribeIndexResponse res = client.describeIndex(collectionName);
        assert(res_create.ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(Utils.getIndexParamValue(index1.getParamsInJson(), "nlist"), n_list);
        Assert.assertEquals(index1.getIndexType(), indexTypeNew);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_describe_index_table_not_existed(MilvusClient client, String collectionName) {
        String collectionNameNew = collectionName + "_";
        DescribeIndexResponse res = client.describeIndex(collectionNameNew);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_describe_index_without_connect(MilvusClient client, String collectionName) {
        DescribeIndexResponse res = client.describeIndex(collectionName);
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
        DescribeIndexResponse res2 = client.describeIndex(collectionName);
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
        DescribeIndexResponse res2 = client.describeIndex(collectionName);
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
        DescribeIndexResponse res_desc = client.describeIndex(collectionName);
        assert(res_desc.getResponse().ok());
        Index index1 = res_desc.getIndex().get();
        Assert.assertEquals(Utils.getIndexParamValue(index1.getParamsInJson(), "nlist"), 0);
        Assert.assertEquals(index1.getIndexType(), defaultIndexType);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_drop_index_table_not_existed(MilvusClient client, String collectionName) {
        String collectionNameNew = collectionName + "_";
        Response res_drop = client.dropIndex(collectionNameNew);
        assert(!res_drop.ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_drop_index_without_connect(MilvusClient client, String collectionName) {
        Response res_drop = client.dropIndex(collectionName);
        assert(!res_drop.ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_drop_index_no_index_created(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Response res_drop = client.dropIndex(collectionName);
        assert(res_drop.ok());
        DescribeIndexResponse res = client.describeIndex(collectionName);
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
        DescribeIndexResponse res = client.describeIndex(collectionName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), defaultIndexType);
    }
}
