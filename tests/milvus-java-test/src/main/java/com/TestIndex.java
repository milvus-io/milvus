package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class TestIndex {
    int index_file_size = 10;
    int dimension = 128;
    int n_list = 1024;
    int default_n_list = 16384;
    int nb = 100000;
    IndexType indexType = IndexType.IVF_SQ8;
    IndexType defaultIndexType = IndexType.FLAT;

    public List<List<Float>> gen_vectors(Integer nb) {
        List<List<Float>> xb = new LinkedList<>();
        Random random = new Random();
        for (int i = 0; i < nb; ++i) {
            LinkedList<Float> vector = new LinkedList<>();
            for (int j = 0; j < dimension; j++) {
                vector.add(random.nextFloat());
            }
            xb.add(vector);
        }
        return xb;
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_create_index(MilvusClient client, String tableName) throws InterruptedException {
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(res_create.ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_create_index_repeatably(MilvusClient client, String tableName) throws InterruptedException {
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        res_create = client.createIndex(createIndexParam);
        assert(res_create.ok());
        DescribeIndexResponse res = client.describeIndex(tableName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getNList(), n_list);
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_create_index_FLAT(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.FLAT;
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(res_create.ok());
        DescribeIndexResponse res = client.describeIndex(tableName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

//    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
//    public void test_create_index_FLAT_timeout(MilvusClient client, String tableName) throws InterruptedException {
//        int nb = 500000;
//        IndexType indexType = IndexType.IVF_SQ8;
//        List<List<Float>> vectors = gen_vectors(nb);
//        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
//        client.insert(insertParam);
//        Index index = new Index.Builder().withIndexType(indexType)
//                .withNList(n_list)
//                .build();
//        System.out.println(new Date());
//        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).withTimeout(1).build();
//        Response res_create = client.createIndex(createIndexParam);
//        assert(!res_create.ok());
//    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_create_index_IVFLAT(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVFLAT;
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(res_create.ok());
        DescribeIndexResponse res = client.describeIndex(tableName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_create_index_IVFSQ8(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVF_SQ8;
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(res_create.ok());
        DescribeIndexResponse res = client.describeIndex(tableName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_create_index_IVFSQ8H(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVF_SQ8_H;
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(res_create.ok());
        DescribeIndexResponse res = client.describeIndex(tableName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_create_index_with_no_vector(MilvusClient client, String tableName) {
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(res_create.ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_create_index_table_not_existed(MilvusClient client, String tableName) throws InterruptedException {
        String tableNameNew = tableName + "_";
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableNameNew).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(!res_create.ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_index_without_connect(MilvusClient client, String tableName) throws InterruptedException {
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(!res_create.ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_create_index_invalid_n_list(MilvusClient client, String tableName) throws InterruptedException {
        int n_list = 0;
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(!res_create.ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_describe_index(MilvusClient client, String tableName) throws InterruptedException {
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(res_create.ok());
        DescribeIndexResponse res = client.describeIndex(tableName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getNList(), n_list);
        Assert.assertEquals(index1.getIndexType(), indexType);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_alter_index(MilvusClient client, String tableName) throws InterruptedException {
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(res_create.ok());
        // Create another index
        IndexType indexTypeNew = IndexType.IVFLAT;
        int n_list_new = n_list + 1;
        Index index_new = new Index.Builder().withIndexType(indexTypeNew)
                .withNList(n_list_new)
                .build();
        CreateIndexParam createIndexParamNew = new CreateIndexParam.Builder(tableName).withIndex(index_new).build();
        Response res_create_new = client.createIndex(createIndexParamNew);
        assert(res_create_new.ok());
        DescribeIndexResponse res = client.describeIndex(tableName);
        assert(res_create.ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getNList(), n_list_new);
        Assert.assertEquals(index1.getIndexType(), indexTypeNew);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_describe_index_table_not_existed(MilvusClient client, String tableName) throws InterruptedException {
        String tableNameNew = tableName + "_";
        DescribeIndexResponse res = client.describeIndex(tableNameNew);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_describe_index_without_connect(MilvusClient client, String tableName) throws InterruptedException {
        DescribeIndexResponse res = client.describeIndex(tableName);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_drop_index(MilvusClient client, String tableName) throws InterruptedException {
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(defaultIndexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(res_create.ok());
        Response res_drop = client.dropIndex(tableName);
        assert(res_drop.ok());
        DescribeIndexResponse res = client.describeIndex(tableName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getNList(), default_n_list);
        Assert.assertEquals(index1.getIndexType(), defaultIndexType);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_drop_index_repeatably(MilvusClient client, String tableName) throws InterruptedException {
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(defaultIndexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        Response res_create = client.createIndex(createIndexParam);
        assert(res_create.ok());
        Response res_drop = client.dropIndex(tableName);
        res_drop = client.dropIndex(tableName);
        assert(res_drop.ok());
        DescribeIndexResponse res = client.describeIndex(tableName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getNList(), default_n_list);
        Assert.assertEquals(index1.getIndexType(), defaultIndexType);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_drop_index_table_not_existed(MilvusClient client, String tableName) throws InterruptedException {
        String tableNameNew = tableName + "_";
        Response res_drop = client.dropIndex(tableNameNew);
        assert(!res_drop.ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_drop_index_without_connect(MilvusClient client, String tableName) throws InterruptedException {
        Response res_drop = client.dropIndex(tableName);
        assert(!res_drop.ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_drop_index_no_index_created(MilvusClient client, String tableName) throws InterruptedException {
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Response res_drop = client.dropIndex(tableName);
        assert(res_drop.ok());
        DescribeIndexResponse res = client.describeIndex(tableName);
        assert(res.getResponse().ok());
        Index index1 = res.getIndex().get();
        Assert.assertEquals(index1.getNList(), default_n_list);
        Assert.assertEquals(index1.getIndexType(), defaultIndexType);
    }

}
