package com;

import io.milvus.client.InsertParam;
import io.milvus.client.InsertResponse;
import io.milvus.client.MilvusClient;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestAddVectors {
    int dimension = 128;

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
    public void test_add_vectors_table_not_existed(MilvusClient client, String tableName) throws InterruptedException {
        int nb = 10000;
        List<List<Float>> vectors = gen_vectors(nb);
        String tableNameNew = tableName + "_";
        InsertParam insertParam = new InsertParam.Builder(tableNameNew, vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_add_vectors_without_connect(MilvusClient client, String tableName) throws InterruptedException {
        int nb = 100;
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_add_vectors(MilvusClient client, String tableName) throws InterruptedException {
        int nb = 10000;
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Thread.currentThread().sleep(1000);
        // Assert table row count
        Assert.assertEquals(client.getTableRowCount(tableName).getTableRowCount(), nb);
    }

//    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
//    public void test_add_vectors_timeout(MilvusClient client, String tableName) throws InterruptedException {
//        int nb = 200000;
//        List<List<Float>> vectors = gen_vectors(nb);
//        System.out.println(new Date());
//        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).withTimeout(1).build();
//        InsertResponse res = client.insert(insertParam);
//        assert(!res.getResponse().ok());
//    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_add_vectors_big_data(MilvusClient client, String tableName) throws InterruptedException {
        int nb = 500000;
        List<List<Float>> vectors = gen_vectors(nb);
        System.out.println(new Date());
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_add_vectors_with_ids(MilvusClient client, String tableName) throws InterruptedException {
        int nb = 10000;
        List<List<Float>> vectors = gen_vectors(nb);
        // Add vectors with ids
        List<Long> vectorIds;
        vectorIds = Stream.iterate(0L, n -> n)
                .limit(nb)
                .collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).withVectorIds(vectorIds).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Thread.currentThread().sleep(2000);
        // Assert table row count
        Assert.assertEquals(client.getTableRowCount(tableName).getTableRowCount(), nb);
    }

    // TODO: MS-628
    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_add_vectors_with_invalid_ids(MilvusClient client, String tableName) {
        int nb = 10;
        List<List<Float>> vectors = gen_vectors(nb);
        // Add vectors with ids
        List<Long> vectorIds;
        vectorIds = Stream.iterate(0L, n -> n)
                .limit(nb+1)
                .collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).withVectorIds(vectorIds).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_add_vectors_with_invalid_dimension(MilvusClient client, String tableName) {
        int nb = 10000;
        List<List<Float>> vectors = gen_vectors(nb);
        vectors.get(0).add((float) 0);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_add_vectors_with_invalid_vectors(MilvusClient client, String tableName) {
        int nb = 10000;
        List<List<Float>> vectors = gen_vectors(nb);
        vectors.set(0, new ArrayList<>());
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_add_vectors_repeatably(MilvusClient client, String tableName) throws InterruptedException {
        int nb = 100000;
        int loops = 10;
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        InsertResponse res = null;
        for (int i = 0; i < loops; ++i ) {
            long startTime = System.currentTimeMillis();
            res = client.insert(insertParam);
            long endTime = System.currentTimeMillis();
            System.out.println("Total execution time: " + (endTime-startTime) + "ms");
        }
        Thread.currentThread().sleep(1000);
        // Assert table row count
        Assert.assertEquals(client.getTableRowCount(tableName).getTableRowCount(), nb * loops);
    }

}
