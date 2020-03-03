package com;

import io.milvus.client.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TestMix {
    private int dimension = 128;
    private int nb = 100000;
    int n_list = 8192;
    int n_probe = 20;
    int top_k = 10;
    double epsilon = 0.001;
    int index_file_size = 20;

    public List<Float> normalize(List<Float> w2v){
        float squareSum = w2v.stream().map(x -> x * x).reduce((float) 0, Float::sum);
        final float norm = (float) Math.sqrt(squareSum);
        w2v = w2v.stream().map(x -> x / norm).collect(Collectors.toList());
        return w2v;
    }

    public List<List<Float>> gen_vectors(int nb, boolean norm) {
        List<List<Float>> xb = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < nb; ++i) {
            List<Float> vector = new ArrayList<>();
            for (int j = 0; j < dimension; j++) {
                vector.add(random.nextFloat());
            }
            if (norm == true) {
                vector = normalize(vector);
            }
            xb.add(vector);
        }
        return xb;
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_vectors_threads(MilvusClient client, String tableName) throws InterruptedException {
        int thread_num = 10;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(IndexType.IVF_SQ8)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        ForkJoinPool executor = new ForkJoinPool();
        for (int i = 0; i < thread_num; i++) {
            executor.execute(
                    () -> {
                        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
                        SearchResponse res_search = client.search(searchParam);
                        assert (res_search.getResponse().ok());
                    });
        }
        executor.awaitQuiescence(100, TimeUnit.SECONDS);
        executor.shutdown();
    }

    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void test_connect_threads(String host, String port) throws ConnectFailedException {
        int thread_num = 100;
        ForkJoinPool executor = new ForkJoinPool();
        for (int i = 0; i < thread_num; i++) {
            executor.execute(
                    () -> {
                        MilvusClient client = new MilvusGrpcClient();
                        ConnectParam connectParam = new ConnectParam.Builder()
                                .withHost(host)
                                .withPort(port)
                                .build();
                        try {
                            client.connect(connectParam);
                        } catch (ConnectFailedException e) {
                            e.printStackTrace();
                        }
                        assert(client.isConnected());
                        try {
                            client.disconnect();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
        }
        executor.awaitQuiescence(100, TimeUnit.SECONDS);
        executor.shutdown();
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_add_vectors_threads(MilvusClient client, String tableName) throws InterruptedException {
        int thread_num = 10;
        List<List<Float>> vectors = gen_vectors(nb,false);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        ForkJoinPool executor = new ForkJoinPool();
        for (int i = 0; i < thread_num; i++) {
            executor.execute(
                    () -> {
                        InsertResponse res_insert = client.insert(insertParam);
                        assert (res_insert.getResponse().ok());
                    });
        }
        executor.awaitQuiescence(100, TimeUnit.SECONDS);
        executor.shutdown();

        Thread.sleep(2000);
        GetTableRowCountResponse getTableRowCountResponse = client.getTableRowCount(tableName);
        Assert.assertEquals(getTableRowCountResponse.getTableRowCount(), thread_num * nb);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_add_vectors_partition_threads(MilvusClient client, String tableName) throws InterruptedException {
        int thread_num = 10;
        String tag = RandomStringUtils.randomAlphabetic(10);
        String partitionName = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition partition = new io.milvus.client.Partition.Builder(tableName, partitionName, tag).build();
        client.createPartition(partition);
        List<List<Float>> vectors = gen_vectors(nb,false);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).withPartitionTag(tag).build();
        ForkJoinPool executor = new ForkJoinPool();
        for (int i = 0; i < thread_num; i++) {
            executor.execute(
                    () -> {
                        InsertResponse res_insert = client.insert(insertParam);
                        assert (res_insert.getResponse().ok());
                    });
        }
        executor.awaitQuiescence(100, TimeUnit.SECONDS);
        executor.shutdown();

        Thread.sleep(2000);
        GetTableRowCountResponse getTableRowCountResponse = client.getTableRowCount(tableName);
        Assert.assertEquals(getTableRowCountResponse.getTableRowCount(), thread_num * nb);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_add_index_vectors_threads(MilvusClient client, String tableName) throws InterruptedException {
        int thread_num = 50;
        List<List<Float>> vectors = gen_vectors(nb,false);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        ForkJoinPool executor = new ForkJoinPool();
        for (int i = 0; i < thread_num; i++) {
            executor.execute(
                    () -> {
                        InsertResponse res_insert = client.insert(insertParam);
                        Index index = new Index.Builder().withIndexType(IndexType.IVF_SQ8)
                                .withNList(n_list)
                                .build();
                        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
                        client.createIndex(createIndexParam);
                        assert (res_insert.getResponse().ok());
                    });
        }
        executor.awaitQuiescence(300, TimeUnit.SECONDS);
        executor.shutdown();
        Thread.sleep(2000);
        GetTableRowCountResponse getTableRowCountResponse = client.getTableRowCount(tableName);
        Assert.assertEquals(getTableRowCountResponse.getTableRowCount(), thread_num * nb);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_add_search_vectors_threads(MilvusClient client, String tableName) throws InterruptedException {
        int thread_num = 50;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, true);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        ForkJoinPool executor = new ForkJoinPool();
        for (int i = 0; i < thread_num; i++) {
            executor.execute(
                    () -> {
                        InsertResponse res_insert = client.insert(insertParam);
                        assert (res_insert.getResponse().ok());
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
                        SearchResponse res_search = client.search(searchParam);
                        assert (res_search.getResponse().ok());
                        List<List<SearchResponse.QueryResult>> res = client.search(searchParam).getQueryResultsList();
                        double distance = res.get(0).get(0).getDistance();
                        if (tableName.startsWith("L2")) {
                            Assert.assertEquals(distance, 0.0, epsilon);
                        }else if (tableName.startsWith("IP")) {
                            Assert.assertEquals(distance, 1.0, epsilon);
                        }
                    });
        }
        executor.awaitQuiescence(300, TimeUnit.SECONDS);
        executor.shutdown();
        Thread.sleep(2000);
        GetTableRowCountResponse getTableRowCountResponse = client.getTableRowCount(tableName);
        Assert.assertEquals(getTableRowCountResponse.getTableRowCount(), thread_num * nb);
    }

    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void test_create_insert_delete_threads(String host, String port) {
        int thread_num = 100;
        List<List<Float>> vectors = gen_vectors(nb,false);
        ForkJoinPool executor = new ForkJoinPool();
        for (int i = 0; i < thread_num; i++) {
            executor.execute(
                    () -> {
                        MilvusClient client = new MilvusGrpcClient();
                        ConnectParam connectParam = new ConnectParam.Builder()
                                .withHost(host)
                                .withPort(port)
                                .build();
                        try {
                            client.connect(connectParam);
                        } catch (ConnectFailedException e) {
                            e.printStackTrace();
                        }
                        assert(client.isConnected());
                        String tableName = RandomStringUtils.randomAlphabetic(10);
                        TableSchema tableSchema = new TableSchema.Builder(tableName, dimension)
                                                                 .withIndexFileSize(index_file_size)
                                                                 .withMetricType(MetricType.IP)
                                                                 .build();
                        client.createTable(tableSchema);
                        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
                        client.insert(insertParam);
                        Response response = client.dropTable(tableName);
                        Assert.assertTrue(response.ok());
                        try {
                            client.disconnect();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
        }
        executor.awaitQuiescence(100, TimeUnit.SECONDS);
        executor.shutdown();
    }

}
