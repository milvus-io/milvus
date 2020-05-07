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
    int nq = 10;
    int n_list = 8192;
    int n_probe = 20;
    int top_k = 10;
    double epsilon = 0.001;
    int index_file_size = 20;
    String indexParam = "{\"nlist\":\"1024\"}";

    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<List<Float>> queryVectors = vectors.subList(0,nq);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_vectors_threads(MilvusClient client, String collectionName) throws InterruptedException {
        int thread_num = 10;
        int nq = 5;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, IndexType.IVF_SQ8)
                .withParamsInJson(indexParam)
                .build();
        client.createIndex(index);
        ForkJoinPool executor = new ForkJoinPool();
        for (int i = 0; i < thread_num; i++) {
            executor.execute(
                    () -> {
                        String params = "{\"nprobe\":\"1\"}";
                        SearchParam searchParam = new SearchParam.Builder(collectionName)
                                .withFloatVectors(queryVectors)
                                .withParamsInJson(params)
                                .withTopK(top_k).build();
                        SearchResponse res_search = client.search(searchParam);
                        assert (res_search.getResponse().ok());
                    });
        }
        executor.awaitQuiescence(100, TimeUnit.SECONDS);
        executor.shutdown();
    }

    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void test_connect_threads(String host, int port) throws ConnectFailedException {
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

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_vectors_threads(MilvusClient client, String collectionName) throws InterruptedException {
        int thread_num = 10;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
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
        GetCollectionRowCountResponse getCollectionRowCountResponse = client.getCollectionRowCount(collectionName);
        Assert.assertEquals(getCollectionRowCountResponse.getCollectionRowCount(), thread_num * nb);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_vectors_partition_threads(MilvusClient client, String collectionName) throws InterruptedException {
        int thread_num = 10;
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).withPartitionTag(tag).build();
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
        GetCollectionRowCountResponse getCollectionRowCountResponse = client.getCollectionRowCount(collectionName);
        Assert.assertEquals(getCollectionRowCountResponse.getCollectionRowCount(), thread_num * nb);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_index_vectors_threads(MilvusClient client, String collectionName) throws InterruptedException {
        int thread_num = 50;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        ForkJoinPool executor = new ForkJoinPool();
        for (int i = 0; i < thread_num; i++) {
            executor.execute(
                    () -> {
                        InsertResponse res_insert = client.insert(insertParam);
                        Index index = new Index.Builder(collectionName, IndexType.IVF_SQ8)
                                .withParamsInJson("{\"nlist\":\"1024\"}")
                                .build();
                        client.createIndex(index);
                        assert (res_insert.getResponse().ok());
                    });
        }
        executor.awaitQuiescence(300, TimeUnit.SECONDS);
        executor.shutdown();
        Thread.sleep(2000);
        GetCollectionRowCountResponse getCollectionRowCountResponse = client.getCollectionRowCount(collectionName);
        Assert.assertEquals(getCollectionRowCountResponse.getCollectionRowCount(), thread_num * nb);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_add_search_vectors_threads(MilvusClient client, String collectionName) throws InterruptedException {
        int thread_num = 50;
        int nq = 5;
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
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
                        SearchParam searchParam = new SearchParam.Builder(collectionName)
                                .withFloatVectors(queryVectors)
                                .withParamsInJson("{\"nlist\":\"1024\"}")
                                .withTopK(top_k).build();
                        SearchResponse res_search = client.search(searchParam);
                        assert (res_search.getResponse().ok());
                        List<List<SearchResponse.QueryResult>> res = client.search(searchParam).getQueryResultsList();
                        double distance = res.get(0).get(0).getDistance();
                        if (collectionName.startsWith("L2")) {
                            Assert.assertEquals(distance, 0.0, epsilon);
                        }else if (collectionName.startsWith("IP")) {
                            Assert.assertEquals(distance, 1.0, epsilon);
                        }
                    });
        }
        executor.awaitQuiescence(300, TimeUnit.SECONDS);
        executor.shutdown();
        Thread.sleep(2000);
        GetCollectionRowCountResponse getCollectionRowCountResponse = client.getCollectionRowCount(collectionName);
        Assert.assertEquals(getCollectionRowCountResponse.getCollectionRowCount(), thread_num * nb);
    }

    @Test(dataProvider = "DefaultConnectArgs", dataProviderClass = MainClass.class)
    public void test_create_insert_delete_threads(String host, int port) {
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
                        String collectionName = RandomStringUtils.randomAlphabetic(10);
                        CollectionMapping tableSchema = new CollectionMapping.Builder(collectionName, dimension)
                                                                 .withIndexFileSize(index_file_size)
                                                                 .withMetricType(MetricType.IP)
                                                                 .build();
                        client.createCollection(tableSchema);
                        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
                        client.insert(insertParam);
                        Response response = client.dropCollection(collectionName);
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
