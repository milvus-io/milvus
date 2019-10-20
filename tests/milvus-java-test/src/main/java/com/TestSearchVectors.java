package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestSearchVectors {
    int index_file_size = 10;
    int dimension = 128;
    int n_list = 1024;
    int default_n_list = 16384;
    int nb = 100000;
    int n_probe = 20;
    int top_k = 10;
    double epsilon = 0.001;
    IndexType indexType = IndexType.IVF_SQ8;
    IndexType defaultIndexType = IndexType.FLAT;


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

    public static Date getDeltaDate(int delta) {
        Date today = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(today);
        c.add(Calendar.DAY_OF_MONTH, delta);
        return c.getTime();
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_table_not_existed(MilvusClient client, String tableName) throws InterruptedException {
        String tableNameNew = tableName + "_";
        int nq = 5;
        int nb = 100;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        SearchParam searchParam = new SearchParam.Builder(tableNameNew, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_index_IVFLAT(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVFLAT;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res_search.size(), nq);
        Assert.assertEquals(res_search.get(0).size(), top_k);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_ids_IVFLAT(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVFLAT;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, true);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        List<Long> vectorIds;
        vectorIds = Stream.iterate(0L, n -> n)
                .limit(nb)
                .collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).withVectorIds(vectorIds).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res_search.get(0).get(0).getVectorId(), 0L);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_IVFLAT(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVFLAT;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Thread.sleep(2000);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res_search.size(), nq);
        Assert.assertEquals(res_search.get(0).size(), top_k);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_distance_IVFLAT(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVFLAT;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, true);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        double distance = res_search.get(0).get(0).getDistance();
        if (tableName.startsWith("L2")) {
            Assert.assertEquals(distance, 0.0, epsilon);
        }else if (tableName.startsWith("IP")) {
            Assert.assertEquals(distance, 1.0, epsilon);
        }
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_index_IVFSQ8(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVF_SQ8;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res_search.size(), nq);
        Assert.assertEquals(res_search.get(0).size(), top_k);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_IVFSQ8(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVF_SQ8;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Thread.sleep(1000);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res_search.size(), nq);
        Assert.assertEquals(res_search.get(0).size(), top_k);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_distance_IVFSQ8(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVF_SQ8;
        int nq = 5;
        int nb = 1000;
        List<List<Float>> vectors = gen_vectors(nb, true);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(default_n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
        List<List<Double>> res_search = client.search(searchParam).getResultDistancesList();
        for (int i = 0; i < nq; i++) {
            double distance = res_search.get(i).get(0);
            System.out.println(distance);
            if (tableName.startsWith("L2")) {
                Assert.assertEquals(distance, 0.0, epsilon);
            }else if (tableName.startsWith("IP")) {
                Assert.assertEquals(distance, 1.0, epsilon);
            }
        }
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_index_FLAT(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.FLAT;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res_search.size(), nq);
        Assert.assertEquals(res_search.get(0).size(), top_k);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_FLAT(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.FLAT;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Thread.sleep(1000);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res_search.size(), nq);
        Assert.assertEquals(res_search.get(0).size(), top_k);
    }

//    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
//    public void test_search_FLAT_timeout(MilvusClient client, String tableName) throws InterruptedException {
//        IndexType indexType = IndexType.FLAT;
//        int nb = 100000;
//        int nq = 1000;
//        int top_k = 2048;
//        List<List<Float>> vectors = gen_vectors(nb, false);
//        List<List<Float>> vectors = gen_vectors(nb, false);
//        List<List<Float>> queryVectors = vectors.subList(0,nq);
//        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
//        client.insert(insertParam);
//        Thread.sleep(1000);
//        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).withTimeout(1).build();
//        System.out.println(new Date());
//        SearchResponse res_search = client.search(searchParam);
//        assert (!res_search.getResponse().ok());
//    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_FLAT_big_data_size(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.FLAT;
        int nb = 100000;
        int nq = 2000;
        int top_k = 2048;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Thread.sleep(1000);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
        System.out.println(new Date());
        SearchResponse res_search = client.search(searchParam);
        assert (res_search.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_distance_FLAT(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.FLAT;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, true);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        double distance = res_search.get(0).get(0).getDistance();
        if (tableName.startsWith("L2")) {
            Assert.assertEquals(distance, 0.0, epsilon);
        }else if (tableName.startsWith("IP")) {
            Assert.assertEquals(distance, 1.0, epsilon);
        }
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_invalid_n_probe(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVF_SQ8;
        int nq = 5;
        int n_probe_new = 0;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe_new).withTopK(top_k).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_invalid_top_k(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVF_SQ8;
        int nq = 5;
        int top_k_new = 0;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k_new).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

//    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
//    public void test_search_invalid_query_vectors(MilvusClient client, String tableName) throws InterruptedException {
//        IndexType indexType = IndexType.IVF_SQ8;
//        int nq = 5;
//        List<List<Float>> vectors = gen_vectors(nb, false);
//        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
//        client.insert(insertParam);
//        Index index = new Index.Builder().withIndexType(indexType)
//                .withNList(n_list)
//                .build();
//        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
//        client.createIndex(createIndexParam);
//        TableParam tableParam = new TableParam.Builder(tableName).build();
//        SearchParam searchParam = new SearchParam.Builder(tableName, null).withNProbe(n_probe).withTopK(top_k).build();
//        SearchResponse res_search = client.search(searchParam);
//        assert (!res_search.getResponse().ok());
//    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_index_range(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVF_SQ8;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        List<DateRange> dateRange = new ArrayList<>();
        dateRange.add(new DateRange(getDeltaDate(-1), getDeltaDate(1)));
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).withDateRanges(dateRange).build();
        SearchResponse res_search = client.search(searchParam);
        assert (res_search.getResponse().ok());
        List<List<SearchResponse.QueryResult>> res = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res.size(), nq);
        Assert.assertEquals(res.get(0).size(), top_k);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_range(MilvusClient client, String tableName) throws InterruptedException {
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        List<DateRange> dateRange = new ArrayList<>();
        dateRange.add(new DateRange(getDeltaDate(-1), getDeltaDate(1)));
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Thread.sleep(1000);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).withDateRanges(dateRange).build();
        SearchResponse res_search = client.search(searchParam);
        assert (res_search.getResponse().ok());
        List<List<SearchResponse.QueryResult>> res = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res.size(), nq);
        Assert.assertEquals(res.get(0).size(), top_k);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_index_range_no_result(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVF_SQ8;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        List<DateRange> dateRange = new ArrayList<>();
        dateRange.add(new DateRange(getDeltaDate(-3), getDeltaDate(-1)));
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).withDateRanges(dateRange).build();
        SearchResponse res_search = client.search(searchParam);
        assert (res_search.getResponse().ok());
        List<List<SearchResponse.QueryResult>> res = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res.size(), 0);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_range_no_result(MilvusClient client, String tableName) throws InterruptedException {
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        List<DateRange> dateRange = new ArrayList<>();
        dateRange.add(new DateRange(getDeltaDate(-3), getDeltaDate(-1)));
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Thread.sleep(1000);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).withDateRanges(dateRange).build();
        SearchResponse res_search = client.search(searchParam);
        assert (res_search.getResponse().ok());
        List<List<SearchResponse.QueryResult>> res = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res.size(), 0);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_index_range_invalid(MilvusClient client, String tableName) throws InterruptedException {
        IndexType indexType = IndexType.IVF_SQ8;
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        List<DateRange> dateRange = new ArrayList<>();
        dateRange.add(new DateRange(getDeltaDate(2), getDeltaDate(-1)));
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder().withIndexType(indexType)
                .withNList(n_list)
                .build();
        CreateIndexParam createIndexParam = new CreateIndexParam.Builder(tableName).withIndex(index).build();
        client.createIndex(createIndexParam);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).withDateRanges(dateRange).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_search_range_invalid(MilvusClient client, String tableName) throws InterruptedException {
        int nq = 5;
        List<List<Float>> vectors = gen_vectors(nb, false);
        List<List<Float>> queryVectors = vectors.subList(0,nq);
        List<DateRange> dateRange = new ArrayList<>();
        dateRange.add(new DateRange(getDeltaDate(2), getDeltaDate(-1)));
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        client.insert(insertParam);
        Thread.sleep(1000);
        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(n_probe).withTopK(top_k).withDateRanges(dateRange).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

}
