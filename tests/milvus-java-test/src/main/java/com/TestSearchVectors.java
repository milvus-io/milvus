package com;

import io.milvus.client.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestSearchVectors {
    int dimension = 128;
    int n_list = 1024;
    int default_n_list = 16384;
    int nb = 10000;
    int small_nb = 10;
    int n_probe = 20;
    int top_k = 10;
    int nq = 5;
    double epsilon = 0.001;
    IndexType indexType = IndexType.IVF_SQ8;
    IndexType defaultIndexType = IndexType.FLAT;
    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<List<Float>> small_vectors = Utils.genVectors(small_nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);
    List<List<Float>> queryVectors = vectors.subList(0, nq);
    List<ByteBuffer> queryVectorsBinary = vectorsBinary.subList(0, nq);
    String indexParam = Utils.setIndexParam(n_list);
    public String searchParamStr = Utils.setSearchParam(n_probe);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_collection_not_existed(MilvusClient client, String collectionName)  {
        String collectionNameNew = collectionName + "_";
        SearchParam searchParam = new SearchParam.Builder(collectionNameNew)
                .withFloatVectors(queryVectors)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_index_IVFLAT(MilvusClient client, String collectionName)  {
        IndexType indexType = IndexType.IVFLAT;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        client.flush(collectionName);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        client.createIndex(index);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withFloatVectors(queryVectors)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res_search.size(), nq);
        Assert.assertEquals(res_search.get(0).size(), top_k);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_ids_IVFLAT(MilvusClient client, String collectionName)  {
        IndexType indexType = IndexType.IVFLAT;
        List<Long> vectorIds;
        vectorIds = Stream.iterate(0L, n -> n)
                .limit(nb)
                .collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).withVectorIds(vectorIds).build();
        InsertResponse res = client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        client.createIndex(index);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withFloatVectors(queryVectors)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res_search.get(0).get(0).getVectorId(), 0L);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_distance_IVFLAT(MilvusClient client, String collectionName)  {
        IndexType indexType = IndexType.IVFLAT;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        client.createIndex(index);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withFloatVectors(queryVectors)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        double distance = res_search.get(0).get(0).getDistance();
        if (collectionName.startsWith("L2")) {
            Assert.assertEquals(distance, 0.0, epsilon);
        }else if (collectionName.startsWith("IP")) {
            Assert.assertEquals(distance, 1.0, epsilon);
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_partition(MilvusClient client, String collectionName) {
        IndexType indexType = IndexType.IVFLAT;
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        client.createIndex(index);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withFloatVectors(queryVectors)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        double distance = res_search.get(0).get(0).getDistance();
        if (collectionName.startsWith("L2")) {
            Assert.assertEquals(distance, 0.0, epsilon);
        }else if (collectionName.startsWith("IP")) {
            Assert.assertEquals(distance, 1.0, epsilon);
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_partition_not_exited(MilvusClient client, String collectionName) {
        IndexType indexType = IndexType.IVFLAT;
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        String tagNew = RandomStringUtils.randomAlphabetic(10);
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tagNew);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withFloatVectors(queryVectors)
                .withParamsInJson(searchParamStr)
                .withPartitionTags(queryTags)
                .withTopK(top_k).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
        Assert.assertEquals(res_search.getQueryResultsList().size(), 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_partition_empty(MilvusClient client, String collectionName) {
        IndexType indexType = IndexType.IVFLAT;
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        String tagNew = "";
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tagNew);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withFloatVectors(queryVectors)
                .withParamsInJson(searchParamStr)
                .withPartitionTags(queryTags)
                .withTopK(top_k).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_distance_FLAT(MilvusClient client, String collectionName)  {
        IndexType indexType = IndexType.FLAT;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res = client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        client.createIndex(index);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withFloatVectors(queryVectors)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        double distance = res_search.get(0).get(0).getDistance();
        if (collectionName.startsWith("L2")) {
            Assert.assertEquals(distance, 0.0, epsilon);
        }else if (collectionName.startsWith("IP")) {
            Assert.assertEquals(distance, 1.0, epsilon);
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_invalid_n_probe(MilvusClient client, String collectionName)  {
        int n_probe_new = -1;
        String searchParamStrNew = Utils.setSearchParam(n_probe_new);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        client.createIndex(index);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withFloatVectors(queryVectors)
                .withParamsInJson(searchParamStrNew)
                .withTopK(top_k).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_count_lt_top_k(MilvusClient client, String collectionName) {
        int top_k_new = 100;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(small_vectors).build();
        client.insert(insertParam);
        client.flush(collectionName);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withFloatVectors(queryVectors)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k_new).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res_search.size(), nq);
        Assert.assertEquals(res_search.get(0).size(), small_vectors.size());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_invalid_top_k(MilvusClient client, String collectionName) {
        int top_k_new = 0;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withFloatVectors(queryVectors)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k_new).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

    // Binary tests
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_search_collection_not_existed_binary(MilvusClient client, String collectionName)  {
        String collectionNameNew = collectionName + "_";
        SearchParam searchParam = new SearchParam.Builder(collectionNameNew)
                .withBinaryVectors(queryVectorsBinary)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_search_index_IVFLAT_binary(MilvusClient client, String collectionName)  {
        IndexType indexType = IndexType.IVFLAT;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse res = client.insert(insertParam);
        client.flush(collectionName);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        client.createIndex(index);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withBinaryVectors(queryVectorsBinary)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res_search.size(), nq);
        Assert.assertEquals(res_search.get(0).size(), top_k);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_search_ids_IVFLAT_binary(MilvusClient client, String collectionName)  {
        IndexType indexType = IndexType.IVFLAT;
        List<Long> vectorIds;
        vectorIds = Stream.iterate(0L, n -> n)
                .limit(nb)
                .collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).withVectorIds(vectorIds).build();
        InsertResponse res = client.insert(insertParam);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        client.createIndex(index);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withBinaryVectors(queryVectorsBinary)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k).build();
        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
        Assert.assertEquals(res_search.get(0).get(0).getVectorId(), 0L);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_search_partition_not_existed_binary(MilvusClient client, String collectionName) {
        IndexType indexType = IndexType.IVFLAT;
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse res = client.insert(insertParam);
        String tagNew = RandomStringUtils.randomAlphabetic(10);
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tagNew);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withBinaryVectors(queryVectorsBinary)
                .withParamsInJson(searchParamStr)
                .withPartitionTags(queryTags)
                .withTopK(top_k).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
        Assert.assertEquals(res_search.getQueryResultsList().size(), 0);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_search_invalid_n_probe_binary(MilvusClient client, String collectionName)  {
        int n_probe_new = 0;
        String searchParamStrNew = Utils.setSearchParam(n_probe_new);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        client.insert(insertParam);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withBinaryVectors(queryVectorsBinary)
                .withParamsInJson(searchParamStrNew)
                .withTopK(top_k).build();
        SearchResponse res_search = client.search(searchParam);
        assert (res_search.getResponse().ok());
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_search_invalid_top_k_binary(MilvusClient client, String collectionName) {
        int top_k_new = 0;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        client.insert(insertParam);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withBinaryVectors(queryVectorsBinary)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k_new).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

}
