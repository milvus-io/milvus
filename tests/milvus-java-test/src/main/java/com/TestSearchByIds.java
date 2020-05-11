package com;

import io.milvus.client.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TestSearchByIds {
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
    List<Long> default_ids = Utils.toListIds(1111);
    List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
    List<List<Float>> small_vectors = Utils.genVectors(small_nb, dimension, true);
    List<ByteBuffer> vectorsBinary = Utils.genBinaryVectors(nb, dimension);
    String indexParam = Utils.setIndexParam(n_list);
    public String searchParamStr = Utils.setSearchParam(n_probe);

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_collection_not_existed(MilvusClient client, String collectionName)  {
        String collectionNameNew = collectionName + "_";
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionNameNew)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k)
                .withIDs(default_ids)
                .build();
        SearchResponse res_search = client.searchByIds(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_collection_empty(MilvusClient client, String collectionName)  {
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionName)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k)
                .withIDs(default_ids)
                .build();
        SearchResponse res_search = client.searchByIds(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_no_result(MilvusClient client, String collectionName)  {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        client.flush(collectionName);
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionName)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k)
                .withIDs(default_ids)
                .build();
        List<List<SearchResponse.QueryResult>> res_search = client.searchByIds(searchParam).getQueryResultsList();
        assert (client.searchByIds(searchParam).getResponse().ok());
        Assert.assertEquals(res_search.get(0).size(), 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_count_lt_top_k(MilvusClient client, String collectionName)  {
        int top_k = 100;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(small_vectors).build();
        InsertResponse res_insert = client.insert(insertParam);
        client.flush(collectionName);
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionName)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k)
                .withIDs(Utils.toListIds(res_insert.getVectorIds().get(0)))
                .build();
        List<List<SearchResponse.QueryResult>> res_search = client.searchByIds(searchParam).getQueryResultsList();
        // reason: "Failed to query by id in collection L2_FmVKbqSZaN, result doesn\'t match id count"
        assert (!client.searchByIds(searchParam).getResponse().ok());
//        Assert.assertEquals(res_search.size(), default_ids.size());
//        Assert.assertEquals(res_search.get(0).get(0).getVectorId(), -1);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_index_IVFLAT(MilvusClient client, String collectionName)  {
        IndexType indexType = IndexType.IVFLAT;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        InsertResponse res_insert = client.insert(insertParam);
        client.flush(collectionName);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        client.createIndex(index);
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionName)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k)
                .withIDs(res_insert.getVectorIds())
                .build();
        List<List<SearchResponse.QueryResult>> res_search = client.searchByIds(searchParam).getQueryResultsList();
        for (int i=0; i<vectors.size(); ++i) {
            Assert.assertEquals(res_search.get(i).size(), top_k);
            long vectorId = res_search.get(i).get(0).getVectorId();
            long insertId = res_insert.getVectorIds().get(i);
            Assert.assertEquals(vectorId, insertId);
            double distance = res_search.get(i).get(0).getDistance();
            if (collectionName.startsWith("L2")) {
                Assert.assertEquals(distance, 0.0, epsilon);
            }else if (collectionName.startsWith("IP")) {
                Assert.assertEquals(distance, 1.0, epsilon);
            }
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_partition(MilvusClient client, String collectionName) {
        IndexType indexType = IndexType.IVFLAT;
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).withPartitionTag(tag).build();
        InsertResponse res_insert = client.insert(insertParam);
        client.flush(collectionName);
        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
        client.createIndex(index);
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tag);
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionName)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k)
                .withIDs(Utils.toListIds(res_insert.getVectorIds().get(0)))
                .withPartitionTags(queryTags)
                .build();
        List<List<SearchResponse.QueryResult>> res_search = client.searchByIds(searchParam).getQueryResultsList();
        double distance = res_search.get(0).get(0).getDistance();
        if (collectionName.startsWith("L2")) {
            Assert.assertEquals(distance, 0.0, epsilon);
        }else if (collectionName.startsWith("IP")) {
            Assert.assertEquals(distance, 1.0, epsilon);
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_partition_not_exited(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).withPartitionTag(tag).build();
        client.insert(insertParam);
        client.flush(collectionName);
        String tagNew = RandomStringUtils.randomAlphabetic(10);
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tagNew);
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionName)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k)
                .withIDs(default_ids)
                .withPartitionTags(queryTags)
                .build();
        SearchResponse res_search = client.searchByIds(searchParam);
        assert (!res_search.getResponse().ok());
        Assert.assertEquals(res_search.getQueryResultsList().size(), 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_partition_empty(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).withPartitionTag(tag).build();
        client.insert(insertParam);
        client.flush(collectionName);
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tag);
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionName)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k)
                .withIDs(default_ids)
                .withPartitionTags(queryTags)
                .build();
        SearchResponse res_search = client.searchByIds(searchParam);
        assert (res_search.getResponse().ok());
        Assert.assertEquals(res_search.getQueryResultsList().size(), 1);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_invalid_n_probe(MilvusClient client, String collectionName)  {
        int n_probe_new = 0;
        String searchParamStrNew = Utils.setSearchParam(n_probe_new);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionName)
                .withParamsInJson(searchParamStrNew)
                .withTopK(top_k)
                .withIDs(default_ids)
                .build();
        SearchResponse res_search = client.searchByIds(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void test_search_invalid_top_k(MilvusClient client, String collectionName) {
        int top_k_new = 0;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).build();
        client.insert(insertParam);
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionName)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k_new)
                .withIDs(default_ids)
                .build();
        SearchResponse res_search = client.searchByIds(searchParam);
        assert (!res_search.getResponse().ok());
    }

    // Binary tests
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_search_collection_not_existed_binary(MilvusClient client, String collectionName)  {
        String collectionNameNew = collectionName + "_";
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionNameNew)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k)
                .withIDs(default_ids)
                .build();
        SearchResponse res_search = client.searchByIds(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_search_ids_binary(MilvusClient client, String collectionName)  {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        InsertResponse res_insert = client.insert(insertParam);
        client.flush(collectionName);
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionName)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k)
                .withIDs(res_insert.getVectorIds())
                .build();
        List<List<SearchResponse.QueryResult>> res_search = client.searchByIds(searchParam).getQueryResultsList();
        for (int i = 0; i < top_k; i++) {
            long insert_id = res_insert.getVectorIds().get(i);
            long get_id = res_search.get(i).get(0).getVectorId();
            System.out.println(get_id);
            Assert.assertEquals(insert_id, get_id);
        }
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_search_invalid_n_probe_binary(MilvusClient client, String collectionName)  {
        int n_probe_new = 0;
        String searchParamStrNew = Utils.setSearchParam(n_probe_new);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        client.insert(insertParam);
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionName)
                .withParamsInJson(searchParamStrNew)
                .withTopK(top_k)
                .withIDs(default_ids)
                .build();
        SearchResponse res_search = client.searchByIds(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void test_search_invalid_top_k_binary(MilvusClient client, String collectionName) {
        int top_k_new = 0;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
        client.insert(insertParam);
        SearchByIdsParam searchParam = new SearchByIdsParam.Builder(collectionName)
                .withParamsInJson(searchParamStr)
                .withTopK(top_k_new)
                .withIDs(default_ids)
                .build();
        SearchResponse res_search = client.searchByIds(searchParam);
        assert (!res_search.getResponse().ok());
    }

}
