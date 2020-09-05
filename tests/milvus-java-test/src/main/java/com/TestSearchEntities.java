package com;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.*;

public class TestSearchEntities {

    int n_probe = 20;
    int top_k = Constants.topk;
    int nq = Constants.nq;

    List<List<Float>> queryVectors = Constants.vectors.subList(0, nq);
    List<ByteBuffer> queryVectorsBinary = Constants.vectorsBinary.subList(0, nq);

    public String dsl = Constants.searchParam;

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchCollectionNotExisted(MilvusClient client, String collectionName)  {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        SearchParam searchParam = new SearchParam.Builder(collectionNameNew).withDSL(dsl).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchCollectionEmpty(MilvusClient client, String collectionName)  {
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse res_search = client.search(searchParam);
        assert (res_search.getResponse().ok());
        Assert.assertEquals(res_search.getResultIdsList().size(), 0);
    }

    // # 3429
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchCollection(MilvusClient client, String collectionName)  {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse res_search = client.search(searchParam);
        Assert.assertEquals(res_search.getResultIdsList().size(), Constants.nq);
        Assert.assertEquals(res_search.getResultDistancesList().size(), Constants.nq);
        Assert.assertEquals(res_search.getResultIdsList().get(0).size(), Constants.topk);
        Assert.assertEquals(res_search.getResultIdsList().get(0).get(0), ids.get(0));
        Assert.assertEquals(res_search.getFieldsMap().get(0).size(), top_k);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchDistance(MilvusClient client, String collectionName)  {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse res_search = client.search(searchParam);
        for (int i = 0; i < Constants.nq; i++) {
            double distance = res_search.getResultDistancesList().get(i).get(0);
            Assert.assertEquals(distance, 0.0, Constants.epsilon);
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchDistanceIP(MilvusClient client, String collectionName)  {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        String dsl = Utils.setSearchParam("IP", Constants.vectors.subList(0, nq), top_k, n_probe);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse res_search = client.search(searchParam);
        for (int i = 0; i < Constants.nq; i++) {
            double distance = res_search.getResultDistancesList().get(i).get(0);
            Assert.assertEquals(distance, 1.0, Constants.epsilon);
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchPartition(MilvusClient client, String collectionName) {
        String tag = "tag";
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tag);
        client.createPartition(collectionName, tag);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        client.flush(collectionName);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).withPartitionTags(queryTags).build();
        SearchResponse res_search = client.search(searchParam);
        Assert.assertEquals(res_search.getResultDistancesList().size(), 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchPartitionNotExited(MilvusClient client, String collectionName) {
        String tag = Utils.genUniqueStr("tag");
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tag);
        client.createPartition(collectionName, tag);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert (res.getResponse().ok());
        client.flush(collectionName);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).withPartitionTags(queryTags).build();
        SearchResponse res_search = client.search(searchParam);
        Assert.assertEquals(res_search.getResultDistancesList().size(), 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchInvalidNProbe(MilvusClient client, String collectionName)  {
        int n_probe_new = -1;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
        Response res_create = client.createIndex(index);
        String dsl = Utils.setSearchParam(Constants.defaultMetricType, Constants.vectors.subList(0, nq), top_k, n_probe_new);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse res_search = client.search(searchParam);
        assert(!res_search.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchCountLessThanTopK(MilvusClient client, String collectionName) {
        int top_k_new = 100;
        int nb = 50;
        List<Map<String,Object>> entities = Utils.genDefaultEntities(Constants.dimension, nb, Utils.genVectors(nb, Constants.dimension, false));
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(entities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        String dsl = Utils.setSearchParam(Constants.defaultMetricType, Constants.vectors.subList(0, nq), top_k_new, n_probe);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse res_search = client.search(searchParam);
        assert(res_search.getResponse().ok());
        Assert.assertEquals(res_search.getResultIdsList().size(), Constants.nq);
        Assert.assertEquals(res_search.getResultDistancesList().size(), Constants.nq);
        Assert.assertEquals(res_search.getResultIdsList().get(0).size(), nb);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchInvalidTopK(MilvusClient client, String collectionName) {
        int top_k = -1;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
//        Index index = new Index.Builder(collectionName, Constants.floatFieldName).withParamsInJson(Constants.indexParam).build();
//        Response res_create = client.createIndex(index);
        String dsl = Utils.setSearchParam(Constants.defaultMetricType, Constants.vectors.subList(0, nq), top_k, n_probe);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse res_search = client.search(searchParam);
        assert(!res_search.getResponse().ok());
    }

    // TODO
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchMultiMust(MilvusClient client, String collectionName){
        JSONObject vectorParam = Utils.genVectorParam(Constants.defaultMetricType, Constants.vectors.subList(0,nq), top_k, n_probe);
        JSONObject boolParam = new JSONObject();
        JSONObject mustParam = new JSONObject();
        JSONArray jsonArray = new JSONArray();
        // [vector]
        jsonArray.add(vectorParam);
        JSONObject mustParam1 = new JSONObject();
        // must:[]
        mustParam1.put("must", jsonArray);
        JSONArray jsonArray1 = new JSONArray();
        // [must:[]]
        jsonArray1.add(mustParam1);
        mustParam.put("must", jsonArray1);
        boolParam.put("bool", mustParam);
//        insert
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        String query = boolParam.toJSONString();
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(query).build();
        SearchResponse resSearch = client.search(searchParam);
        assert(resSearch.getResponse().ok());
        Assert.assertEquals(resSearch.getResultIdsList().size(), Constants.nq);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), Constants.nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), Constants.topk);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchMultiVectors(MilvusClient client, String collectionName) {
        String dsl = String.format(
                "{\"bool\": {"
                        + "\"must\": [{"
                        + "    \"range\": {"
                        + "        \"int64\": {\"GT\": -10, \"LT\": 1000}"
                        + "    }},{"
                        + "    \"vector\": {"
                        + "        \"float_vector\": {"
                        + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": %s, \"params\": {\"nprobe\": 20}"
                        + "    }}},{"
                        + "    \"vector\": {"
                        + "        \"float_vector\": {"
                        + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": %s, \"params\": {\"nprobe\": 20}\"\n"
                        + "    }}}]}}",
                top_k, queryVectors, top_k, queryVectors);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse resSearch = client.search(searchParam);
        Assert.assertFalse(resSearch.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchNoVectors(MilvusClient client, String collectionName) {
        String dsl = String.format(
                "{\"bool\": {"
                        + "\"must\": [{"
                        + "    \"range\": {"
                        + "        \"int64\": {\"GT\": -10, \"LT\": 1000}"
                        + "    }},{"
                        + "    \"vector\": {"
                        + "        \"float_vector\": {"
                        + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"params\": {\"nprobe\": 20}"
                        + "    }}}]}}",
                top_k);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse resSearch = client.search(searchParam);
        Assert.assertFalse(resSearch.getResponse().ok());
    }

    // #3599
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void  testSearchVectorNotExisted(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        client.flush(collectionName);
        List<List<Float>> query = Utils.genVectors(nq,64, false);
        String dsl = String.format(
                "{\"bool\": {"
                        + "\"must\": [{"
                        + "    \"range\": {"
                        + "        \"int64\": {\"GT\": -10, \"LT\": 1000}"
                        + "    }},{"
                        + "    \"vector\": {"
                        + "        \"float_vector\": {"
                        + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": %s, \"params\": {\"nprobe\": 20}"
                        + "    }}}]}}",
                top_k, query);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse resSearch = client.search(searchParam);
        Assert.assertFalse(resSearch.getResponse().ok());
    }

    // #3601
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void  testSearchVectorDifferentDim(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        client.flush(collectionName);
        List<List<Float>> query = Utils.genVectors(nq,64, false);
        String dsl = String.format(
                "{\"bool\": {"
                        + "\"must\": [{"
                        + "    \"range\": {"
                        + "        \"int64\": {\"GT\": -10, \"LT\": 1000}"
                        + "    }},{"
                        + "    \"vector\": {"
                        + "        \"float_vector\": {"
                        + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": %s, \"params\": {\"nprobe\": 20}"
                        + "    }}}]}}",
                top_k, query);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse resSearch = client.search(searchParam);
        Assert.assertFalse(resSearch.getResponse().ok());
    }

    // Binary tests
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testSearchCollectionNotExistedBinary(MilvusClient client, String collectionName)  {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        String queryKey = "placeholder";
        Map<String, List<ByteBuffer>> binaryQueryEntities = new HashMap<>();
        binaryQueryEntities.put(queryKey, queryVectorsBinary);
        JSONObject binaryVectorParam = Utils.genBinaryVectorParam(Constants.defaultBinaryMetricType, queryKey, top_k, n_probe);
        List<JSONObject> leafParams = new ArrayList<>();
        leafParams.add(binaryVectorParam);
        String dsl = Utils.genDefaultSearchParam(leafParams);
        System.out.println(dsl);
        SearchParam searchParam = new SearchParam.Builder(collectionNameNew)
                .withBinaryEntities(binaryQueryEntities)
                .withDSL(dsl)
                .build();
        SearchResponse resSearch = client.search(searchParam);
        Assert.assertFalse(resSearch.getResponse().ok());
    }

    // TODO
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testSearchCollectionBinary(MilvusClient client, String collectionName)  {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        String queryKey = "placeholder";
        Map<String, List<ByteBuffer>> binaryQueryEntities = new HashMap<>();
        binaryQueryEntities.put(queryKey, queryVectorsBinary);
        JSONObject binaryVectorParam = Utils.genBinaryVectorParam(Constants.defaultBinaryMetricType, queryKey, top_k, n_probe);
        List<JSONObject> leafParams = new ArrayList<>();
        leafParams.add(binaryVectorParam);
        String dsl = Utils.genDefaultSearchParam(leafParams);
        System.out.println(dsl);
        SearchParam searchParam = new SearchParam.Builder(collectionName)
                .withDSL(dsl)
                .withBinaryEntities(binaryQueryEntities)
                .build();
        SearchResponse resSearch = client.search(searchParam);
        Assert.assertTrue(resSearch.getResponse().ok());
        Assert.assertEquals(resSearch.getResultIdsList().size(), nq);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), top_k);
    }
//
//    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
//    public void test_search_index_IVFLAT_binary(MilvusClient client, String collectionName)  {
//        IndexType indexType = IndexType.IVFLAT;
//        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
//        InsertResponse res = client.insert(insertParam);
//        client.flush(collectionName);
//        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
//        client.createIndex(index);
//        SearchParam searchParam = new SearchParam.Builder(collectionName)
//                .withBinaryVectors(queryVectorsBinary)
//                .withParamsInJson(searchParamStr)
//                .withTopK(top_k).build();
//        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
//        Assert.assertEquals(res_search.size(), nq);
//        Assert.assertEquals(res_search.get(0).size(), top_k);
//    }
//
//    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
//    public void test_search_ids_IVFLAT_binary(MilvusClient client, String collectionName)  {
//        IndexType indexType = IndexType.IVFLAT;
//        List<Long> vectorIds;
//        vectorIds = Stream.iterate(0L, n -> n)
//                .limit(nb)
//                .collect(Collectors.toList());
//        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).withVectorIds(vectorIds).build();
//        InsertResponse res = client.insert(insertParam);
//        Index index = new Index.Builder(collectionName, indexType).withParamsInJson(indexParam).build();
//        client.createIndex(index);
//        SearchParam searchParam = new SearchParam.Builder(collectionName)
//                .withBinaryVectors(queryVectorsBinary)
//                .withParamsInJson(searchParamStr)
//                .withTopK(top_k).build();
//        List<List<SearchResponse.QueryResult>> res_search = client.search(searchParam).getQueryResultsList();
//        Assert.assertEquals(res_search.get(0).get(0).getVectorId(), 0L);
//    }
//
//    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
//    public void test_search_partition_not_existed_binary(MilvusClient client, String collectionName) {
//        IndexType indexType = IndexType.IVFLAT;
//        String tag = RandomStringUtils.randomAlphabetic(10);
//        Response createpResponse = client.createPartition(collectionName, tag);
//        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
//        InsertResponse res = client.insert(insertParam);
//        String tagNew = RandomStringUtils.randomAlphabetic(10);
//        List<String> queryTags = new ArrayList<>();
//        queryTags.add(tagNew);
//        SearchParam searchParam = new SearchParam.Builder(collectionName)
//                .withBinaryVectors(queryVectorsBinary)
//                .withParamsInJson(searchParamStr)
//                .withPartitionTags(queryTags)
//                .withTopK(top_k).build();
//        SearchResponse res_search = client.search(searchParam);
//        assert (!res_search.getResponse().ok());
//        Assert.assertEquals(res_search.getQueryResultsList().size(), 0);
//    }
//
//    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
//    public void test_search_invalid_n_probe_binary(MilvusClient client, String collectionName)  {
//        int n_probe_new = 0;
//        String searchParamStrNew = Utils.setSearchParam(n_probe_new);
//        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
//        client.insert(insertParam);
//        SearchParam searchParam = new SearchParam.Builder(collectionName)
//                .withBinaryVectors(queryVectorsBinary)
//                .withParamsInJson(searchParamStrNew)
//                .withTopK(top_k).build();
//        SearchResponse res_search = client.search(searchParam);
//        assert (res_search.getResponse().ok());
//    }
//
//    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
//    public void test_search_invalid_top_k_binary(MilvusClient client, String collectionName) {
//        int top_k_new = 0;
//        InsertParam insertParam = new InsertParam.Builder(collectionName).withBinaryVectors(vectorsBinary).build();
//        client.insert(insertParam);
//        SearchParam searchParam = new SearchParam.Builder(collectionName)
//                .withBinaryVectors(queryVectorsBinary)
//                .withParamsInJson(searchParamStr)
//                .withTopK(top_k_new).build();
//        SearchResponse res_search = client.search(searchParam);
//        assert (!res_search.getResponse().ok());
//    }

}
