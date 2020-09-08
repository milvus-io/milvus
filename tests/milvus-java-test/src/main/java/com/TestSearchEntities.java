package com;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class TestSearchEntities {

    int n_probe = 20;
    int top_k = Constants.topk;
    int nq = Constants.nq;
    String binaryIndexType = "BIN_IVF_FLAT";

    List<List<Float>> queryVectors = Constants.vectors.subList(0, nq);
    List<List<Byte>> queryVectorsBinary = Constants.vectorsBinary.subList(0, nq);

    public String floatDsl = Constants.searchParam;
    public String binaryDsl = Constants.binarySearchParam;

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchCollectionNotExisted(MilvusClient client, String collectionName)  {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        SearchParam searchParam = new SearchParam.Builder(collectionNameNew).withDSL(floatDsl).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchCollectionEmpty(MilvusClient client, String collectionName)  {
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(floatDsl).build();
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
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(floatDsl).build();
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
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(floatDsl).build();
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
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(floatDsl).withPartitionTags(queryTags).build();
        SearchResponse res_search = client.search(searchParam);
        Assert.assertEquals(res_search.getResultDistancesList().size(), 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchPartitionNotExited(MilvusClient client, String collectionName) {
        String tag = Utils.genUniqueStr("tag");
        String tagNew = Utils.genUniqueStr("tagNew");
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tagNew);
        client.createPartition(collectionName, tag);
        InsertParam insertParam = new InsertParam.Builder(collectionName)
                .withFields(Constants.defaultEntities)
                .withPartitionTag(tag)
                .build();
        InsertResponse res = client.insert(insertParam);
        assert (res.getResponse().ok());
        client.flush(collectionName);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(floatDsl).withPartitionTags(queryTags).build();
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

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void  testSearchVectorNotExisted(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        client.flush(collectionName);
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
                top_k, new ArrayList<>());
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse resSearch = client.search(searchParam);
        Assert.assertTrue(resSearch.getResponse().ok());
        Assert.assertEquals(resSearch.getResultIdsList().size(), 0);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), 0);
    }

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

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testAsyncSearch(MilvusClient client, String collectionName) {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        List<Long> ids = res.getEntityIds();
        client.flush(collectionName);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(floatDsl).build();
        ListenableFuture<SearchResponse> searchResFuture = client.searchAsync(searchParam);
        Futures.addCallback(
                searchResFuture, new FutureCallback<SearchResponse>() {
                    @Override
                    public void onSuccess(SearchResponse searchResponse) {
                        Assert.assertNotNull(searchResponse);
                        Assert.assertTrue(searchResponse.ok());
                        Assert.assertEquals(searchResponse.getResultIdsList().size(), Constants.nq);
                        Assert.assertEquals(searchResponse.getResultDistancesList().size(), Constants.nq);
                        Assert.assertEquals(searchResponse.getResultIdsList().get(0).size(), Constants.topk);
                        Assert.assertEquals(searchResponse.getFieldsMap().get(0).size(), top_k);
                    }
                    @Override
                    public void onFailure(Throwable t) {
                        System.out.println(t.getMessage());
                    }
                }, MoreExecutors.directExecutor()
        );
    }

    // Binary tests
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testSearchCollectionNotExistedBinary(MilvusClient client, String collectionName)  {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        SearchParam searchParam = new SearchParam.Builder(collectionNameNew).withDSL(binaryDsl).build();
        SearchResponse resSearch = client.search(searchParam);
        Assert.assertFalse(resSearch.getResponse().ok());
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testSearchCollectionBinary(MilvusClient client, String collectionName)  {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        client.flush(collectionName);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(binaryDsl).build();
        SearchResponse resSearch = client.search(searchParam);
        Assert.assertTrue(resSearch.getResponse().ok());
        Assert.assertEquals(resSearch.getResultIdsList().size(), nq);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), top_k);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testSearchIndexIVFLATBinary(MilvusClient client, String collectionName)  {
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        InsertResponse resInsert = client.insert(insertParam);
        client.flush(collectionName);
        String binaryIndexParam = Utils.setIndexParam(binaryIndexType, Constants.defaultBinaryMetricType, Constants.n_list);
        Index index = new Index.Builder(collectionName, Constants.binaryFieldName).withParamsInJson(binaryIndexParam).build();
        Response resBuild = client.createIndex(index);
        Assert.assertTrue(resBuild.ok());
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(binaryDsl).build();
        SearchResponse resSearch = client.search(searchParam);
        Assert.assertTrue(resSearch.getResponse().ok());
        Assert.assertEquals(resSearch.getResultIdsList().size(), nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), top_k);
    }

    @Test(dataProvider = "BinaryIdCollection", dataProviderClass = MainClass.class)
    public void testSearchIdsIVFLATBinary(MilvusClient client, String collectionName)  {
        List<Long> entityIds = LongStream.range(0, Constants.nb).boxed().collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).withEntityIds(entityIds).build();
        InsertResponse res = client.insert(insertParam);
        String binaryIndexParam = Utils.setIndexParam(binaryIndexType, Constants.defaultBinaryMetricType, Constants.n_list);
        Index index = new Index.Builder(collectionName, Constants.binaryFieldName).withParamsInJson(binaryIndexParam).build();
        Response resBuild = client.createIndex(index);
        Assert.assertTrue(res.ok());
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(binaryDsl).build();
        SearchResponse resSearch = client.search(searchParam);
        Assert.assertTrue(resSearch.getResponse().ok());
        Assert.assertEquals(resSearch.getResultIdsList().size(), nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), top_k);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).get(0), entityIds.get(0));
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testSearchPartitionNotExistedBinary(MilvusClient client, String collectionName) {
        String tag = Utils.genUniqueStr("tag");
        Response createpResponse = client.createPartition(collectionName, tag);
        assert (createpResponse.ok());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).withPartitionTag(tag).build();
        InsertResponse res = client.insert(insertParam);
        client.flush(collectionName);
        String tagNew = Utils.genUniqueStr("tagNew");
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tagNew);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(binaryDsl).withPartitionTags(queryTags).build();
        SearchResponse res_search = client.search(searchParam);
        Assert.assertEquals(res_search.getResultDistancesList().size(), 0);
    }

//    #3656
    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testSearchInvalidNProbeBinary(MilvusClient client, String collectionName)  {
        int n_probe_new = 0;
        String dsl = Utils.setBinarySearchParam(Constants.defaultBinaryMetricType, queryVectorsBinary, top_k, n_probe_new);
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        client.insert(insertParam);
        String binaryIndexParam = Utils.setIndexParam(binaryIndexType, Constants.defaultBinaryMetricType, Constants.n_list);
        Index index = new Index.Builder(collectionName, Constants.binaryFieldName).withParamsInJson(binaryIndexParam).build();
        Response resBuild = client.createIndex(index);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testSearchInvalidTopKBinary(MilvusClient client, String collectionName) {
        int top_k_new = 0;
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFields(Constants.defaultBinaryEntities).build();
        client.insert(insertParam);
        client.flush(collectionName);
        String dsl = Utils.setBinarySearchParam(Constants.defaultBinaryMetricType, queryVectorsBinary, top_k_new, n_probe);
        SearchParam searchParam = new SearchParam.Builder(collectionName).withDSL(dsl).build();
        SearchResponse res_search = client.search(searchParam);
        assert (!res_search.getResponse().ok());
    }

}
