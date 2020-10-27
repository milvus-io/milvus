package com;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.milvus.client.DataType;
import io.milvus.client.Index;
import io.milvus.client.IndexType;
import io.milvus.client.InsertParam;
import io.milvus.client.JsonBuilder;
import io.milvus.client.MetricType;
import io.milvus.client.MilvusClient;
import io.milvus.client.SearchParam;
import io.milvus.client.SearchResult;
import io.milvus.client.exception.InvalidDsl;
import io.milvus.client.exception.ServerSideMilvusException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSearchEntities {

    public String floatDsl = Constants.searchParam;
    public String binaryDsl = Constants.binarySearchParam;
    int n_probe = 20;
    int top_k = Constants.topk;
    int nq = Constants.nq;
    List<List<Float>> queryVectors = Constants.vectors.subList(0, nq);

    @Test(
            dataProvider = "Collection",
            dataProviderClass = MainClass.class,
            expectedExceptions = ServerSideMilvusException.class)
    public void testSearchCollectionNotExisted(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        SearchParam searchParam = SearchParam.create(collectionNameNew).setDsl(floatDsl);
        client.search(searchParam);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchCollectionEmpty(MilvusClient client, String collectionName) {
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(floatDsl);
        SearchResult res_search = client.search(searchParam);
        Assert.assertEquals(res_search.getResultIdsList().size(), 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchCollection(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        List<Long> ids = client.insert(insertParam);
        client.flush(collectionName);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(floatDsl);
        SearchResult res_search = client.search(searchParam);
        Assert.assertEquals(res_search.getResultIdsList().size(), Constants.nq);
        Assert.assertEquals(res_search.getResultDistancesList().size(), Constants.nq);
        Assert.assertEquals(res_search.getResultIdsList().get(0).size(), Constants.topk);
        Assert.assertEquals(res_search.getResultIdsList().get(0).get(0), ids.get(0));
        Assert.assertEquals(res_search.getFieldsMap().get(0).size(), top_k);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchDistance(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        client.insert(insertParam);
        client.flush(collectionName);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(floatDsl);
        SearchResult res_search = client.search(searchParam);
        for (int i = 0; i < Constants.nq; i++) {
            double distance = res_search.getResultDistancesList().get(i).get(0);
            Assert.assertEquals(distance, 0.0, Constants.epsilon);
        }
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchDistanceIP(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        client.insert(insertParam);
        client.flush(collectionName);
        String dsl = Utils.setSearchParam(MetricType.IP, queryVectors, top_k, n_probe);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        SearchResult res_search = client.search(searchParam);
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
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        client.insert(insertParam);
        client.flush(collectionName);
        SearchParam searchParam =
                SearchParam.create(collectionName).setDsl(floatDsl).setPartitionTags(queryTags);
        SearchResult res_search = client.search(searchParam);
        Assert.assertEquals(res_search.getResultDistancesList().size(), 0);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchPartitionNotExisted(MilvusClient client, String collectionName) {
        String tag = Utils.genUniqueStr("tag");
        String tagNew = Utils.genUniqueStr("tagNew");
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tagNew);
        client.createPartition(collectionName, tag);
        Map<String, List> entities = Constants.defaultEntities;
        InsertParam insertParam =
                InsertParam.create(collectionName)
                        .addField(
                                Constants.intFieldName,
                                DataType.INT64,
                                entities.get(Constants.intFieldName))
                        .addField(
                                Constants.floatFieldName,
                                DataType.FLOAT,
                                entities.get(Constants.floatFieldName))
                        .addVectorField(
                                Constants.floatVectorFieldName,
                                DataType.VECTOR_FLOAT,
                                entities.get(Constants.floatVectorFieldName));
        client.insert(insertParam);
        client.flush(collectionName);
        SearchParam searchParam =
                SearchParam.create(collectionName).setDsl(floatDsl).setPartitionTags(queryTags);
        SearchResult res_search = client.search(searchParam);
        Assert.assertEquals(res_search.getResultDistancesList().size(), 0);
    }

    @Test(
            dataProvider = "Collection",
            dataProviderClass = MainClass.class,
            expectedExceptions = ServerSideMilvusException.class)
    public void testSearchInvalidNProbe(MilvusClient client, String collectionName) {
        int n_probe_new = -1;
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        client.insert(insertParam);
        client.flush(collectionName);
        Index index =
                Index.create(collectionName, Constants.floatVectorFieldName)
                        .setIndexType(IndexType.IVF_SQ8)
                        .setMetricType(MetricType.L2)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
        String dsl =
                Utils.setSearchParam(Constants.defaultMetricType, queryVectors, top_k, n_probe_new);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        client.search(searchParam);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchCountLessThanTopK(MilvusClient client, String collectionName) {
        int top_k_new = 100;
        int nb = 50;
        Map<String, List> entities =
                Utils.genDefaultEntities(nb, Utils.genVectors(nb, Constants.dimension, false));
        InsertParam insertParam =
                InsertParam.create(collectionName)
                        .addField(
                                Constants.intFieldName,
                                DataType.INT64,
                                entities.get(Constants.intFieldName))
                        .addField(
                                Constants.floatFieldName,
                                DataType.FLOAT,
                                entities.get(Constants.floatFieldName))
                        .addVectorField(
                                Constants.floatVectorFieldName,
                                DataType.VECTOR_FLOAT,
                                entities.get(Constants.floatVectorFieldName));
        client.insert(insertParam);
        client.flush(collectionName);
        String dsl =
                Utils.setSearchParam(Constants.defaultMetricType, queryVectors, top_k_new, n_probe);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        SearchResult res_search = client.search(searchParam);
        Assert.assertEquals(res_search.getResultIdsList().size(), Constants.nq);
        Assert.assertEquals(res_search.getResultDistancesList().size(), Constants.nq);
        Assert.assertEquals(res_search.getResultIdsList().get(0).size(), nb);
    }

    @Test(
            dataProvider = "Collection",
            dataProviderClass = MainClass.class,
            expectedExceptions = ServerSideMilvusException.class)
    public void testSearchInvalidTopK(MilvusClient client, String collectionName) {
        int top_k = -1;
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        client.insert(insertParam);
        client.flush(collectionName);
        String dsl =
                Utils.setSearchParam(Constants.defaultMetricType, queryVectors, top_k, n_probe);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        client.search(searchParam);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchMultiMust(MilvusClient client, String collectionName) {
        JSONObject vectorParam =
                Utils.genVectorParam(
                        Constants.defaultMetricType,
                        Constants.vectors.subList(0, nq),
                        top_k,
                        n_probe);
        JSONObject boolParam = new JSONObject();
        JSONObject mustParam = new JSONObject();
        JSONArray jsonArray = new JSONArray();
        jsonArray.add(vectorParam);
        JSONObject mustParam1 = new JSONObject();
        mustParam1.put("must", jsonArray);
        JSONArray jsonArray1 = new JSONArray();
        jsonArray1.add(mustParam1);
        mustParam.put("must", jsonArray1);
        boolParam.put("bool", mustParam);
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        client.insert(insertParam);
        client.flush(collectionName);
        String dsl = boolParam.toJSONString();
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        SearchResult resSearch = client.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), Constants.nq);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), Constants.nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), Constants.topk);
    }

    @Test(
            dataProvider = "Collection",
            dataProviderClass = MainClass.class,
            expectedExceptions = InvalidDsl.class)
    public void testSearchMultiVectors(MilvusClient client, String collectionName) {
        String dsl =
                String.format(
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
                                + "    }}"
                                + "]}}",
                        top_k, queryVectors, top_k, queryVectors);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        client.search(searchParam);
    }

    @Test(
            dataProvider = "Collection",
            dataProviderClass = MainClass.class,
            expectedExceptions = InvalidDsl.class)
    public void testSearchNoVectors(MilvusClient client, String collectionName) {
        String dsl =
                String.format(
                        "{\"bool\": {"
                                + "\"must\": [{"
                                + "    \"range\": {"
                                + "        \"int64\": {\"GT\": -10, \"LT\": 1000}"
                                + "    }},{"
                                + "    \"vector\": {"
                                + "        \"float_vector\": {"
                                + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"params\": {\"nprobe\": 20}"
                                + "    }}}"
                                + "]}}",
                        top_k);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        client.search(searchParam);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchVectorNotExisted(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        client.insert(insertParam);
        client.flush(collectionName);
        String dsl =
                String.format(
                        "{\"bool\": {"
                                + "\"must\": [{"
                                + "    \"range\": {"
                                + "        \"int64\": {\"GT\": -10, \"LT\": 1000}"
                                + "    }},{"
                                + "    \"vector\": {"
                                + "        \"float_vector\": {"
                                + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": %s, \"params\": {\"nprobe\": 20}"
                                + "    }}}"
                                + "]}}",
                        top_k, new ArrayList<>());
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        SearchResult resSearch = client.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), 0);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), 0);
    }

    @Test(
            dataProvider = "Collection",
            dataProviderClass = MainClass.class,
            expectedExceptions = ServerSideMilvusException.class)
    public void testSearchVectorDifferentDim(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        client.insert(insertParam);
        client.flush(collectionName);
        List<List<Float>> query = Utils.genVectors(nq, 64, false);
        String dsl =
                String.format(
                        "{\"bool\": {"
                                + "\"must\": [{"
                                + "    \"range\": {"
                                + "        \"int64\": {\"GT\": -10, \"LT\": 1000}"
                                + "    }},{"
                                + "    \"vector\": {"
                                + "        \"float_vector\": {"
                                + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": %s, \"params\": {\"nprobe\": 20}"
                                + "    }}}"
                                + "]}}",
                        top_k, query);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        client.search(searchParam);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testAsyncSearch(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        client.insert(insertParam);
        client.flush(collectionName);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(floatDsl);
        ListenableFuture<SearchResult> searchResFuture = client.searchAsync(searchParam);
        Futures.addCallback(
                searchResFuture,
                new FutureCallback<SearchResult>() {
                    @Override
                    public void onSuccess(SearchResult searchResult) {
                        Assert.assertNotNull(searchResult);
                        Assert.assertEquals(searchResult.getResultIdsList().size(), Constants.nq);
                        Assert.assertEquals(
                                searchResult.getResultDistancesList().size(), Constants.nq);
                        Assert.assertEquals(
                                searchResult.getResultIdsList().get(0).size(), Constants.topk);
                        Assert.assertEquals(searchResult.getFieldsMap().get(0).size(), top_k);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        System.out.println(t.getMessage());
                    }
                },
                MoreExecutors.directExecutor());
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchVectorsWithTerm(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        client.insert(insertParam);
        client.flush(collectionName);
        String dsl =
                String.format(
                        "{\"bool\": {"
                                + "\"must\": [{"
                                + "    \"term\": {"
                                + "        \"int64\": [0,1]"
                                + "    }},{"
                                + "    \"vector\": {"
                                + "        \"float_vector\": {"
                                + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": %s, \"params\": {\"nprobe\": 20}"
                                + "    }}}"
                                + "]}}",
                        top_k, queryVectors);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        SearchResult resSearch = client.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), Constants.nq);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), Constants.nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), 2);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testSearchEntitiesLessThanTopK(MilvusClient client, String collectionName) {
        InsertParam insertParam = Utils.genInsertParam(collectionName);
        client.insert(insertParam);
        client.flush(collectionName);
        List<List<Float>> query = Utils.genVectors(nq, Constants.dimension, false);
        String dsl =
                String.format(
                        "{\"bool\": {"
                                + "\"must\": [{"
                                + "    \"range\": {"
                                + "        \"int64\": {\"GT\": -1, \"LT\": 1}"
                                + "    }},{"
                                + "    \"vector\": {"
                                + "        \"float_vector\": {"
                                + "            \"topk\": %d, \"metric_type\": \"L2\", \"type\": \"float\", \"query\": %s, \"params\": {\"nprobe\": 20}"
                                + "    }}}"
                                + "]}}",
                        top_k, query);
        SearchParam searchParam =
                SearchParam.create(collectionName)
                        .setDsl(dsl)
                        .setParamsInJson("{\"fields\": [\"int64\", \"float_vector\"]}");
        SearchResult searchResult = client.search(searchParam);
        Assert.assertEquals(searchResult.getQueryResultsList().get(0).size(), 1);
    }

    // Binary tests
    @Test(
            dataProvider = "BinaryCollection",
            dataProviderClass = MainClass.class,
            expectedExceptions = ServerSideMilvusException.class)
    public void testSearchCollectionNotExistedBinary(MilvusClient client, String collectionName) {
        String collectionNameNew = Utils.genUniqueStr(collectionName);
        SearchParam searchParam = SearchParam.create(collectionNameNew).setDsl(binaryDsl);
        client.search(searchParam);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testSearchCollectionBinary(MilvusClient client, String collectionName) {
        List<Long> intValues = new ArrayList<>(Constants.nb);
        List<Float> floatValues = new ArrayList<>(Constants.nb);
        List<ByteBuffer> vectors = Utils.genBinaryVectors(Constants.nb, Constants.dimension);
        for (int i = 0; i < Constants.nb; ++i) {
            intValues.add((long) i);
            floatValues.add((float) i);
        }
        InsertParam insertParam =
                InsertParam.create(collectionName)
                        .addField(Constants.intFieldName, DataType.INT64, intValues)
                        .addField(Constants.floatFieldName, DataType.FLOAT, floatValues)
                        .addVectorField(
                                Constants.binaryVectorFieldName, DataType.VECTOR_BINARY, vectors);
        client.insert(insertParam);
        client.flush(collectionName);
        List<String> vectorsToSearch =
                vectors.subList(0, Constants.nq).stream()
                        .map(byteBuffer -> Arrays.toString(byteBuffer.array()))
                        .collect(Collectors.toList());
        String dsl =
                String.format(
                        "{\"bool\": {"
                                + "\"must\": [{"
                                + "    \"vector\": {"
                                + "        \"binary_vector\": {"
                                + "            \"topk\": %d, \"metric_type\": \"JACCARD\", \"type\": \"binary\", \"query\": %s, \"params\": {\"nprobe\": 20}"
                                + "    }}"
                                + "}]}}",
                        top_k, vectorsToSearch.toString());
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        SearchResult resSearch = client.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), nq);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), top_k);
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testSearchIVFLATBinary(MilvusClient client, String collectionName) {
        List<Long> intValues = new ArrayList<>(Constants.nb);
        List<Float> floatValues = new ArrayList<>(Constants.nb);
        List<ByteBuffer> vectors = Utils.genBinaryVectors(Constants.nb, Constants.dimension);
        for (int i = 0; i < Constants.nb; ++i) {
            intValues.add((long) i);
            floatValues.add((float) i);
        }
        InsertParam insertParam =
                InsertParam.create(collectionName)
                        .addField(Constants.intFieldName, DataType.INT64, intValues)
                        .addField(Constants.floatFieldName, DataType.FLOAT, floatValues)
                        .addVectorField(
                                Constants.binaryVectorFieldName, DataType.VECTOR_BINARY, vectors);
        List<Long> entityIds = client.insert(insertParam);
        client.flush(collectionName);
        Index index =
                Index.create(collectionName, Constants.binaryVectorFieldName)
                        .setIndexType(IndexType.BIN_FLAT)
                        .setMetricType(Constants.defaultBinaryMetricType)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
        SearchParam searchParam =
                SearchParam.create(collectionName)
                        .setDsl(
                                Utils.setBinarySearchParam(
                                        Constants.defaultBinaryMetricType,
                                        vectors.subList(0, Constants.nq),
                                        Constants.topk,
                                        n_probe));
        SearchResult resSearch = client.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), top_k);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).get(0), entityIds.get(0));
    }

    @Test(dataProvider = "BinaryCollection", dataProviderClass = MainClass.class)
    public void testSearchPartitionNotExistedBinary(MilvusClient client, String collectionName) {
        String tag = Utils.genUniqueStr("tag");
        client.createPartition(collectionName, tag);
        List<Long> intValues = new ArrayList<>(Constants.nb);
        List<Float> floatValues = new ArrayList<>(Constants.nb);
        List<ByteBuffer> vectors = Utils.genBinaryVectors(Constants.nb, Constants.dimension);
        for (int i = 0; i < Constants.nb; ++i) {
            intValues.add((long) i);
            floatValues.add((float) i);
        }
        InsertParam insertParam =
                InsertParam.create(collectionName)
                        .addField(Constants.intFieldName, DataType.INT64, intValues)
                        .addField(Constants.floatFieldName, DataType.FLOAT, floatValues)
                        .addVectorField(
                                Constants.binaryVectorFieldName, DataType.VECTOR_BINARY, vectors);
        client.insert(insertParam);
        client.flush(collectionName);
        String tagNew = Utils.genUniqueStr("tagNew");
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tagNew);
        SearchParam searchParam =
                SearchParam.create(collectionName).setDsl(binaryDsl).setPartitionTags(queryTags);
        client.search(searchParam);
    }

    //    #3656
    @Test(
            dataProvider = "BinaryCollection",
            dataProviderClass = MainClass.class,
            expectedExceptions = ServerSideMilvusException.class)
    public void testSearchInvalidNProbeBinary(MilvusClient client, String collectionName) {
        int n_probe_new = 0;
        List<Long> intValues = new ArrayList<>(Constants.nb);
        List<Float> floatValues = new ArrayList<>(Constants.nb);
        List<ByteBuffer> vectors = Utils.genBinaryVectors(Constants.nb, Constants.dimension);
        for (int i = 0; i < Constants.nb; ++i) {
            intValues.add((long) i);
            floatValues.add((float) i);
        }
        InsertParam insertParam =
                InsertParam.create(collectionName)
                        .addField(Constants.intFieldName, DataType.INT64, intValues)
                        .addField(Constants.floatFieldName, DataType.FLOAT, floatValues)
                        .addVectorField(
                                Constants.binaryVectorFieldName, DataType.VECTOR_BINARY, vectors);
        client.insert(insertParam);
        client.flush(collectionName);
        Index index =
                Index.create(collectionName, Constants.binaryVectorFieldName)
                        .setIndexType(Constants.defaultBinaryIndexType)
                        .setMetricType(Constants.defaultBinaryMetricType)
                        .setParamsInJson(
                                new JsonBuilder().param("nlist", Constants.n_list).build());
        client.createIndex(index);
        String dsl =
                Utils.setBinarySearchParam(
                        Constants.defaultBinaryMetricType,
                        vectors.subList(0, Constants.nq),
                        top_k,
                        n_probe_new);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        client.search(searchParam);
    }

    @Test(
            dataProvider = "BinaryCollection",
            dataProviderClass = MainClass.class,
            expectedExceptions = ServerSideMilvusException.class)
    public void testSearchInvalidTopKBinary(MilvusClient client, String collectionName) {
        int top_k = -1;
        List<Long> intValues = new ArrayList<>(Constants.nb);
        List<Float> floatValues = new ArrayList<>(Constants.nb);
        List<ByteBuffer> vectors = Utils.genBinaryVectors(Constants.nb, Constants.dimension);
        for (int i = 0; i < Constants.nb; ++i) {
            intValues.add((long) i);
            floatValues.add((float) i);
        }
        InsertParam insertParam =
                InsertParam.create(collectionName)
                        .addField(Constants.intFieldName, DataType.INT64, intValues)
                        .addField(Constants.floatFieldName, DataType.FLOAT, floatValues)
                        .addVectorField(
                                Constants.binaryVectorFieldName, DataType.VECTOR_BINARY, vectors);
        client.insert(insertParam);
        client.flush(collectionName);
        String dsl =
                Utils.setBinarySearchParam(
                        Constants.defaultBinaryMetricType,
                        vectors.subList(0, Constants.nq),
                        top_k,
                        n_probe);
        SearchParam searchParam = SearchParam.create(collectionName).setDsl(dsl);
        client.search(searchParam);
    }
}
