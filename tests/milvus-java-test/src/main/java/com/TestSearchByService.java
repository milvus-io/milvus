package com;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.milvus.client.*;
import io.milvus.client.dsl.MilvusService;
import io.milvus.client.dsl.Query;
import io.milvus.client.dsl.Schema;
import io.milvus.client.exception.ServerSideMilvusException;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class TestSearchByService {
    int n_probe = Constants.n_probe;
    int top_k = Constants.topk;
    int nq = Constants.nq;
    int segmentRowLimit = Constants.segmentRowLimit;
    ConnectParam connectParam = Constants.connectParam;
    String collectionName = RandomStringUtils.randomAlphabetic(10);
    MilvusClient client = new MilvusGrpcClient(connectParam).withLogging();

    FloatSchema floatSchema = new FloatSchema();
    BinarySchema binarySchema = new BinarySchema();

    List<List<Float>> queryVectors = Constants.vectors.subList(0, nq);
    List<ByteBuffer> binaryQueryVectors = Constants.vectorsBinary.subList(0, nq);

    MilvusService service = new MilvusService(client, collectionName, floatSchema);
    MilvusService binaryService = new MilvusService(client, collectionName, binarySchema);

    @AfterMethod
    public void teardown() {
        if (service.hasCollection(collectionName)) {
            service.dropCollection();
        }
        if (binaryService.hasCollection(collectionName)) {
            binaryService.dropCollection();
        }
    }

    // create collection and insert entities, flush
    private List<Long> init_data(int nb) {
        Map<String, List> entities;
        if (nb == Constants.nb) {
            entities = Constants.defaultEntities;
        } else {
            entities = Utils.genDefaultEntities(nb, Utils.genVectors(nb, Constants.dimension, false));
        }
        service.createCollection(new JsonBuilder().param("segment_row_limit", segmentRowLimit).build());
        Assert.assertTrue(service.hasCollection(collectionName));
        List<Long> ids = service.insert(insertParam -> insertParam.with(floatSchema.int64Field, entities.get(Constants.intFieldName))
                .with(floatSchema.floatField, entities.get(Constants.floatFieldName))
                .with(floatSchema.floatVectorField, entities.get(Constants.floatVectorFieldName)));
        service.flush();
        Assert.assertEquals(nb, service.countEntities());
        return ids;
    }

    // create collection and insert binary entities, flush
    private List<Long> init_binary_data(List<ByteBuffer> binaryVectors) {
        int nb = binaryVectors.size();
        List<Long> intValues = new ArrayList<>(nb);
        List<Float> floatValues = new ArrayList<>(nb);
        for (int i = 0; i < nb; ++i) {
            intValues.add((long) i);
            floatValues.add((float) i);
        }
//        List<ByteBuffer> binaryVectors = Utils.genBinaryVectors(nb, Constants.dimension);
        binaryService.createCollection(new JsonBuilder().param("segment_row_limit", segmentRowLimit).build());
        Assert.assertTrue(binaryService.hasCollection(collectionName));
        List<Long> ids = binaryService.insert(insertParam -> insertParam.with(binarySchema.int64Field, intValues)
                .with(binarySchema.floatField, floatValues)
                .with(binarySchema.binaryVectorField, binaryVectors));
        binaryService.flush();
        Assert.assertEquals(nb, binaryService.countEntities());
        return ids;
    }

    // gen search query
    private Query genDefaultQuery(MetricType metricType, List<List<Float>> queryVectors, int topk, int nprobe) {
        Query query =
                Query.bool(
                        Query.must(
                                floatSchema.floatVectorField.query(queryVectors)
                                        .metricType(metricType)
                                        .top(topk)
                                        .param("nprobe", nprobe)));
        return query;
    }

    // gen default binary search query
    private Query genDefaultBinaryQuery(MetricType metricType, List<ByteBuffer> binaryQueryVectors, int topk, int nprobe) {
        Query query =
                Query.bool(
                        Query.must(
                                binarySchema.binaryVectorField.query(binaryQueryVectors)
                                        .metricType(metricType)
                                        .top(topk)
                                        .param("nprobe", nprobe)));
        return query;
    }

    @Test(expectedExceptions = ServerSideMilvusException.class)
    public void testSearchCollectionNotExisted() {
        Query query = genDefaultQuery(MetricType.L2, queryVectors, top_k, n_probe);
        SearchParam searchParam = service.buildSearchParam(query).setParamsInJson("{\"fields\": [\"int64\", \"float_vector\"]}");
        service.search(searchParam);
    }

    @Test()
    public void testSearchCollectionEmpty() {
        service.createCollection(new JsonBuilder().param("segment_row_limit", segmentRowLimit).build());
        Assert.assertTrue(service.hasCollection(collectionName));
        Query query = genDefaultQuery(MetricType.L2, queryVectors, top_k, n_probe);
        SearchParam searchParam = service.buildSearchParam(query);
        SearchResult resSearch = service.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), 0);
    }

    @Test
    public void testSearchCollection() {
        List<Long> ids = init_data(Constants.nb);
        Query query = genDefaultQuery(MetricType.L2, queryVectors, top_k, n_probe);
        SearchParam searchParam = service.buildSearchParam(query).setParamsInJson("{\"fields\": [\"int64\"]}");
        SearchResult resSearch = service.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), nq);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), nq);
        Assert.assertEquals(resSearch.getTopK(), top_k);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).get(0), ids.get(0));
        Assert.assertEquals(resSearch.getFieldsMap().get(0).size(), top_k);
    }

    @Test
    public void testSearchDistance() {
        List<Long> ids = init_data(Constants.nb);
        Query query = genDefaultQuery(MetricType.L2, queryVectors, top_k, n_probe);
        SearchParam searchParam = service.buildSearchParam(query).setParamsInJson("{\"fields\": [\"int64\"]}");
        SearchResult resSearch = service.search(searchParam);
        List<List<Float>> distances = resSearch.getResultDistancesList();
        distances.stream().forEach(distance -> {
                    Assert.assertEquals(distance.get(0), 0.0, Constants.epsilon);
                }
        );
        Assert.assertEquals(resSearch.getQueryResultsList().size(), nq);
        Assert.assertEquals(resSearch.getQueryResultsList().get(0).size(), top_k);
    }

    @Test
    public void testSearchDistanceIP() {
        List<Long> ids = init_data(Constants.nb);
        Query query = genDefaultQuery(MetricType.IP, queryVectors, top_k, n_probe);
        SearchParam searchParam = service.buildSearchParam(query).setParamsInJson("{\"fields\": [\"int64\"]}");
        SearchResult resSearch = service.search(searchParam);
        List<List<Float>> distances = resSearch.getResultDistancesList();
        distances.stream().forEach(distance -> {
                    Assert.assertEquals(distance.get(0), 1.0, Constants.epsilon);
                }
        );
    }

    @Test
    public void testSearchPartition() {
        String tag = "tag";
        List<String> queryTags = new ArrayList<>();
        queryTags.add(tag);
        List<Long> ids = init_data(Constants.nb);
        service.createPartition(tag);
        Query query = genDefaultQuery(MetricType.L2, queryVectors, top_k, n_probe);
        SearchParam searchParam = service.buildSearchParam(query).setPartitionTags(queryTags).setParamsInJson("{\"fields\": [\"int64\"]}");
        SearchResult resSearch = service.search(searchParam);
        Assert.assertEquals(resSearch.getTopK(), 0);
        Assert.assertEquals(resSearch.getQueryResultsList().size(), nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), 0);
    }

    @Test(expectedExceptions = ServerSideMilvusException.class)
    public void testSearchInvalidNProbe() {
        int n_probe_new = -1;
        List<Long> ids = init_data(Constants.nb);
        service.createIndex(
                floatSchema.floatVectorField,
                IndexType.IVF_SQ8,
                MetricType.L2,
                new JsonBuilder().param("nlist", Constants.n_list).build());
        Query query = genDefaultQuery(MetricType.L2, queryVectors, top_k, n_probe_new);
        SearchParam searchParam = service.buildSearchParam(query);
        service.search(searchParam);
    }

    @Test()
    public void testSearchCountLessThanTopK() {
        int top_k_new = 100;
        int nb = 50;
        List<Long> ids = init_data(nb);
        Query query = genDefaultQuery(MetricType.L2, queryVectors, top_k_new, n_probe);
        SearchParam searchParam = service.buildSearchParam(query);
        SearchResult resSearch = service.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), nq);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), nb);
    }

    @Test(expectedExceptions = ServerSideMilvusException.class)
    public void testSearchInvalidTopK() {
        int top_k = -1;
        List<Long> ids = init_data(Constants.nb);
        Query query = genDefaultQuery(MetricType.L2, queryVectors, top_k, n_probe);
        SearchParam searchParam = service.buildSearchParam(query);
        service.search(searchParam);
    }

    @Test()
    public void testSearchMultiMust() {
        Query query =
                Query.bool(
                        Query.must(
                                Query.must(
                                        floatSchema.floatVectorField.query(queryVectors)
                                                .metricType(MetricType.L2)
                                                .top(top_k)
                                                .param("nprobe", n_probe)
                                )
                        ));
        List<Long> ids = init_data(Constants.nb);
        SearchParam searchParam = service.buildSearchParam(query);
        SearchResult resSearch = service.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), Constants.nq);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), Constants.nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), Constants.topk);
    }

    //    TODO
    @Test(expectedExceptions = ServerSideMilvusException.class)
    public void testSearchMultiVectors() {
        Query query =
                Query.bool(
                        Query.must(
                                Query.must(
                                        floatSchema.floatVectorField.query(queryVectors)
                                                .metricType(MetricType.L2)
                                                .top(top_k)
                                                .param("nprobe", n_probe),
                                        floatSchema.floatVectorField.query(queryVectors)
                                                .metricType(MetricType.L2)
                                                .top(top_k)
                                                .param("nprobe", n_probe)
                                )
                        ));
        List<Long> ids = init_data(Constants.nb);
        SearchParam searchParam = service.buildSearchParam(query);
        service.search(searchParam);
    }

    @Test
    public void testSearchVectorNotExisted() {
        Query query =
                Query.bool(
                        Query.must(
                                Query.must(
                                        floatSchema.floatVectorField.query(new ArrayList<>())
                                                .metricType(MetricType.L2)
                                                .top(top_k)
                                                .param("nprobe", n_probe)
                                )
                        ));
        List<Long> ids = init_data(Constants.nb);
        SearchParam searchParam = service.buildSearchParam(query);
        SearchResult resSearch = service.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), 0);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), 0);
    }

    @Test(expectedExceptions = ServerSideMilvusException.class)
    public void testSearchVectorDifferentDim() {
        List<Long> ids = init_data(Constants.nb);
        List<List<Float>> queryVectors = Utils.genVectors(nq, 64, false);
        Query query = genDefaultQuery(MetricType.L2, queryVectors, top_k, n_probe);
        SearchParam searchParam = service.buildSearchParam(query);
        service.search(searchParam);
    }

    @Test
    public void testAsyncSearch() {
        List<Long> ids = init_data(Constants.nb);
        Query query = genDefaultQuery(MetricType.L2, queryVectors, top_k, n_probe);
        SearchParam searchParam = service.buildSearchParam(query);
        ListenableFuture<SearchResult> searchResFuture = service.searchAsync(searchParam);
        Futures.addCallback(
                searchResFuture,
                new FutureCallback<SearchResult>() {
                    @Override
                    public void onSuccess(SearchResult searchResult) {
                        Assert.assertNotNull(searchResult);
                        Assert.assertEquals(searchResult.getResultIdsList().size(), Constants.nq);
                        Assert.assertEquals(searchResult.getResultDistancesList().size(), Constants.nq);
                        Assert.assertEquals(searchResult.getResultIdsList().get(0).size(), Constants.topk);
                        Assert.assertEquals(searchResult.getFieldsMap().get(0).size(), top_k);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        System.out.println(t.getMessage());
                    }
                },
                MoreExecutors.directExecutor());
    }

    @Test
    public void testSearchVectorsWithTerm() {
        List<Long> ids = init_data(Constants.nb);
        Query query =
                Query.bool(
                        Query.must(
                                Query.must(
                                        floatSchema.int64Field.in(0L, 1L),
                                        floatSchema.floatVectorField.query(queryVectors)
                                                .metricType(MetricType.L2)
                                                .top(top_k)
                                                .param("nprobe", n_probe)
                                )
                        ));
        SearchParam searchParam = service.buildSearchParam(query);
        SearchResult resSearch = service.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), Constants.nq);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), Constants.nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), 2);
    }

    @Test
    public void testSearchEntitiesLessThanTopK() {
        List<Long> ids = init_data(Constants.nb);
        Query query =
                Query.bool(
                        Query.must(
                                Query.must(
                                        floatSchema.floatField.gte(0.0f).lt(1.0f),
                                        floatSchema.floatVectorField.query(queryVectors)
                                                .metricType(MetricType.L2)
                                                .top(top_k)
                                                .param("nprobe", n_probe)
                                )
                        ));
        SearchParam searchParam = service.buildSearchParam(query);
        SearchResult resSearch = service.search(searchParam);
        Assert.assertEquals(resSearch.getQueryResultsList().get(0).size(), 1);
    }

    // ------------------------binary collection test--------------------

    @Test(expectedExceptions = ServerSideMilvusException.class)
    public void testSearchCollectionNotExistedBinary() {
        Query query = genDefaultBinaryQuery(MetricType.L2, binaryQueryVectors, top_k, n_probe);
        SearchParam searchParam = service.buildSearchParam(query).setParamsInJson("{\"fields\": [\"int64\", \"binary_vector\"]}");
        service.search(searchParam);
    }

    @Test
    public void testSearchCollectionBinary() {
        List<Long> ids = init_binary_data(Constants.vectorsBinary);
        Query query = genDefaultBinaryQuery(MetricType.JACCARD, binaryQueryVectors, top_k, n_probe);
        SearchParam searchParam = binaryService.buildSearchParam(query);
        SearchResult resSearch = binaryService.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), nq);
        Assert.assertEquals(resSearch.getResultDistancesList().size(), nq);
        Assert.assertEquals(resSearch.getTopK(), top_k);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).get(0), ids.get(0));
        Assert.assertEquals(resSearch.getFieldsMap().get(0).size(), top_k);
    }

    @Test
    public void testSearchIVFLATBinary() {
        List<Long> ids = init_binary_data(Constants.vectorsBinary);
        binaryService.createIndex(
                binarySchema.binaryVectorField,
                IndexType.BIN_FLAT,
                MetricType.JACCARD,
                new JsonBuilder().param("nlist", Constants.n_list).build());
        Query query = genDefaultBinaryQuery(MetricType.JACCARD, binaryQueryVectors, top_k, n_probe);
        SearchParam searchParam = binaryService.buildSearchParam(query);
        SearchResult resSearch = binaryService.search(searchParam);
        Assert.assertEquals(resSearch.getResultIdsList().size(), nq);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).size(), top_k);
        Assert.assertEquals(resSearch.getResultIdsList().get(0).get(0), ids.get(0));
    }

    @Test
    public void testSearchPartitionNotExistedBinary() {
        List<Long> ids = init_binary_data(Constants.vectorsBinary);
        String tag = Utils.genUniqueStr("tag");
        List<String> tags = new ArrayList<String>();
        tags.add(tag);
//        binaryService.createPartition(tag);
        Query query = genDefaultBinaryQuery(MetricType.JACCARD, binaryQueryVectors, top_k, n_probe);
        SearchParam searchParam = binaryService.buildSearchParam(query).setPartitionTags(tags);
        SearchResult resSearch = binaryService.search(searchParam);
        Assert.assertEquals(resSearch.getTopK(), 0);
    }

    @Test(expectedExceptions = ServerSideMilvusException.class)
    public void testSearchInvalidNProbeBinary() {
        List<Long> ids = init_binary_data(Constants.vectorsBinary);
        int n_probe_new = 0;
        binaryService.createIndex(
                binarySchema.binaryVectorField,
                IndexType.BIN_IVF_FLAT,
                MetricType.JACCARD,
                new JsonBuilder().param("nlist", Constants.n_list).build());
        Query query = genDefaultBinaryQuery(MetricType.JACCARD, Constants.vectorsBinary, top_k, n_probe_new);
        SearchParam searchParam = binaryService.buildSearchParam(query);
        binaryService.search(searchParam);
    }

    @Test(expectedExceptions = ServerSideMilvusException.class)
    public void testSearchInvalidTopKBinary() {
        List<Long> ids = init_binary_data(Constants.vectorsBinary);
        int top_k = -1;
        Query query = genDefaultBinaryQuery(MetricType.JACCARD, Constants.vectorsBinary, top_k, n_probe);
        SearchParam searchParam = binaryService.buildSearchParam(query);
        binaryService.search(searchParam);
    }

    //    float vector schema
    private class FloatSchema extends Schema {
        Int64Field int64Field = new Int64Field("int64");
        FloatField floatField = new FloatField("float");
        FloatVectorField floatVectorField = new FloatVectorField("float_vector", Constants.dimension);
    }

    //  binary vector schema
    private class BinarySchema extends Schema {
        Int64Field int64Field = new Int64Field("int64");
        FloatField floatField = new FloatField("float");
        BinaryVectorField binaryVectorField = new BinaryVectorField("binary_vector", Constants.dimension);
    }
}

