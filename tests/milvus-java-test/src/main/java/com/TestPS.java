package com;

import io.milvus.client.*;
import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestPS {
    private static int dimension = 512;
    private static String host = "localhost";
    private static String port = "19530";

    public static void setHost(String host) {
        TestPS.host = host;
    }

    public static void setPort(String port) {
        TestPS.port = port;
    }



    public static void main(String[] args) throws ExecutionException {
        int nb = 10000;
        int nq = 1;
        int top_k = 2;
        int loops = 100000;
        int index_file_size = 1024;
        String collectionName = "random_1m_2048_512_ip_sq8";


        List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);


        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("h", "host", true, "milvus-server hostname/ip");
        options.addOption("p", "port", true, "milvus-server port");
        try {
            CommandLine cmd = parser.parse(options, args);
            String host = cmd.getOptionValue("host");
            if (host != null) {
                setHost(host);
            }
            String port = cmd.getOptionValue("port");
            if (port != null) {
                setPort(port);
            }
            System.out.println("Host: "+host+", Port: "+port);
        }
        catch(ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage() );
        }

        ConnectParam connectParam = new ConnectParam.Builder()
            .withHost(host)
            .withPort(Integer.parseInt(port))
            .build();
        MilvusClient client = new MilvusGrpcClient(connectParam);
        if(client.hasCollection(collectionName).hasCollection()) {
            client.dropCollection(collectionName);
        }
        CollectionMapping tableSchema = new CollectionMapping.Builder(collectionName, dimension)
            .withIndexFileSize(index_file_size)
            .withMetricType(MetricType.L2)
            .build();
        client.createCollection(tableSchema);
        List<Long> vectorIds;
        vectorIds = Stream.iterate(0L, n -> n)
            .limit(nb)
            .collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).withVectorIds(vectorIds).build();
        for (int i = 0; i < 100; ++i) {
            InsertResponse res_insert = client.insert(insertParam);
            assert (res_insert.getResponse().ok());
        }
        System.out.println(client.countEntities(collectionName).getCollectionEntityCount());

        ExecutorService executors = Executors.newFixedThreadPool(50);
        List<Future> resultList = new ArrayList<Future>();
        for (int i = 0; i < loops; i++) {
            List<List<Float>> queryVectors = Utils.genVectors(nq, dimension, true);
            Future future= executors.submit(
                () -> {
                    String params = "{\"nprobe\":1024}";
                    SearchParam searchParam = new SearchParam.Builder(collectionName)
                        .withFloatVectors(queryVectors)
                        .withParamsInJson(params)
                        .withTopK(top_k).build();
                    SearchResponse res_search = client.search(searchParam);
                    assert (res_search.getResponse().ok());
                });
            resultList.add(future);
        }
//        IntStream.range(0, loops).parallel().forEach(index -> {
//                        List<List<Float>> queryVectors = Utils.genVectors(nq, dimension, true);
//                        String params = "{\"nprobe\":1024}";
//                        SearchParam searchParam = new SearchParam.Builder(collectionName)
//                                .withFloatVectors(queryVectors)
//                                .withParamsInJson(params)
//                                .withTopK(top_k).build();
//                        SearchResponse res_search = client.search(searchParam);
//                        assert (res_search.getResponse().ok());
//                });
//        executor_search.awaitQuiescence(300, TimeUnit.SECONDS);
        executors.shutdown();
        CountEntitiesResponse getTableRowCountResponse = client.countEntities(collectionName);
        System.out.println(getTableRowCountResponse.getCollectionEntityCount());
        for (Future f: resultList) {
            System.out.println(f.get());
        }
//        int thread_num = 50;
//        ForkJoinPool executor = new ForkJoinPool();
//        for (int i = 0; i < thread_num; i++) {
//            executor.execute(
//                    () -> {
//                        String params = "{\"nprobe\":\"1024\"}";
//                        SearchParam searchParam = new SearchParam.Builder(collectionName)
//                                .withFloatVectors(queryVectors)
//                                .withParamsInJson(params)
//                                .withTopK(top_k).build();
//                        SearchResponse res_search = client.search(searchParam);
//                        assert (res_search.getResponse().ok());
//                    });
//        }
//        executor.awaitQuiescence(100, TimeUnit.SECONDS);
//        executor.shutdown();
//        CountEntitiesResponse getTableRowCountResponse = client.countEntities(collectionName);
//        System.out.println(getTableRowCountResponse.getCollectionEntityCount());
    }
}