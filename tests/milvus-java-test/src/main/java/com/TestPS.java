package com;

import io.milvus.client.*;
import org.apache.commons.cli.*;

import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestPS {
    private static int dimension = 128;
    private static String host = "127.0.0.1";
    private static String port = "19530";

    public static void setHost(String host) {
        TestPS.host = host;
    }

    public static void setPort(String port) {
        TestPS.port = port;
    }



    public static void main(String[] args) throws ConnectFailedException {
        int nb = 10000;
        int nq = 5;
        int nprobe = 32;
        int top_k = 10;
        int loops = 100000;
//        int index_file_size = 1024;
        String collectionName = "sift_1b_2048_128_l2";


        List<List<Float>> vectors = Utils.genVectors(nb, dimension, true);
        List<List<Float>> queryVectors = vectors.subList(0, nq);

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

        MilvusClient client = new MilvusGrpcClient();
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(host)
                .withPort(Integer.parseInt(port))
                .build();
        client.connect(connectParam);
//        String collectionName = RandomStringUtils.randomAlphabetic(10);
//        TableSchema tableSchema = new TableSchema.Builder(collectionName, dimension)
//                .withIndexFileSize(index_file_size)
//                .withMetricType(MetricType.IP)
//                .build();
//        Response res = client.createTable(tableSchema);
        List<Long> vectorIds;
        vectorIds = Stream.iterate(0L, n -> n)
                .limit(nb)
                .collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(collectionName).withFloatVectors(vectors).withVectorIds(vectorIds).build();
        ForkJoinPool executor_search = new ForkJoinPool();
        for (int i = 0; i < loops; i++) {
            executor_search.execute(
                    () -> {
                        InsertResponse res_insert = client.insert(insertParam);
                        assert (res_insert.getResponse().ok());
                        System.out.println("In insert");
                        SearchParam searchParam = new SearchParam.Builder(collectionName).withFloatVectors(queryVectors).withTopK(top_k).build();
                        SearchResponse res_search = client.search(searchParam);
                        assert (res_search.getResponse().ok());
                    });
        }
        executor_search.awaitQuiescence(300, TimeUnit.SECONDS);
        executor_search.shutdown();
        CountEntitiesResponse getTableRowCountResponse = client.countEntities(collectionName);
        System.out.println(getTableRowCountResponse.getCollectionEntityCount());
    }
}
