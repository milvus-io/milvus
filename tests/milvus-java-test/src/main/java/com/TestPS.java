package com;

import io.milvus.client.*;
import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestPS {
    private static int dimension = 128;
    private static String host = "192.168.1.101";
    private static String port = "19530";

    public static void setHost(String host) {
        TestPS.host = host;
    }

    public static void setPort(String port) {
        TestPS.port = port;
    }

    public static List<Float> normalize(List<Float> w2v){
        float squareSum = w2v.stream().map(x -> x * x).reduce((float) 0, Float::sum);
        final float norm = (float) Math.sqrt(squareSum);
        w2v = w2v.stream().map(x -> x / norm).collect(Collectors.toList());
        return w2v;
    }

    public static List<List<Float>> gen_vectors(int nb, boolean norm) {
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

    public static void main(String[] args) throws ConnectFailedException {
        int nq = 5;
        int nb = 1;
        int nprobe = 32;
        int top_k = 10;
        int loops = 100000;
//        int index_file_size = 1024;
        String tableName = "sift_1b_2048_128_l2";

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

        List<List<Float>> vectors = gen_vectors(nb, true);
        List<List<Float>> queryVectors = gen_vectors(nq, true);
        MilvusClient client = new MilvusGrpcClient();
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(host)
                .withPort(port)
                .build();
        client.connect(connectParam);
//        String tableName = RandomStringUtils.randomAlphabetic(10);
//        TableSchema tableSchema = new TableSchema.Builder(tableName, dimension)
//                .withIndexFileSize(index_file_size)
//                .withMetricType(MetricType.IP)
//                .build();
//        Response res = client.createTable(tableSchema);
        List<Long> vectorIds;
        vectorIds = Stream.iterate(0L, n -> n)
                .limit(nb)
                .collect(Collectors.toList());
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).withVectorIds(vectorIds).build();
        ForkJoinPool executor_search = new ForkJoinPool();
        for (int i = 0; i < loops; i++) {
            executor_search.execute(
                    () -> {
                        InsertResponse res_insert = client.insert(insertParam);
                        assert (res_insert.getResponse().ok());
                        System.out.println("In insert");
                        SearchParam searchParam = new SearchParam.Builder(tableName, queryVectors).withNProbe(nprobe).withTopK(top_k).build();
                        SearchResponse res_search = client.search(searchParam);
                        assert (res_search.getResponse().ok());
                    });
        }
        executor_search.awaitQuiescence(300, TimeUnit.SECONDS);
        executor_search.shutdown();
        GetTableRowCountResponse getTableRowCountResponse = client.getTableRowCount(tableName);
        System.out.println(getTableRowCountResponse.getTableRowCount());
    }
}
