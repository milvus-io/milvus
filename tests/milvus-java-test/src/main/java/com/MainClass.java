package com;

import io.milvus.client.*;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.SkipException;
import org.testng.TestNG;
import org.testng.annotations.DataProvider;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

import java.util.ArrayList;
import java.util.List;

public class MainClass {
    private static String host = "127.0.0.1";
    private static int port = 19530;
    private int index_file_size = 50;
    public int dimension = 128;

    public static void setHost(String host) {
        MainClass.host = host;
    }

    public static void setPort(int port) {
        MainClass.port = port;
    }

    @DataProvider(name="DefaultConnectArgs")
    public static Object[][] defaultConnectArgs(){
        return new Object[][]{{host, port}};
    }

    @DataProvider(name="ConnectInstance")
    public Object[][] connectInstance() throws ConnectFailedException {
        MilvusClient client = new MilvusGrpcClient();
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(host)
                .withPort(port)
                .build();
        client.connect(connectParam);
        String collectionName = RandomStringUtils.randomAlphabetic(10);
        return new Object[][]{{client, collectionName}};
    }

    @DataProvider(name="DisConnectInstance")
    public Object[][] disConnectInstance() throws ConnectFailedException {
        // Generate connection instance
        MilvusClient client = new MilvusGrpcClient();
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(host)
                .withPort(port)
                .build();
        client.connect(connectParam);
        try {
            client.disconnect();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String collectionName = RandomStringUtils.randomAlphabetic(10);
        return new Object[][]{{client, collectionName}};
    }

    @DataProvider(name="Collection")
    public Object[][] provideCollection() throws ConnectFailedException, InterruptedException {
        Object[][] collections = new Object[2][2];
        MetricType[] metricTypes = { MetricType.L2, MetricType.IP };
        for (int i = 0; i < metricTypes.length; ++i) {
            String collectionName = metricTypes[i].toString()+"_"+RandomStringUtils.randomAlphabetic(10);
            // Generate connection instance
            MilvusClient client = new MilvusGrpcClient();
            ConnectParam connectParam = new ConnectParam.Builder()
                    .withHost(host)
                    .withPort(port)
                    .build();
            client.connect(connectParam);
//            List<String> tableNames = client.listCollections().getCollectionNames();
//            for (int j = 0; j < tableNames.size(); ++j
//                 ) {
//                client.dropCollection(tableNames.get(j));
//            }
//            Thread.currentThread().sleep(2000);
            CollectionMapping cm = new CollectionMapping.Builder(collectionName, dimension)
                    .withIndexFileSize(index_file_size)
                    .withMetricType(metricTypes[i])
                    .build();
            Response res = client.createCollection(cm);
            if (!res.ok()) {
                System.out.println(res.getMessage());
                throw new SkipException("Collection created failed");
            }
            collections[i] = new Object[]{client, collectionName};
        }
        return collections;
    }

    @DataProvider(name="BinaryCollection")
    public Object[][] provideBinaryCollection() throws ConnectFailedException, InterruptedException {
        Object[][] collections = new Object[3][2];
        MetricType[] metricTypes = { MetricType.JACCARD, MetricType.HAMMING, MetricType.TANIMOTO };
        for (int i = 0; i < metricTypes.length; ++i) {
            String collectionName = metricTypes[i].toString()+"_"+RandomStringUtils.randomAlphabetic(10);
            // Generate connection instance
            MilvusClient client = new MilvusGrpcClient();
            ConnectParam connectParam = new ConnectParam.Builder()
                    .withHost(host)
                    .withPort(port)
                    .build();
            client.connect(connectParam);
//            List<String> tableNames = client.listCollections().getCollectionNames();
//            for (int j = 0; j < tableNames.size(); ++j
//            ) {
//                client.dropCollection(tableNames.get(j));
//            }
//            Thread.currentThread().sleep(2000);
            CollectionMapping cm = new CollectionMapping.Builder(collectionName, dimension)
                    .withIndexFileSize(index_file_size)
                    .withMetricType(metricTypes[i])
                    .build();
            Response res = client.createCollection(cm);
            if (!res.ok()) {
                System.out.println(res.getMessage());
                throw new SkipException("Collection created failed");
            }
            collections[i] = new Object[]{client, collectionName};
        }
        return collections;
    }


    public static void main(String[] args) {
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
                setPort(Integer.parseInt(port));
            }
            System.out.println("Host: "+host+", Port: "+port);
        }
        catch(ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage() );
        }

        XmlSuite suite = new XmlSuite();
        suite.setName("TmpSuite");

        XmlTest test = new XmlTest(suite);
        test.setName("TmpTest");
        List<XmlClass> classes = new ArrayList<XmlClass>();

        classes.add(new XmlClass("com.TestPing"));
        classes.add(new XmlClass("com.TestAddVectors"));
        classes.add(new XmlClass("com.TestConnect"));
        classes.add(new XmlClass("com.TestDeleteVectors"));
        classes.add(new XmlClass("com.TestIndex"));
        classes.add(new XmlClass("com.TestCompact"));
        classes.add(new XmlClass("com.TestSearchVectors"));
        classes.add(new XmlClass("com.TestCollection"));
        classes.add(new XmlClass("com.TestCollectionCount"));
        classes.add(new XmlClass("com.TestFlush"));
        classes.add(new XmlClass("com.TestPartition"));
        classes.add(new XmlClass("com.TestGetVectorByID"));
        classes.add(new XmlClass("com.TestCollectionInfo"));
//        classes.add(new XmlClass("com.TestSearchByIds"));

        test.setXmlClasses(classes) ;

        List<XmlSuite> suites = new ArrayList<XmlSuite>();
        suites.add(suite);
        TestNG tng = new TestNG();
        tng.setXmlSuites(suites);
        tng.run();

    }

}
