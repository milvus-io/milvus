package com;

import io.milvus.client.*;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.TestNG;
import org.testng.annotations.DataProvider;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

import java.util.ArrayList;
import java.util.List;

public class MainClass {
    private static String HOST = "127.0.0.1";
    //    private static String HOST = "192.168.1.238";
    private static int PORT = 19530;
    public static final ConnectParam CONNECT_PARAM =
            new ConnectParam.Builder().withHost(HOST).withPort(PORT).build();
    private static MilvusClient client;
    private final int segmentRowCount = 5000;

    public static String getHost() {
        return MainClass.HOST;
    }

    public static void setHost(String host) {
        MainClass.HOST = host;
    }

    public static int getPort() {
        return MainClass.PORT;
    }

    public static void setPort(int port) {
        MainClass.PORT = port;
    }

    @DataProvider(name = "DefaultConnectArgs")
    public static Object[][] defaultConnectArgs() {
        return new Object[][]{{HOST, PORT}};
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
            System.out.println("Host: " + host + ", Port: " + port);
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
        }

        XmlSuite suite = new XmlSuite();
        suite.setName("TmpSuite");

        XmlTest test = new XmlTest(suite);
        test.setName("TmpTest");
        List<XmlClass> classes = new ArrayList<XmlClass>();

        //        classes.add(new XmlClass("com.TestPing"));
        //        classes.add(new XmlClass("com.TestInsertEntities"));
        //        classes.add(new XmlClass("com.TestConnect"));
        //        classes.add(new XmlClass("com.TestDeleteEntities"));
        //        classes.add(new XmlClass("com.TestIndex"));
        //        classes.add(new XmlClass("com.TestCompact"));
        //        classes.add(new XmlClass("com.TestSearchEntities"));
        //        classes.add(new XmlClass("com.TestCollection"));
        //        classes.add(new XmlClass("com.TestCollectionCount"));
        //        classes.add(new XmlClass("com.TestFlush"));
        //        classes.add(new XmlClass("com.TestPartition"));
        //        classes.add(new XmlClass("com.TestGetEntityByID"));
        //        classes.add(new XmlClass("com.TestCollectionInfo"));
        //        classes.add(new XmlClass("com.TestSearchByIds"));
        classes.add(new XmlClass("com.TestBeforeAndAfter"));

        test.setXmlClasses(classes);

        List<XmlSuite> suites = new ArrayList<XmlSuite>();
        suites.add(suite);
        TestNG tng = new TestNG();
        tng.setXmlSuites(suites);
        tng.run();
    }

    @DataProvider(name = "ConnectInstance")
    public Object[][] connectInstance() throws Exception {
        ConnectParam connectParam = new ConnectParam.Builder().withHost(HOST).withPort(PORT).build();
        client = new MilvusGrpcClient(connectParam).withLogging();
        String collectionName = RandomStringUtils.randomAlphabetic(10);
        return new Object[][]{{client, collectionName}};
    }

    @DataProvider(name = "DisConnectInstance")
    public Object[][] disConnectInstance() {
        // Generate connection instance
        client = new MilvusGrpcClient(CONNECT_PARAM).withLogging();
        client.close();
        String collectionName = RandomStringUtils.randomAlphabetic(10);
        return new Object[][]{{client, collectionName}};
    }

    private Object[][] genCollection(boolean isBinary, boolean autoId) throws Exception {
        Object[][] collection;
        String collectionName = Utils.genUniqueStr("collection");
        // Generate connection instance
        client = new MilvusGrpcClient(CONNECT_PARAM).withLogging();
        CollectionMapping cm =
                CollectionMapping.create(collectionName)
                        .addField(Constants.intFieldName, DataType.INT64)
                        .addField(Constants.floatFieldName, DataType.FLOAT)
                        .setParamsInJson(
                                new JsonBuilder()
                                        .param("segment_row_limit", segmentRowCount)
                                        .param("auto_id", autoId)
                                        .build());
        if (isBinary) {
            cm.addVectorField("binary_vector", DataType.VECTOR_BINARY, Constants.dimension);
        } else {
            cm.addVectorField("float_vector", DataType.VECTOR_FLOAT, Constants.dimension);
        }
        client.createCollection(cm);
        collection = new Object[][]{{client, collectionName}};
        return collection;
    }

    @DataProvider(name = "Collection")
    public Object[][] provideCollection() throws Exception {
        Object[][] collection = genCollection(false, true);
        return collection;
    }

    @DataProvider(name = "IdCollection")
    public Object[][] provideIdCollection() throws Exception {
        Object[][] idCollection = genCollection(false, false);
        return idCollection;
    }

    @DataProvider(name = "BinaryCollection")
    public Object[][] provideBinaryCollection() throws Exception {
        Object[][] binaryCollection = genCollection(true, true);
        return binaryCollection;
    }

    @DataProvider(name = "BinaryIdCollection")
    public Object[][] provideBinaryIdCollection() throws Exception {
        Object[][] binaryIdCollection = genCollection(true, false);
        return binaryIdCollection;
    }
}
