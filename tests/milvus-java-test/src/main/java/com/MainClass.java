package com;

import io.milvus.client.*;
import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.SkipException;
import org.testng.TestNG;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.AfterTest;
import org.testng.annotations.DataProvider;
import org.testng.xml.XmlClass;
import org.testng.xml.XmlSuite;
import org.testng.xml.XmlTest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MainClass {
    private static String HOST = "127.0.0.1";
    private static int PORT = 19530;
    private int segmentRowCount = 5000;
    public int dimension = 128;
    private static ConnectParam CONNECT_PARAM = new ConnectParam.Builder()
            .withHost(HOST)
            .withPort(PORT)
            .build();

    public static void setHost(String host) {
        MainClass.HOST = host;
    }

    public static void setPort(int port) {
        MainClass.PORT = port;
    }

    @DataProvider(name="DefaultConnectArgs")
    public static Object[][] defaultConnectArgs(){
        return new Object[][]{{HOST, PORT}};
    }

    @DataProvider(name="ConnectInstance")
    public Object[][] connectInstance() throws ConnectFailedException {
        MilvusClient client = new MilvusGrpcClient();
        ConnectParam connectParam = new ConnectParam.Builder()
                .withHost(HOST)
                .withPort(PORT)
                .build();
        client.connect(connectParam);
        String collectionName = RandomStringUtils.randomAlphabetic(10);
        return new Object[][]{{client, collectionName}};
    }

    @DataProvider(name="DisConnectInstance")
    public Object[][] disConnectInstance() throws ConnectFailedException {
        // Generate connection instance
        MilvusClient client = new MilvusGrpcClient();
        client.connect(CONNECT_PARAM);
        try {
            client.disconnect();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String collectionName = RandomStringUtils.randomAlphabetic(10);
        return new Object[][]{{client, collectionName}};
    }

    private Object[][] genCollection(boolean isBinary, boolean autoId) throws ConnectFailedException {
        Object[][] collection;
        String collectionName = Utils.genUniqueStr("collection");
        List<Map<String, Object>> defaultFields = Utils.genDefaultFields(dimension,isBinary);
        String jsonParams = String.format("{\"segment_row_count\": %s, \"auto_id\": %s}",segmentRowCount, autoId);
        // Generate connection instance
        MilvusClient client = new MilvusGrpcClient();
        client.connect(CONNECT_PARAM);
        CollectionMapping cm = new CollectionMapping.Builder(collectionName)
                .withFields(defaultFields)
                .withParamsInJson(jsonParams)
                .build();
        Response res = client.createCollection(cm);
        if (!res.ok()) {
            System.out.println(res.getMessage());
            throw new SkipException("Collection created failed");
        }
        collection = new Object[][]{{client, collectionName}};
        return collection;
    }
    @DataProvider(name="Collection")
    public Object[][] provideCollection() throws ConnectFailedException, InterruptedException {
        Object[][] collection = genCollection(false,true);
        return collection;
//            List<String> tableNames = client.listCollections().getCollectionNames();
//            for (int j = 0; j < tableNames.size(); ++j
//                 ) {
//                client.dropCollection(tableNames.get(j));
//            }
//            Thread.currentThread().sleep(2000);
    }
    @DataProvider(name="IdCollection")
    public Object[][] provideIdCollection() throws ConnectFailedException, InterruptedException {
        Object[][] idCollection = genCollection(false,false);
        return idCollection;
    }

    @DataProvider(name="BinaryCollection")
    public Object[][] provideBinaryCollection() throws ConnectFailedException, InterruptedException {
        Object[][] binaryCollection = genCollection(true,true);
        return binaryCollection;
    }

    @DataProvider(name="BinaryIdCollection")
    public Object[][] provideBinaryIdCollection() throws ConnectFailedException, InterruptedException {
        Object[][] binaryIdCollection = genCollection(true,false);
        return binaryIdCollection;
    }

    @AfterSuite
    public void dropCollection(){
//        MilvusClient client = new MilvusGrpcClient();
//        List<String> collectionNames = client.listCollections().getCollectionNames();
//        collectionNames.forEach(client::dropCollection);
        System.out.println("after suite");
    }
    @AfterMethod
    public void after(){
        System.out.println("after method");
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

//        classes.add(new XmlClass("com.TestPing"));
//        classes.add(new XmlClass("com.TestAddVectors"));
        classes.add(new XmlClass("com.TestConnect"));
//        classes.add(new XmlClass("com.TestDeleteVectors"));
//        classes.add(new XmlClass("com.TestIndex"));
//        classes.add(new XmlClass("com.TestCompact"));
//        classes.add(new XmlClass("com.TestSearchVectors"));
        classes.add(new XmlClass("com.TestCollection"));
//        classes.add(new XmlClass("com.TestCollectionCount"));
//        classes.add(new XmlClass("com.TestFlush"));
//        classes.add(new XmlClass("com.TestPartition"));
//        classes.add(new XmlClass("com.TestGetVectorByID"));
//        classes.add(new XmlClass("com.TestCollectionInfo"));
//        classes.add(new XmlClass("com.TestSearchByIds"));

        test.setXmlClasses(classes) ;

        List<XmlSuite> suites = new ArrayList<XmlSuite>();
        suites.add(suite);
        TestNG tng = new TestNG();
        tng.setXmlSuites(suites);
        tng.run();

    }

}
