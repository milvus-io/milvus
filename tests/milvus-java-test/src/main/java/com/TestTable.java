package com;


import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class TestTable {
    int index_file_size = 50;
    int dimension = 128;

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_table(MilvusClient client, String tableName){
        TableSchema tableSchema = new TableSchema.Builder(tableName, dimension)
                .withIndexFileSize(index_file_size)
                .withMetricType(MetricType.L2)
                .build();
        Response res = client.createTable(tableSchema);
        assert(res.ok());
        Assert.assertEquals(res.ok(), true);
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_table_disconnect(MilvusClient client, String tableName){
        TableSchema tableSchema = new TableSchema.Builder(tableName, dimension)
                .withIndexFileSize(index_file_size)
                .withMetricType(MetricType.L2)
                .build();
        Response res = client.createTable(tableSchema);
        assert(!res.ok());
    }

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_table_repeatably(MilvusClient client, String tableName){
        TableSchema tableSchema = new TableSchema.Builder(tableName, dimension)
                .withIndexFileSize(index_file_size)
                .withMetricType(MetricType.L2)
                .build();
        Response res = client.createTable(tableSchema);
        Assert.assertEquals(res.ok(), true);
        Response res_new = client.createTable(tableSchema);
        Assert.assertEquals(res_new.ok(), false);
    }

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void test_create_table_wrong_params(MilvusClient client, String tableName){
        Integer dimension = 0;
        TableSchema tableSchema = new TableSchema.Builder(tableName, dimension)
                .withIndexFileSize(index_file_size)
                .withMetricType(MetricType.L2)
                .build();
        Response res = client.createTable(tableSchema);
        System.out.println(res.toString());
        Assert.assertEquals(res.ok(), false);
    }

    @Test(dataProvider = "ConnectInstance", dataProviderClass = MainClass.class)
    public void test_show_tables(MilvusClient client, String tableName){
        Integer tableNum = 10;
        ShowTablesResponse res = null;
        for (int i = 0; i < tableNum; ++i) {
            String tableNameNew = tableName+"_"+Integer.toString(i);
            TableSchema tableSchema = new TableSchema.Builder(tableNameNew, dimension)
                    .withIndexFileSize(index_file_size)
                    .withMetricType(MetricType.L2)
                    .build();
            client.createTable(tableSchema);
            List<String> tableNames = client.showTables().getTableNames();
            Assert.assertTrue(tableNames.contains(tableNameNew));
        }
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_show_tables_without_connect(MilvusClient client, String tableName){
        ShowTablesResponse res = client.showTables();
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_drop_table(MilvusClient client, String tableName) throws InterruptedException {
        Response res = client.dropTable(tableName);
        assert(res.ok());
        Thread.currentThread().sleep(1000);
        List<String> tableNames = client.showTables().getTableNames();
        Assert.assertFalse(tableNames.contains(tableName));
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_drop_table_not_existed(MilvusClient client, String tableName) throws InterruptedException {
        Response res = client.dropTable(tableName+"_");
        assert(!res.ok());
        List<String> tableNames = client.showTables().getTableNames();
        Assert.assertTrue(tableNames.contains(tableName));
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_drop_table_without_connect(MilvusClient client, String tableName) throws InterruptedException {
        Response res = client.dropTable(tableName);
        assert(!res.ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_describe_table(MilvusClient client, String tableName) throws InterruptedException {
        DescribeTableResponse res = client.describeTable(tableName);
        assert(res.getResponse().ok());
        TableSchema tableSchema = res.getTableSchema().get();
        Assert.assertEquals(tableSchema.getDimension(), dimension);
        Assert.assertEquals(tableSchema.getTableName(), tableName);
        Assert.assertEquals(tableSchema.getIndexFileSize(), index_file_size);
        Assert.assertEquals(tableSchema.getMetricType().name(), tableName.substring(0,2));
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_describe_table_without_connect(MilvusClient client, String tableName) throws InterruptedException {
        DescribeTableResponse res = client.describeTable(tableName);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_has_table_not_existed(MilvusClient client, String tableName) throws InterruptedException {
        HasTableResponse res = client.hasTable(tableName+"_");
        assert(res.getResponse().ok());
        Assert.assertFalse(res.hasTable());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_has_table_without_connect(MilvusClient client, String tableName) throws InterruptedException {
        HasTableResponse res = client.hasTable(tableName);
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_has_table(MilvusClient client, String tableName) throws InterruptedException {
        HasTableResponse res = client.hasTable(tableName);
        assert(res.getResponse().ok());
        Assert.assertTrue(res.hasTable());
    }


}
