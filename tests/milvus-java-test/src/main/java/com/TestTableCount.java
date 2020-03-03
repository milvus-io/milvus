package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestTableCount {
    int index_file_size = 50;
    int dimension = 128;

    public List<List<Float>> gen_vectors(Integer nb) {
        List<List<Float>> xb = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < nb; ++i) {
            ArrayList<Float> vector = new ArrayList<>();
            for (int j = 0; j < dimension; j++) {
                vector.add(random.nextFloat());
            }
            xb.add(vector);
        }
        return xb;
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_table_count_no_vectors(MilvusClient client, String tableName) {
        Assert.assertEquals(client.getTableRowCount(tableName).getTableRowCount(), 0);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_table_count_table_not_existed(MilvusClient client, String tableName) {
        GetTableRowCountResponse res = client.getTableRowCount(tableName+"_");
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_table_count_without_connect(MilvusClient client, String tableName) {
        GetTableRowCountResponse res = client.getTableRowCount(tableName+"_");
        assert(!res.getResponse().ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_table_count(MilvusClient client, String tableName) throws InterruptedException {
        int nb = 10000;
        List<List<Float>> vectors = gen_vectors(nb);
        // Add vectors
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();;
        client.insert(insertParam);
        Thread.currentThread().sleep(2000);
        Assert.assertEquals(client.getTableRowCount(tableName).getTableRowCount(), nb);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_table_count_multi_tables(MilvusClient client, String tableName) throws InterruptedException {
        int nb = 10000;
        List<List<Float>> vectors = gen_vectors(nb);
        Integer tableNum = 10;
        GetTableRowCountResponse res = null;
        for (int i = 0; i < tableNum; ++i) {
            String tableNameNew = tableName + "_" + Integer.toString(i);
            TableSchema tableSchema = new TableSchema.Builder(tableNameNew, dimension)
                    .withIndexFileSize(index_file_size)
                    .withMetricType(MetricType.L2)
                    .build();
            client.createTable(tableSchema);
            // Add vectors
            InsertParam insertParam = new InsertParam.Builder(tableNameNew, vectors).build();
            client.insert(insertParam);
        }
        Thread.currentThread().sleep(1000);
        for (int i = 0; i < tableNum; ++i) {
            String tableNameNew = tableName + "_" + Integer.toString(i);
            res = client.getTableRowCount(tableNameNew);
            Assert.assertEquals(res.getTableRowCount(), nb);
        }
    }

}


