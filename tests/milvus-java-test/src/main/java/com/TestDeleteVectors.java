package com;

import io.milvus.client.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.*;

public class TestDeleteVectors {
    int index_file_size = 50;
    int dimension = 128;

    public List<List<Float>> gen_vectors(Integer nb) {
        List<List<Float>> xb = new LinkedList<>();
        Random random = new Random();
        for (int i = 0; i < nb; ++i) {
            LinkedList<Float> vector = new LinkedList<>();
            for (int j = 0; j < dimension; j++) {
                vector.add(random.nextFloat());
            }
            xb.add(vector);
        }
        return xb;
    }

    public static Date getDeltaDate(int delta) {
        Date today = new Date();
        Calendar c = Calendar.getInstance();
        c.setTime(today);
        c.add(Calendar.DAY_OF_MONTH, delta);
        return c.getTime();
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_delete_vectors(MilvusClient client, String tableName) throws InterruptedException {
        int nb = 10000;
        List<List<Float>> vectors = gen_vectors(nb);
        // Add vectors
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Thread.sleep(1000);
        DateRange dateRange = new DateRange(getDeltaDate(-1), getDeltaDate(1));
        DeleteByRangeParam param = new DeleteByRangeParam.Builder(dateRange, tableName).build();
        Response res_delete = client.deleteByRange(param);
        assert(res_delete.ok());
        Thread.sleep(1000);
        // Assert table row count
        TableParam tableParam = new TableParam.Builder(tableName).build();
        Assert.assertEquals(client.getTableRowCount(tableParam).getTableRowCount(), 0);
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_delete_vectors_table_not_existed(MilvusClient client, String tableName) throws InterruptedException {
        String tableNameNew = tableName + "_";
        DateRange dateRange = new DateRange(getDeltaDate(-1), getDeltaDate(1));
        DeleteByRangeParam param = new DeleteByRangeParam.Builder(dateRange, tableNameNew).build();
        Response res_delete = client.deleteByRange(param);
        assert(!res_delete.ok());
    }

    @Test(dataProvider = "DisConnectInstance", dataProviderClass = MainClass.class)
    public void test_delete_vectors_without_connect(MilvusClient client, String tableName) throws InterruptedException {
        DateRange dateRange = new DateRange(getDeltaDate(-1), getDeltaDate(1));
        DeleteByRangeParam param = new DeleteByRangeParam.Builder(dateRange, tableName).build();
        Response res_delete = client.deleteByRange(param);
        assert(!res_delete.ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_delete_vectors_table_empty(MilvusClient client, String tableName) throws InterruptedException {
        DateRange dateRange = new DateRange(getDeltaDate(-1), getDeltaDate(1));
        DeleteByRangeParam param = new DeleteByRangeParam.Builder(dateRange, tableName).build();
        Response res_delete = client.deleteByRange(param);
        assert(res_delete.ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_delete_vectors_invalid_date_range(MilvusClient client, String tableName) throws InterruptedException {
        int nb = 100;
        List<List<Float>> vectors = gen_vectors(nb);
        // Add vectors
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Thread.sleep(1000);
        DateRange dateRange = new DateRange(getDeltaDate(1), getDeltaDate(0));
        DeleteByRangeParam param = new DeleteByRangeParam.Builder(dateRange, tableName).build();
        Response res_delete = client.deleteByRange(param);
        assert(!res_delete.ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_delete_vectors_invalid_date_range_1(MilvusClient client, String tableName) throws InterruptedException {
        int nb = 100;
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        DateRange dateRange = new DateRange(getDeltaDate(2), getDeltaDate(-1));
        DeleteByRangeParam param = new DeleteByRangeParam.Builder(dateRange, tableName).build();
        Response res_delete = client.deleteByRange(param);
        assert(!res_delete.ok());
    }

    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_delete_vectors_no_result(MilvusClient client, String tableName) throws InterruptedException {
        int nb = 100;
        List<List<Float>> vectors = gen_vectors(nb);
        InsertParam insertParam = new InsertParam.Builder(tableName, vectors).build();
        InsertResponse res = client.insert(insertParam);
        assert(res.getResponse().ok());
        Thread.sleep(1000);
        DateRange dateRange = new DateRange(getDeltaDate(-3), getDeltaDate(-2));
        DeleteByRangeParam param = new DeleteByRangeParam.Builder(dateRange, tableName).build();
        Response res_delete = client.deleteByRange(param);
        assert(res_delete.ok());
        TableParam tableParam = new TableParam.Builder(tableName).build();
        Assert.assertEquals(client.getTableRowCount(tableParam).getTableRowCount(), nb);
    }

}
