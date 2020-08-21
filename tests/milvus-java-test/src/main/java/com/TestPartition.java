package com;

import io.milvus.client.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;

public class TestPartition {
    int dimension = 128;

    // ----------------------------- create partition cases in ---------------------------------

    // create partition
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreatePartition(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        assert (createpResponse.ok());
        // show partitions
        List<String> partitions = client.listPartitions(collectionName).getPartitionList();
        System.out.println(partitions);
        Assert.assertTrue(partitions.contains(tag));
    }

    // create partition, tag name existed
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreatePartitionTagNameExisted(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        assert (createpResponse.ok());
        Response createpResponseNew = client.createPartition(collectionName, tag);
        assert (!createpResponseNew.ok());
    }

    // ----------------------------- has partition cases in ---------------------------------
    // has partition, tag name not existed
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testHasPartitionTagNameNotExisted(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        assert (createpResponse.ok());
        String tagNew = RandomStringUtils.randomAlphabetic(10);
        HasPartitionResponse haspResponse = client.hasPartition(collectionName, tagNew);
        assert (haspResponse.ok());
        Assert.assertFalse(haspResponse.hasPartition());
    }

    // has partition, tag name existed
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testHasPartitionTagNameExisted(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        assert (createpResponse.ok());
        HasPartitionResponse haspResponse = client.hasPartition(collectionName, tag);
        assert (haspResponse.ok());
        Assert.assertTrue(haspResponse.hasPartition());
    }

    // ----------------------------- drop partition cases in ---------------------------------

    // drop a partition created before, drop by partition name
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropPartition(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponseNew = client.createPartition(collectionName, tag);
        assert (createpResponseNew.ok());
        Response response = client.dropPartition(collectionName, tag);
        assert (response.ok());
        // show partitions
        System.out.println(client.listPartitions(collectionName).getPartitionList());
        int length = client.listPartitions(collectionName).getPartitionList().size();
        // _default
        Assert.assertEquals(length, 1);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropPartitionDefault(MilvusClient client, String collectionName) {
        String tag = "_default";
        Response createpResponseNew = client.createPartition(collectionName, tag);
        assert (!createpResponseNew.ok());
//         show partitions
//        System.out.println(client.listPartitions(collectionName).getPartitionList());
//        int length = client.listPartitions(collectionName).getPartitionList().size();
//        // _default
//        Assert.assertEquals(length, 1);
    }

    // drop a partition repeat created before, drop by partition name
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropPartitionRepeat(MilvusClient client, String collectionName) throws InterruptedException {
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        assert (createpResponse.ok());
        Response response = client.dropPartition(collectionName, tag);
        assert (response.ok());
        Thread.currentThread().sleep(2000);
        Response newResponse = client.dropPartition(collectionName, tag);
        assert (!newResponse.ok());
    }

    // drop a partition not created before
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropPartitionNotExisted(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        assert (createpResponse.ok());
        String tagNew = RandomStringUtils.randomAlphabetic(10);
        Response response = client.dropPartition(collectionName, tagNew);
        assert(!response.ok());
    }

    // drop a partition not created before
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropPartitionTagNotExisted(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        assert(createpResponse.ok());
        String newTag = RandomStringUtils.randomAlphabetic(10);
        Response response = client.dropPartition(collectionName, newTag);
        assert(!response.ok());
    }

    // ----------------------------- show partitions cases in ---------------------------------

    // create partition, then show partitions
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testShowPartitions(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        assert (createpResponse.ok());
        ListPartitionsResponse response = client.listPartitions(collectionName);
        assert (response.getResponse().ok());
        Assert.assertTrue(response.getPartitionList().contains(tag));
    }

    // create multi partition, then show partitions
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testShowPartitionsMulti(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        Response createpResponse = client.createPartition(collectionName, tag);
        assert (createpResponse.ok());
        String tagNew = RandomStringUtils.randomAlphabetic(10);
        Response newCreatepResponse = client.createPartition(collectionName, tagNew);
        assert (newCreatepResponse.ok());
        ListPartitionsResponse response = client.listPartitions(collectionName);
        assert (response.getResponse().ok());
        System.out.println(response.getPartitionList());
        Assert.assertTrue(response.getPartitionList().contains(tag));
        Assert.assertTrue(response.getPartitionList().contains(tagNew));
    }

}
