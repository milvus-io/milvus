package com;

import io.milvus.client.*;
import io.milvus.client.exception.ServerSideMilvusException;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;
import java.util.List;

public class TestPartition {

    // ----------------------------- create partition cases in ---------------------------------

    // create partition
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testCreatePartition(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        Assert.assertEquals(client.hasPartition(collectionName, tag), true);
        // show partitions
        List<String> partitions = client.listPartitions(collectionName);
        Assert.assertTrue(partitions.contains(tag));
    }

    // create partition, tag name existed
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testCreatePartitionTagNameExisted(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        client.createPartition(collectionName, tag);
    }

    // ----------------------------- has partition cases in ---------------------------------
    // has partition, tag name not existed
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testHasPartitionTagNameNotExisted(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        String tagNew = RandomStringUtils.randomAlphabetic(10);
        Assert.assertFalse(client.hasPartition(collectionName, tagNew));
    }

    // has partition, tag name existed
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testHasPartitionTagNameExisted(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        Assert.assertTrue(client.hasPartition(collectionName, tag));
    }

    // ----------------------------- drop partition cases in ---------------------------------

    // drop a partition created before, drop by partition name
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testDropPartition(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        client.dropPartition(collectionName, tag);
        // show partitions
        System.out.println(client.listPartitions(collectionName));
        int length = client.listPartitions(collectionName).size();
        // _default
        Assert.assertEquals(length, 1);
    }

    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testDropPartitionDefault(MilvusClient client, String collectionName) {
        String tag = "_default";
        client.createPartition(collectionName, tag);
    }

    // drop a partition repeat created before, drop by partition name
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testDropPartitionRepeat(MilvusClient client, String collectionName) throws InterruptedException {
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        client.dropPartition(collectionName, tag);
        Thread.sleep(2000);
        client.dropPartition(collectionName, tag);
    }

    // drop a partition not created before
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testDropPartitionNotExisted(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        String tagNew = RandomStringUtils.randomAlphabetic(10);
        client.dropPartition(collectionName, tagNew);
    }

    // drop a partition not created before
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class, expectedExceptions = ServerSideMilvusException.class)
    public void testDropPartitionTagNotExisted(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        String newTag = RandomStringUtils.randomAlphabetic(10);
        client.dropPartition(collectionName, newTag);
    }

    // ----------------------------- show partitions cases in ---------------------------------

    // create partition, then show partitions
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testShowPartitions(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        List<String> partitions = client.listPartitions(collectionName);
        Assert.assertTrue(partitions.contains(tag));
    }

    // create multi partition, then show partitions
    @Test(dataProvider = "Collection", dataProviderClass = MainClass.class)
    public void testShowPartitionsMulti(MilvusClient client, String collectionName) {
        String tag = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tag);
        String tagNew = RandomStringUtils.randomAlphabetic(10);
        client.createPartition(collectionName, tagNew);
        List<String> partitions = client.listPartitions(collectionName);
        System.out.println(partitions);
        Assert.assertTrue(partitions.contains(tag));
        Assert.assertTrue(partitions.contains(tagNew));
    }

}
