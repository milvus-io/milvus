package com;

import io.milvus.client.MilvusClient;
import io.milvus.client.Response;
import io.milvus.client.ShowPartitionsResponse;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class Partition {
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

    // ----------------------------- create partition cases in ---------------------------------

    // create partition
    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_create_partition(MilvusClient client, String tableName) {
        String partitionName = RandomStringUtils.randomAlphabetic(10);
        String tag = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition partition = new io.milvus.client.Partition.Builder(tableName, partitionName, tag).build();
        Response createpResponse = client.createPartition(partition);
        assert (createpResponse.ok());
        // show partitions
        List<io.milvus.client.Partition> partitions = client.showPartitions(tableName).getPartitionList();
        System.out.println(partitions);
        List<String> partitionNames = new ArrayList<>();
        for (int i=0; i<partitions.size(); ++i) {
            partitionNames.add(partitions.get(i).getPartitionName());
        }
        Assert.assertTrue(partitionNames.contains(partitionName));
    }

    // create partition, partition existed
    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_create_partition_name_existed(MilvusClient client, String tableName) {
        String partitionName = RandomStringUtils.randomAlphabetic(10);
        String tag = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition partition = new io.milvus.client.Partition.Builder(tableName, partitionName, tag).build();
        Response createpResponse = client.createPartition(partition);
        assert (createpResponse.ok());
        String newTag = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition newPartition = new io.milvus.client.Partition.Builder(tableName, partitionName, newTag).build();
        Response newCreatepResponse = client.createPartition(newPartition);
        assert (!newCreatepResponse.ok());
    }

    // create partition, tag name existed
    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_create_partition_tag_name_existed(MilvusClient client, String tableName) {
        String partitionName = RandomStringUtils.randomAlphabetic(10);
        String tag = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition partition = new io.milvus.client.Partition.Builder(tableName, partitionName, tag).build();
        Response createpResponse = client.createPartition(partition);
        assert (createpResponse.ok());
        io.milvus.client.Partition newPartition = new io.milvus.client.Partition.Builder(tableName, partitionName, tag).build();
        Response newCreatepResponse = client.createPartition(newPartition);
        assert (!newCreatepResponse.ok());
    }

    // ----------------------------- drop partition cases in ---------------------------------

    // drop a partition created before, drop by partition name
    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_drop_partition(MilvusClient client, String tableName) {
        String partitionName = RandomStringUtils.randomAlphabetic(10);
        String tag = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition partition = new io.milvus.client.Partition.Builder(tableName, partitionName, tag).build();
        Response createpResponse = client.createPartition(partition);
        assert (createpResponse.ok());
        Response response = client.dropPartition(partitionName);
        assert (response.ok());
        // show partitions
        int length = client.showPartitions(tableName).getPartitionList().size();
        Assert.assertEquals(length, 0);
    }

    // drop a partition repeat created before, drop by partition name
    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_drop_partition_repeat(MilvusClient client, String tableName) throws InterruptedException {
        String partitionName = RandomStringUtils.randomAlphabetic(10);
        String tag = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition partition = new io.milvus.client.Partition.Builder(tableName, partitionName, tag).build();
        Response createpResponse = client.createPartition(partition);
        assert (createpResponse.ok());
        Response response = client.dropPartition(partitionName);
        assert (response.ok());
        Thread.currentThread().sleep(2000);
        Response newResponse = client.dropPartition(partitionName);
        assert (!newResponse.ok());
    }

    // drop a partition created before, drop by tag
    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_drop_partition_with_tag(MilvusClient client, String tableName) {
        String partitionName = RandomStringUtils.randomAlphabetic(10);
        String tag = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition partition = new io.milvus.client.Partition.Builder(tableName, partitionName, tag).build();
        Response createpResponse = client.createPartition(partition);
        assert (createpResponse.ok());
        Response response = client.dropPartition(tableName, tag);
        assert (response.ok());
        // show partitions
        int length = client.showPartitions(tableName).getPartitionList().size();
        Assert.assertEquals(length, 0);
    }

    // drop a partition not created before
    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_drop_partition_not_existed(MilvusClient client, String tableName) {
        String partitionName = RandomStringUtils.randomAlphabetic(10);
        String tag = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition partition = new io.milvus.client.Partition.Builder(tableName, partitionName, tag).build();
        Response createpResponse = client.createPartition(partition);
        assert(createpResponse.ok());
        String newPartitionName = RandomStringUtils.randomAlphabetic(10);
        Response response = client.dropPartition(newPartitionName);
        assert(!response.ok());
    }

    // drop a partition not created before
    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_drop_partition_tag_not_existed(MilvusClient client, String tableName) {
        String partitionName = RandomStringUtils.randomAlphabetic(10);
        String tag = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition partition = new io.milvus.client.Partition.Builder(tableName, partitionName, tag).build();
        Response createpResponse = client.createPartition(partition);
        assert(createpResponse.ok());
        String newTag = RandomStringUtils.randomAlphabetic(10);
        Response response = client.dropPartition(tableName, newTag);
        assert(!response.ok());
    }

    // ----------------------------- show partitions cases in ---------------------------------

    // create partition, then show partitions
    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_show_partitions(MilvusClient client, String tableName) {
        String partitionName = RandomStringUtils.randomAlphabetic(10);
        String tag = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition partition = new io.milvus.client.Partition.Builder(tableName, partitionName, tag).build();
        Response createpResponse = client.createPartition(partition);
        assert (createpResponse.ok());
        ShowPartitionsResponse response = client.showPartitions(tableName);
        assert (response.getResponse().ok());
        List<String> partitionNames = new ArrayList<>();
        for (int i=0; i<response.getPartitionList().size(); ++i) {
            partitionNames.add(response.getPartitionList().get(i).getPartitionName());
            if (response.getPartitionList().get(i).getPartitionName() == partitionName) {
                Assert.assertTrue(response.getPartitionList().get(i).getTableName() == tableName);
                Assert.assertTrue(response.getPartitionList().get(i).getTag() == tag);
            }
        }
        Assert.assertTrue(partitionNames.contains(partitionName));
    }

    // create multi partition, then show partitions
    @Test(dataProvider = "Table", dataProviderClass = MainClass.class)
    public void test_show_partitions_multi(MilvusClient client, String tableName) {
        String partitionName = RandomStringUtils.randomAlphabetic(10);
        String tag = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition partition = new io.milvus.client.Partition.Builder(tableName, partitionName, tag).build();
        Response createpResponse = client.createPartition(partition);
        assert (createpResponse.ok());
        String newPartitionName = RandomStringUtils.randomAlphabetic(10);
        String tagNew = RandomStringUtils.randomAlphabetic(10);
        io.milvus.client.Partition newPartition = new io.milvus.client.Partition.Builder(tableName, newPartitionName, tagNew).build();
        Response newCreatepResponse = client.createPartition(newPartition);
        assert (newCreatepResponse.ok());
        ShowPartitionsResponse response = client.showPartitions(tableName);
        assert (response.getResponse().ok());
        List<String> partitionNames = response.getPartitionNameList();
//        for (int i=0; i<response.getPartitionList().size(); ++i) {
//            partitionNames.add(response.getPartitionList().get(i).getPartitionName());
//            if (response.getPartitionList().get(i).getPartitionName() == newPartitionName) {
//                Assert.assertTrue(response.getPartitionList().get(i).getTableName() == tableName);
//                Assert.assertTrue(response.getPartitionList().get(i).getTag() == tagNew);
//            }
//        }
        System.out.println(partitionNames);
        Assert.assertTrue(partitionNames.contains(partitionName));
        Assert.assertTrue(partitionNames.contains(newPartitionName));
        List<String> tagNames = response.getPartitionTagList();
        Assert.assertTrue(tagNames.contains(tag));
        Assert.assertTrue(tagNames.contains(tagNew));
    }

}
