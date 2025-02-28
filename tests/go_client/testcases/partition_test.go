package testcases

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestPartitionsDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// create multi partitions
	expPar := []string{common.DefaultPartition}
	for i := 0; i < 10; i++ {
		// create par
		parName := common.GenRandomString("par", 4)
		err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
		common.CheckErr(t, err, true)

		// has par
		has, errHas := mc.HasPartition(ctx, client.NewHasPartitionOption(schema.CollectionName, parName))
		common.CheckErr(t, errHas, true)
		require.Truef(t, has, "should has partition")
		expPar = append(expPar, parName)
	}

	// list partitions
	partitionNames, errList := mc.ListPartitions(ctx, client.NewListPartitionOption(schema.CollectionName))
	common.CheckErr(t, errList, true)
	require.ElementsMatch(t, expPar, partitionNames)

	// drop partitions
	for _, par := range partitionNames {
		err := mc.DropPartition(ctx, client.NewDropPartitionOption(schema.CollectionName, par))
		if par == common.DefaultPartition {
			common.CheckErr(t, err, false, "default partition cannot be deleted")
		} else {
			common.CheckErr(t, err, true)
			has2, _ := mc.HasPartition(ctx, client.NewHasPartitionOption(schema.CollectionName, par))
			require.False(t, has2)
		}
	}

	// list partitions
	partitionNames, errList = mc.ListPartitions(ctx, client.NewListPartitionOption(schema.CollectionName))
	common.CheckErr(t, errList, true)
	require.Equal(t, []string{common.DefaultPartition}, partitionNames)
}

func TestCreatePartitionInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// create partition with invalid name
	expPars := []string{common.DefaultPartition}
	for _, invalidName := range common.GenInvalidNames() {
		log.Debug("invalidName", zap.String("currentName", invalidName))
		err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, invalidName))
		if invalidName == "1" {
			common.CheckErr(t, err, true)
			expPars = append(expPars, invalidName)
			continue
		}
		common.CheckErr(t, err, false, "Partition name should not be empty",
			"Partition name can only contain numbers, letters and underscores",
			"The first character of a partition name must be an underscore or letter",
			fmt.Sprintf("The length of a partition name must be less than %d characters", common.MaxCollectionNameLen))
	}

	// create partition with existed partition name -> no error
	parName := common.GenRandomString("par", 3)
	err1 := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err1, true)
	err1 = mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err1, true)
	expPars = append(expPars, parName)

	// create partition with not existed collection name
	err2 := mc.CreatePartition(ctx, client.NewCreatePartitionOption("aaa", common.GenRandomString("par", 3)))
	common.CheckErr(t, err2, false, "not found")

	// create default partition
	err3 := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, common.DefaultPartition))
	common.CheckErr(t, err3, true)

	// list partitions
	pars, errList := mc.ListPartitions(ctx, client.NewListPartitionOption(schema.CollectionName))
	common.CheckErr(t, errList, true)
	require.ElementsMatch(t, expPars, pars)
}

func TestPartitionsNumExceedsMax(t *testing.T) {
	// 120 seconds may timeout for 1024 partitions
	ctx := hp.CreateContext(t, time.Second*300)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// create multi partitions
	for i := 0; i < common.MaxPartitionNum-1; i++ {
		// create par
		parName := fmt.Sprintf("par_%d", i)
		err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
		common.CheckErr(t, err, true)
	}
	pars, errList := mc.ListPartitions(ctx, client.NewListPartitionOption(schema.CollectionName))
	common.CheckErr(t, errList, true)
	require.Len(t, pars, common.MaxPartitionNum)

	// create partition exceed max
	parName := common.GenRandomString("par", 4)
	err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, false, fmt.Sprintf("exceeds max configuration (%d)", common.MaxPartitionNum))
}

func TestDropPartitionInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	errDrop := mc.DropPartition(ctx, client.NewDropPartitionOption("aaa", "aaa"))
	common.CheckErr(t, errDrop, false, "collection not found")

	errDrop1 := mc.DropPartition(ctx, client.NewDropPartitionOption(schema.CollectionName, "aaa"))
	common.CheckErr(t, errDrop1, true)

	err := mc.DropPartition(ctx, client.NewDropPartitionOption(schema.CollectionName, common.DefaultPartition))
	common.CheckErr(t, err, false, "default partition cannot be deleted")

	// list partitions
	pars, errList := mc.ListPartitions(ctx, client.NewListPartitionOption(schema.CollectionName))
	common.CheckErr(t, errList, true)
	require.ElementsMatch(t, []string{common.DefaultPartition}, pars)
}

func TestListHasPartitionInvalid(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// list partitions
	_, errList := mc.ListPartitions(ctx, client.NewListPartitionOption("aaa"))
	common.CheckErr(t, errList, false, "collection not found")

	// list partitions
	_, errHas := mc.HasPartition(ctx, client.NewHasPartitionOption("aaa", "aaa"))
	common.CheckErr(t, errHas, false, "collection not found")
}

func TestDropPartitionData(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := createDefaultMilvusClient(ctx, t)

	// create collection
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	// create multi partitions
	parName := common.GenRandomString("par", 4)
	err := mc.CreatePartition(ctx, client.NewCreatePartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, err, true)

	// has par
	has, errHas := mc.HasPartition(ctx, client.NewHasPartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, errHas, true)
	require.Truef(t, has, "should has partition")

	// insert data into partition -> query check
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema).TWithPartitionName(parName), hp.TNewDataOption())
	res, errQ := mc.Query(ctx, client.NewQueryOption(schema.CollectionName).WithConsistencyLevel(entity.ClStrong).WithPartitions(parName).WithOutputFields(common.QueryCountFieldName))
	common.CheckErr(t, errQ, true)
	count, _ := res.GetColumn(common.QueryCountFieldName).Get(0)
	require.EqualValues(t, common.DefaultNb, count)

	// drop partition
	errDrop := mc.DropPartition(ctx, client.NewDropPartitionOption(schema.CollectionName, parName))
	common.CheckErr(t, errDrop, false, "partition cannot be dropped, partition is loaded, please release it first")

	// release -> drop -> load -> query check
	t.Log("waiting for release implement")
}
