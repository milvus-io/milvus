//go:build L3

package L3cases

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

const (
	configQnNodes = 4
	newRgNode     = 2
)

func teardownTest(t *testing.T, ctx context.Context, mc *base.MilvusClient) {
	t.Helper()
	// release and drop all collections
	collections, _ := mc.ListCollections(ctx, client.NewListCollectionOption())
	for _, collection := range collections {
		err := mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(collection))
		common.CheckErr(t, err, true)
		err = mc.DropCollection(ctx, client.NewDropCollectionOption(collection))
		common.CheckErr(t, err, true)
	}

	// reset resource groups
	rgs, errList := mc.ListResourceGroups(ctx, client.NewListResourceGroupsOption())
	common.CheckErr(t, errList, true)
	for _, rg := range rgs {
		// describe rg and get available node
		err := mc.UpdateResourceGroup(ctx, client.NewUpdateResourceGroupOption(rg, &entity.ResourceGroupConfig{
			Requests: entity.ResourceGroupLimit{NodeNum: 0},
			Limits:   entity.ResourceGroupLimit{NodeNum: 0},
		}))
		common.CheckErr(t, err, true)
	}
	for _, rg := range rgs {
		if rg != common.DefaultRgName {
			errDrop := mc.DropResourceGroup(ctx, client.NewDropResourceGroupOption(rg))
			common.CheckErr(t, errDrop, true)
		}
	}

	rgs2, errList2 := mc.ListResourceGroups(ctx, client.NewListResourceGroupsOption())
	common.CheckErr(t, errList2, true)
	require.Len(t, rgs2, 1)
}

func checkResourceGroup(t *testing.T, ctx context.Context, mc *base.MilvusClient, expRg *entity.ResourceGroup) {
	actualRg, err := mc.DescribeResourceGroup(ctx, client.NewDescribeResourceGroupOption(expRg.Name))
	common.CheckErr(t, err, true)
	log.Ctx(ctx).Info("checkResourceGroup", zap.Any("actualRg", actualRg))
	common.CheckResourceGroup(t, actualRg, expRg)
}

// test rg default: list rg, create rg, describe rg, update rg
func TestRgDefault(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	// describe default rg and check default rg info: Limits: 1000000, Nodes: all
	expDefaultRg := &entity.ResourceGroup{
		Name:             common.DefaultRgName,
		Capacity:         common.DefaultRgCapacity,
		NumAvailableNode: configQnNodes,
		Config: &entity.ResourceGroupConfig{
			Limits: entity.ResourceGroupLimit{NodeNum: 0},
		},
	}
	checkResourceGroup(t, ctx, mc, expDefaultRg)

	// create new rg
	rgName := common.GenRandomString("rg", 6)
	errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeRequest(newRgNode).WithNodeLimit(newRgNode))
	common.CheckErr(t, errCreate, true)

	// list rgs
	rgs, errList := mc.ListResourceGroups(ctx, client.NewListResourceGroupsOption())
	common.CheckErr(t, errList, true)
	require.ElementsMatch(t, rgs, []string{common.DefaultRgName, rgName})

	// describe new rg and check new rg info
	expRg := &entity.ResourceGroup{
		Name:             rgName,
		Capacity:         newRgNode,
		NumAvailableNode: newRgNode,
		Config: &entity.ResourceGroupConfig{
			Requests: entity.ResourceGroupLimit{NodeNum: newRgNode},
			Limits:   entity.ResourceGroupLimit{NodeNum: newRgNode},
		},
	}
	checkResourceGroup(t, ctx, mc, expRg)

	// update resource group
	errUpdate := mc.UpdateResourceGroup(ctx, client.NewUpdateResourceGroupOption(rgName, &entity.ResourceGroupConfig{
		Requests: entity.ResourceGroupLimit{NodeNum: configQnNodes},
		Limits:   entity.ResourceGroupLimit{NodeNum: configQnNodes},
	}))
	common.CheckErr(t, errUpdate, true)

	// check rg info after transfer nodes
	transferRg := &entity.ResourceGroup{
		Name:             rgName,
		Capacity:         configQnNodes,
		NumAvailableNode: configQnNodes,
		Config: &entity.ResourceGroupConfig{
			Requests: entity.ResourceGroupLimit{NodeNum: configQnNodes},
		},
	}
	checkResourceGroup(t, ctx, mc, transferRg)

	// check default rg info: 0 Nodes
	expDefaultRg2 := &entity.ResourceGroup{
		Name:             common.DefaultRgName,
		Capacity:         common.DefaultRgCapacity,
		NumAvailableNode: 0,
		Config: &entity.ResourceGroupConfig{
			Limits: entity.ResourceGroupLimit{NodeNum: 0},
		},
	}
	checkResourceGroup(t, ctx, mc, expDefaultRg2)
}

// test create rg with invalid name
func TestCreateRgInvalidNames(t *testing.T) {
	t.Parallel()
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	type invalidNameStruct struct {
		name   string
		errMsg string
	}

	invalidNames := []invalidNameStruct{
		{name: "", errMsg: "resource group name couldn't be empty"},
		{name: "12-s", errMsg: "name must be an underscore or letter"},
		{name: "(mn)", errMsg: "name must be an underscore or letter"},
		{name: "中文", errMsg: "name must be an underscore or letter"},
		{name: "%$#", errMsg: "name must be an underscore or letter"},
		{name: common.GenLongString(common.MaxCollectionNameLen + 1), errMsg: "name must be less than 255 characters"},
	}
	// create rg with invalid name
	for _, invalidName := range invalidNames {
		errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(invalidName.name))
		common.CheckErr(t, errCreate, false, invalidName.errMsg)
	}
}

// test create rg with existed name
func TestCreateExistedRg(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	// create default rg
	mc.DescribeResourceGroup(ctx, client.NewDescribeResourceGroupOption(common.DefaultRgName))
	err := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(common.DefaultRgName))
	common.CheckErr(t, err, false, "resource group already exist, but create with different config")

	// create same repeatedly
	rgName := common.GenRandomString("rg", 6)
	errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeRequest(configQnNodes).WithNodeLimit(configQnNodes))
	common.CheckErr(t, errCreate, true)
	rg, _ := mc.DescribeResourceGroup(ctx, client.NewDescribeResourceGroupOption(rgName))

	// create existed rg with different config
	errCreate = mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeRequest(newRgNode).WithNodeLimit(newRgNode))
	common.CheckErr(t, errCreate, false, "resource group already exist")
	rg1, _ := mc.DescribeResourceGroup(ctx, client.NewDescribeResourceGroupOption(rgName))
	//require.EqualValues(t, rg1, rg)
	common.CheckResourceGroup(t, rg1, rg)

	// create existed rg with same config
	errCreate = mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeRequest(configQnNodes).WithNodeLimit(configQnNodes))
	common.CheckErr(t, errCreate, true)
	rg2, _ := mc.DescribeResourceGroup(ctx, client.NewDescribeResourceGroupOption(rgName))
	common.CheckResourceGroup(t, rg2, rg)
}

func TestCreateRgWithRequestsLimits(t *testing.T) {
	type requestsLimits struct {
		requests  int
		limits    int
		available int32
		errMsg    string
	}
	reqAndLimits := []requestsLimits{
		{requests: 0, limits: 0, available: 0},
		{requests: -1, limits: 0, errMsg: "node num in `requests` or `limits` should not less than 0"},
		{requests: 0, limits: -2, errMsg: "node num in `requests` or `limits` should not less than 0"},
		{requests: 10, limits: 1, errMsg: "limits node num should not less than requests node num"},
		{requests: 2, limits: 3, available: 3},
		{requests: configQnNodes * 2, limits: configQnNodes * 3, available: configQnNodes},
		{requests: configQnNodes, limits: configQnNodes, available: configQnNodes},
	}
	// connect
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	rgName := common.GenRandomString("rg", 6)
	// create rg with request only -> use default 0 limit -> error
	err := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeRequest(newRgNode))
	common.CheckErr(t, err, false, "limits node num should not less than requests node num")

	//create rg with limit only -> use default 0 request
	err = mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeLimit(newRgNode))
	common.CheckErr(t, err, true)

	for _, rl := range reqAndLimits {
		log.Ctx(ctx).Info("TestCreateRgWithRequestsLimits", zap.Any("reqAndLimit", rl))
		rgName := common.GenRandomString("rg", 6)

		errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeRequest(rl.requests).WithNodeLimit(rl.limits))
		if rl.errMsg != "" {
			common.CheckErr(t, errCreate, false, rl.errMsg)
		} else {
			expDefaultRg := &entity.ResourceGroup{
				Name:             rgName,
				Capacity:         int32(rl.requests),
				NumAvailableNode: rl.available,
				Config: &entity.ResourceGroupConfig{
					Requests: entity.ResourceGroupLimit{NodeNum: int32(rl.requests)},
					Limits:   entity.ResourceGroupLimit{NodeNum: int32(rl.limits)},
				},
			}
			checkResourceGroup(t, ctx, mc, expDefaultRg)
		}
		teardownTest(t, ctx, mc)
	}
}

func TestRgAdvanced(t *testing.T) {
	// test transfer replica from rg to default rg
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	// create new rg with requests 2
	rgName := common.GenRandomString("rg", 6)
	errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName))
	common.CheckErr(t, errCreate, true)

	// load two replicas with multi resource groups
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	_, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName).WithReplica(2).WithResourceGroup(rgName))
	common.CheckErr(t, err, false, "resource group node not enough")

	// update rg -> load -> drop rg
	mc.UpdateResourceGroup(ctx, client.NewUpdateResourceGroupOption(rgName, &entity.ResourceGroupConfig{
		Requests: entity.ResourceGroupLimit{NodeNum: newRgNode},
		Limits:   entity.ResourceGroupLimit{NodeNum: newRgNode},
	}))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName).TWithReplica(2).TWithResourceGroups(rgName))

	// drop rg which loaded replicas
	err = mc.DropResourceGroup(ctx, client.NewDropResourceGroupOption(rgName))
	common.CheckErr(t, err, false, "some replicas still loaded in resource group")

	err = mc.TransferReplica(ctx, client.NewTransferReplicaOption(schema.CollectionName, rgName, common.DefaultRgName, 2))
	common.CheckErr(t, err, true)
	err = mc.DropResourceGroup(ctx, client.NewDropResourceGroupOption(rgName))
	common.CheckErr(t, err, false, "resource group's limits node num is not 0")
}

func TestUpdateRgWithTransfer(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	// create rg0 with requests=2, limits=3, total 4 nodes
	rg0 := common.GenRandomString("rg0", 6)
	rg0Limits := newRgNode + 1
	errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rg0).WithNodeRequest(newRgNode).WithNodeLimit(rg0Limits))
	common.CheckErr(t, errCreate, true)

	// check rg0 available node: 3, default available node: 1
	actualRg0, _ := mc.DescribeResourceGroup(ctx, client.NewDescribeResourceGroupOption(rg0))
	require.Lenf(t, actualRg0.Nodes, rg0Limits, fmt.Sprintf("expected %s has %d available nodes", rg0, rg0Limits))
	actualRgDef, _ := mc.DescribeResourceGroup(ctx, client.NewDescribeResourceGroupOption(common.DefaultRgName))
	require.Lenf(t, actualRgDef.Nodes, configQnNodes-rg0Limits, fmt.Sprintf("expected %s has %d available nodes", common.DefaultRgName, configQnNodes-rg0Limits))

	// create rg1 with TransferFrom & TransferTo & requests=3, limits=4
	rg1 := common.GenRandomString("rg1", 6)
	rg1Requests := newRgNode + 1
	errCreate = mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rg1).WithNodeLimit(configQnNodes).WithNodeRequest(rg1Requests))
	// Note: default Requests & Limits of ResourceGroupConfig struct is 0
	errUpdate := mc.UpdateResourceGroup(ctx, client.NewUpdateResourceGroupOption(rg1, &entity.ResourceGroupConfig{
		Requests: entity.ResourceGroupLimit{NodeNum: int32(rg1Requests)},
		Limits:   entity.ResourceGroupLimit{NodeNum: int32(configQnNodes)},
		TransferFrom: []*entity.ResourceGroupTransfer{
			{ResourceGroup: rg0},
		},
		TransferTo: []*entity.ResourceGroupTransfer{
			{ResourceGroup: common.DefaultRgName},
		},
	}))
	common.CheckErr(t, errUpdate, true)

	// verify available nodes: rg0 + rg1 = configQnNodes = 4
	time.Sleep(time.Duration(rand.Intn(6)) * time.Second)
	actualRg0, _ = mc.DescribeResourceGroup(ctx, client.NewDescribeResourceGroupOption(rg0))
	actualRg1, _ := mc.DescribeResourceGroup(ctx, client.NewDescribeResourceGroupOption(rg1))
	require.EqualValuesf(t, configQnNodes, len(actualRg0.Nodes)+len(actualRg1.Nodes), fmt.Sprintf("Expected the total available nodes of %s and %s is %d ", rg0, rg1, configQnNodes))
	expDefaultRg1 := &entity.ResourceGroup{
		Name:             rg1,
		Capacity:         int32(rg1Requests),
		NumAvailableNode: -1, // not check
		Config: &entity.ResourceGroupConfig{
			Requests: entity.ResourceGroupLimit{NodeNum: int32(rg1Requests)},
			Limits:   entity.ResourceGroupLimit{NodeNum: configQnNodes},
		},
	}
	checkResourceGroup(t, ctx, mc, expDefaultRg1)
}

func TestUpdateRgWithNotExistTransfer(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	rgName := common.GenRandomString("rg", 6)
	errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName))
	common.CheckErr(t, errCreate, true)

	// update rg with not existed TransferFrom rg
	errUpdate := mc.UpdateResourceGroup(ctx, client.NewUpdateResourceGroupOption(rgName, &entity.ResourceGroupConfig{
		TransferFrom: []*entity.ResourceGroupTransfer{
			{ResourceGroup: "aaa"},
		},
	}))
	common.CheckErr(t, errUpdate, false, "resource group in `TransferFrom` aaa not exist")

	errUpdate1 := mc.UpdateResourceGroup(ctx, client.NewUpdateResourceGroupOption(common.GenRandomString("rg", 6), &entity.ResourceGroupConfig{
		Requests: entity.ResourceGroupLimit{NodeNum: newRgNode},
		Limits:   entity.ResourceGroupLimit{NodeNum: newRgNode},
	}))
	common.CheckErr(t, errUpdate1, false, "resource group not found")

	// update rg with not existed TransferTo rg
	errUpdate = mc.UpdateResourceGroup(ctx, client.NewUpdateResourceGroupOption(rgName, &entity.ResourceGroupConfig{
		TransferTo: []*entity.ResourceGroupTransfer{
			{ResourceGroup: "aaa"},
		},
	}))
	common.CheckErr(t, errUpdate, false, "resource group in `TransferTo` aaa not exist")
}

func TestUpdateRgWithRequestsLimits(t *testing.T) {
	type requestsLimits struct {
		requests  int32
		limits    int32
		available int32
		errMsg    string
	}
	reqAndLimits := []requestsLimits{
		{requests: 0, limits: 0, available: 0},
		{requests: -1, limits: 0, errMsg: "node num in `requests` or `limits` should not less than 0"},
		{requests: 0, limits: -2, errMsg: "node num in `requests` or `limits` should not less than 0"},
		{requests: 10, limits: 1, errMsg: "limits node num should not less than requests node num"},
		{requests: 2, limits: 3, available: 3},
		{requests: configQnNodes * 2, limits: configQnNodes * 3, available: configQnNodes},
		{requests: configQnNodes, limits: configQnNodes, available: configQnNodes},
	}
	// connect
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	rgName := common.GenRandomString("rg", 6)
	mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeRequest(newRgNode).WithNodeLimit(newRgNode))

	// default ResourceGroupConfig.Limits.NodeNum is 0
	err := mc.UpdateResourceGroup(ctx, client.NewUpdateResourceGroupOption(rgName, &entity.ResourceGroupConfig{
		Requests: entity.ResourceGroupLimit{NodeNum: 1},
	}))
	common.CheckErr(t, err, false, "limits node num should not less than requests node num")

	for _, rl := range reqAndLimits {
		log.Ctx(ctx).Info("TestUpdateRgWithRequestsLimits", zap.Any("reqAndLimit", rl))
		errCreate := mc.UpdateResourceGroup(ctx, client.NewUpdateResourceGroupOption(rgName, &entity.ResourceGroupConfig{
			Requests: entity.ResourceGroupLimit{NodeNum: rl.requests},
			Limits:   entity.ResourceGroupLimit{NodeNum: rl.limits},
		}))
		if rl.errMsg != "" {
			common.CheckErr(t, errCreate, false, rl.errMsg)
		} else {
			expDefaultRg := &entity.ResourceGroup{
				Name:             rgName,
				Capacity:         rl.requests,
				NumAvailableNode: rl.available,
				Config: &entity.ResourceGroupConfig{
					Requests: entity.ResourceGroupLimit{NodeNum: rl.requests},
					Limits:   entity.ResourceGroupLimit{NodeNum: rl.limits},
				},
			}
			checkResourceGroup(t, ctx, mc, expDefaultRg)
		}
	}
}

// describe/drop/update rg with not existed name
func TestOperateRgNotExisted(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// describe not existed rg
	_, errDescribe := mc.DescribeResourceGroup(ctx, client.NewDescribeResourceGroupOption(common.GenRandomString("rg", 6)))
	common.CheckErr(t, errDescribe, false, "resource group not found")

	// drop non-existed rg
	errDrop := mc.DropResourceGroup(ctx, client.NewDropResourceGroupOption(common.GenRandomString("rg", 6)))
	common.CheckErr(t, errDrop, true)

	errUpdate := mc.UpdateResourceGroup(ctx, client.NewUpdateResourceGroupOption(common.GenRandomString("rg", 6), &entity.ResourceGroupConfig{
		Requests: entity.ResourceGroupLimit{NodeNum: newRgNode},
	}))
	common.CheckErr(t, errUpdate, false, "resource group not found")
}

// drop empty default rg
func TestDropRg(t *testing.T) {
	t.Log("https://github.com/milvus-io/milvus/issues/39942")
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	// try to drop default rg
	errDropDefault := mc.DropResourceGroup(ctx, client.NewDropResourceGroupOption(common.DefaultRgName))
	common.CheckErr(t, errDropDefault, false, "default resource group is not deletable")

	// create new rg with all nodes
	rgName := common.GenRandomString("rg", 6)
	errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeRequest(configQnNodes).WithNodeLimit(configQnNodes))
	common.CheckErr(t, errCreate, true)

	// drop rg and rg available node is not 0
	errDrop := mc.DropResourceGroup(ctx, client.NewDropResourceGroupOption(rgName))
	common.CheckErr(t, errDrop, false, "resource group's limits node num is not 0")

	// describe default rg
	transferRg := &entity.ResourceGroup{
		Name:             common.DefaultRgName,
		Capacity:         common.DefaultRgCapacity,
		NumAvailableNode: 0,
	}
	checkResourceGroup(t, ctx, mc, transferRg)

	// drop empty default rg
	errDrop = mc.DropResourceGroup(ctx, client.NewDropResourceGroupOption(common.DefaultRgName))
	common.CheckErr(t, errDrop, false, "default resource group is not deletable")
}

// test list rgs
func TestListRgs(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	rgNum := 10
	rgs := []string{common.DefaultRgName}

	// list default rg
	listRgs, errList := mc.ListResourceGroups(ctx, client.NewListResourceGroupsOption())
	common.CheckErr(t, errList, true)
	require.EqualValues(t, listRgs, rgs)

	// create 10 new rgs
	for i := 1; i <= rgNum; i++ {
		rgName := common.GenRandomString("rg", 6)
		errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName))
		common.CheckErr(t, errCreate, true)
		rgs = append(rgs, rgName)
	}

	// list rgs
	listRgs, errList = mc.ListResourceGroups(ctx, client.NewListResourceGroupsOption())
	common.CheckErr(t, errList, true)
	require.ElementsMatch(t, listRgs, rgs)
}

// test transfer replica of not existed collection
func TestTransferReplicaNotExisted(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	// create new rg with 0 node
	rgName := common.GenRandomString("rg", 6)
	errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeLimit(newRgNode))
	common.CheckErr(t, errCreate, true)

	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName).TWithResourceGroups(rgName))

	// transfer replica
	errTransfer := mc.TransferReplica(ctx, client.NewTransferReplicaOption(common.GenRandomString("coll", 6), rgName, common.DefaultRgName, 1))
	common.CheckErr(t, errTransfer, false, "collection not found")

	// source not exist
	errSource := mc.TransferReplica(ctx, client.NewTransferReplicaOption(schema.CollectionName, common.GenRandomString("rg", 6), common.DefaultRgName, 1))
	common.CheckErr(t, errSource, false, "resource group not found")

	// target not exist
	errTarget := mc.TransferReplica(ctx, client.NewTransferReplicaOption(schema.CollectionName, common.DefaultRgName, common.GenRandomString("rg", 6), 1))
	common.CheckErr(t, errTarget, false, "resource group not found")

	// transfer to self -> error
	errSelf := mc.TransferReplica(ctx, client.NewTransferReplicaOption(schema.CollectionName, rgName, rgName, 1))
	common.CheckErr(t, errSelf, false, "source resource group and target resource group should not be the same")
}

// test transfer replicas with invalid replica number
func TestTransferReplicaInvalidReplicaNumber(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	// create new rg
	rgName := common.GenRandomString("rg", 6)
	errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeLimit(newRgNode))
	common.CheckErr(t, errCreate, true)

	// create collection -> load 2 replicas with rgName
	_, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())

	// invalid replicas
	type invalidReplicasStruct struct {
		replicaNumber int64
		errMsg        string
	}
	invalidReplicas := []invalidReplicasStruct{
		{replicaNumber: 0, errMsg: "invalid parameter[expected=NumReplica > 0][actual=invalid NumReplica 0]"},
		{replicaNumber: -1, errMsg: "invalid parameter[expected=NumReplica > 0][actual=invalid NumReplica -1]"},
		{replicaNumber: 1, errMsg: "Collection not loaded"},
	}

	for _, invalidReplica := range invalidReplicas {
		// transfer replica
		log.Ctx(ctx).Info("TestTransferReplicaInvalidReplicaNumber", zap.Int64("replica", invalidReplica.replicaNumber))
		errTransfer := mc.TransferReplica(ctx, client.NewTransferReplicaOption(schema.CollectionName, rgName, common.DefaultRgName, invalidReplica.replicaNumber))
		common.CheckErr(t, errTransfer, false, invalidReplica.errMsg)
	}
}

// test transfer replicas
func TestTransferReplicas(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	// create new rg1 and rg2
	rg1Name := common.GenRandomString("rg1", 6)
	errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rg1Name).WithNodeRequest(newRgNode).WithNodeLimit(newRgNode))
	common.CheckErr(t, errCreate, true)

	// init collection: create -> insert -> index -> load 2 replicas with rg1
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	replica := 2
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName).TWithResourceGroups(rg1Name).TWithReplica(replica))

	// transfer from defaultRg (not loaded collection) -> error
	err := mc.TransferReplica(ctx, client.NewTransferReplicaOption(schema.CollectionName, common.DefaultRgName, rg1Name, 1))
	common.CheckErr(t, err, false, "NumReplica not greater than the number of replica in source resource group]")

	// transfer replicas more than actual
	errReplicas := mc.TransferReplica(ctx, client.NewTransferReplicaOption(schema.CollectionName, rg1Name, common.DefaultRgName, int64(replica+1)))
	common.CheckErr(t, errReplicas, false, "NumReplica not greater than the number of replica in source resource group]")

	// transfer replica to default rg
	errDefault := mc.TransferReplica(ctx, client.NewTransferReplicaOption(schema.CollectionName, rg1Name, common.DefaultRgName, 1))
	common.CheckErr(t, errDefault, true)

	expRg1 := &entity.ResourceGroup{
		Name:             rg1Name,
		Capacity:         newRgNode,
		NumAvailableNode: newRgNode,
		NumLoadedReplica: map[string]int32{schema.CollectionName: 1},
	}
	checkResourceGroup(t, ctx, mc, expRg1)
	expRgDef := &entity.ResourceGroup{
		Name:             common.DefaultRgName,
		Capacity:         common.DefaultRgCapacity,
		NumAvailableNode: newRgNode,
		NumLoadedReplica: map[string]int32{schema.CollectionName: 1},
	}
	checkResourceGroup(t, ctx, mc, expRgDef)

	// search
	for i := 0; i < 10; i++ {
		vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
		searchRes, _ := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors))
		common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	}
}

// test transfer replica to 0 available nodes rg
func TestTransferReplicasNodesNotEnough(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	// create new rg with requests 2
	rgName := common.GenRandomString("rg", 6)
	errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName))
	common.CheckErr(t, errCreate, true)

	// load two replicas with multi resource groups
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName).TWithReplica(2))

	// transfer 1 replica into rg which no available node
	errReplica := mc.TransferReplica(ctx, client.NewTransferReplicaOption(schema.CollectionName, common.DefaultRgName, rgName, 1))
	common.CheckErr(t, errReplica, true)

	// check default rg
	transferRg2 := &entity.ResourceGroup{
		Name:             common.DefaultRgName,
		Capacity:         common.DefaultRgCapacity,
		NumAvailableNode: configQnNodes,
		NumLoadedReplica: map[string]int32{schema.CollectionName: 1},
		NumIncomingNode:  map[string]int32{schema.CollectionName: 1},
		Config: &entity.ResourceGroupConfig{
			Limits: entity.ResourceGroupLimit{NodeNum: 0},
		},
	}
	checkResourceGroup(t, ctx, mc, transferRg2)

	// check rg after transfer replica
	expRg := &entity.ResourceGroup{
		Name:             rgName,
		Capacity:         0,
		NumAvailableNode: 0,
		NumLoadedReplica: map[string]int32{schema.CollectionName: 1},
		NumOutgoingNode:  map[string]int32{schema.CollectionName: 1},
		Config: &entity.ResourceGroupConfig{
			Limits: entity.ResourceGroupLimit{NodeNum: 0},
		},
	}
	checkResourceGroup(t, ctx, mc, expRg)

	// search
	vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
	searchRes, _ := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors))
	common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
}

// test transfer replica from rg to default rg
/*
relationship between replicas and rg. there are n rgs:
if n == 1:
	- n:1 & replicas <= rgNode & replicas <= sn
if n > 1:
	- n:n
- (n+1):n -> error
- n:(n+1) -> error
*/
func TestLoadReplicasRgs(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	// create new rg with requests 2
	rgName := common.GenRandomString("rg", 6)
	errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeRequest(newRgNode).WithNodeLimit(newRgNode))
	common.CheckErr(t, errCreate, true)

	// load two replicas with multi resource groups
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))

	// load with not existed rg
	_, errLoad := mc.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName).WithReplica(1).WithResourceGroup(common.GenRandomString("rg", 4)))
	common.CheckErr(t, errLoad, false, " resource group not found")

	// replicas:rg = n:1 but node not enough -> error
	_, errLoad = mc.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName).WithReplica(3).WithResourceGroup(rgName))
	common.CheckErr(t, errLoad, false, "resource group node not enough")

	// replicas:rg = n:1 and node enough -> search
	_, errLoad = mc.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName).WithReplica(2).WithResourceGroup(rgName))
	common.CheckErr(t, errLoad, true)
	for i := 0; i < 10; i++ {
		vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
		searchRes, _ := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors))
		common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	}

	err := mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(schema.CollectionName))
	common.CheckErr(t, err, true)

	// replicas:rg = 2n:n -> error
	_, errLoad = mc.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName).WithReplica(4).WithResourceGroup(rgName, common.DefaultRgName))
	common.CheckErr(t, errLoad, false, "resource group num can only be 0, 1 or same as replica number")

	// replicas:rg = n+1:n -> error
	_, errLoad = mc.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName).WithReplica(3).WithResourceGroup(rgName, common.DefaultRgName))
	common.CheckErr(t, errLoad, false, "resource group num can only be 0, 1 or same as replica number")

	// replicas:rg = n:n+1 -> error
	_, errLoad = mc.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName).WithReplica(1).WithResourceGroup(rgName, common.DefaultRgName))
	common.CheckErr(t, errLoad, false, "resource group num can only be 0, 1 or same as replica number")

	// replicas:rg = n:n -> search
	_, errLoad = mc.LoadCollection(ctx, client.NewLoadCollectionOption(schema.CollectionName).WithReplica(2).WithResourceGroup(rgName, common.DefaultRgName))
	common.CheckErr(t, errLoad, true)
	for i := 0; i < 10; i++ {
		vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
		searchRes, _ := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors))
		common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	}
}

// Transfer the replica to the resource group where the replica is already loaded
func TestTransferReplicaLoadedRg(t *testing.T) {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)
	defer teardownTest(t, ctx, mc)

	// create new rg with requests 2
	rgName := common.GenRandomString("rg", 6)
	errCreate := mc.CreateResourceGroup(ctx, client.NewCreateResourceGroupOption(rgName).WithNodeRequest(newRgNode).WithNodeLimit(newRgNode))
	common.CheckErr(t, errCreate, true)

	// load two replicas with multi resource groups
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc, hp.NewCreateCollectionParams(hp.Int64Vec), hp.TNewFieldsOption(), hp.TNewSchemaOption())
	prepare.InsertData(ctx, t, mc, hp.NewInsertParams(schema), hp.TNewDataOption().TWithNb(common.DefaultNb))
	prepare.FlushData(ctx, t, mc, schema.CollectionName)
	prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName).TWithReplica(2).TWithResourceGroups(rgName, common.DefaultRgName))

	err := mc.TransferReplica(ctx, client.NewTransferReplicaOption(schema.CollectionName, common.DefaultRgName, rgName, 1))
	common.CheckErr(t, err, true)

	// search
	for i := 0; i < 10; i++ {
		vectors := hp.GenSearchVectors(common.DefaultNq, common.DefaultDim, entity.FieldTypeFloatVector)
		searchRes, _ := mc.Search(ctx, client.NewSearchOption(schema.CollectionName, common.DefaultLimit, vectors))
		common.CheckSearchResult(t, searchRes, common.DefaultNq, common.DefaultLimit)
	}
}
