package test

import (
	"encoding/json"
	"github.com/milvus-io/milvus-sdk-go/milvus"
	"testing"
)

var TABLENAME string = "go_test"
var client milvus.MilvusClient = GetClient()

type segmentsInfo struct {
	DataSize  int64  `json:"data_size"`
	IndexName string `json:"index_name"`
	Name      string `json:"name"`
	RowCount  int64  `json:"row_count"`
}

type partitionsInfo struct {
	RowCount int64          `json:"row_count"`
	Segments []segmentsInfo `json:"segments"`
	Tag      string         `json:"tag"`
}

type collectionInfo struct {
	Partitions []partitionsInfo `json:"partitions"`
	RowCount   int64            `json:"row_count"`
}

func GetClient() milvus.MilvusClient {
	var grpcClient milvus.Milvusclient
	client := milvus.NewMilvusClient(grpcClient.Instance)
	connectParam := milvus.ConnectParam{"127.0.0.1", "19530"}
	err := client.Connect(connectParam)
	if err != nil {
		println("Connect failed")
		return nil
	}
	return client
}

func CreateCollection() error {
	boolReply, status, err := client.HasCollection(TABLENAME)
	if boolReply == true {
		return err
	}

	collectionParam := milvus.CollectionParam{TABLENAME, 128, 1024, int64(milvus.L2)}
	status, err = client.CreateCollection(collectionParam)
	if err != nil {
		return err
	}
	if !status.Ok() {
		println("CreateCollection failed")
	}
	return err
}

func TestConnection(t *testing.T) {
	var grpcClient milvus.Milvusclient
	testClient := milvus.NewMilvusClient(grpcClient.Instance)
	connectParam := milvus.ConnectParam{"127.0.0.1", "19530"}
	err := testClient.Connect(connectParam)
	if err != nil {
		t.Error("Connect error: " + err.Error())
	}

	// test wrong uri connect
	connectParam = milvus.ConnectParam{"12345", "111"}
	err = testClient.Connect(connectParam)
	if err == nil {
		t.Error("Use wrong uri to connect, return true")
	}
}

func TestCollection(t *testing.T) {
	param := milvus.CollectionParam{"test_1", 128, 1024, int64(milvus.L2)}
	status, err := client.CreateCollection(param)
	if err != nil {
		t.Error("CreateCollection error")
	}

	if !status.Ok() {
		t.Error("CreateCollection return status wrong!")
	}

	// test ShowCollections
	collections, status, err := client.ListCollections()
	if err != nil {
		t.Error("ShowCollections error")
		return
	}
	if !status.Ok() {
		t.Error("ShowCollections status check error")
	}
	if len(collections) != 1 && collections[0] != TABLENAME {
		t.Error("ShowCollections result check error")
	}

	// test normal hascollection
	hasCollection, status, err := client.HasCollection("test_1")
	if err != nil {
		t.Error("HasCollection error")
		return
	}

	if !status.Ok() {
		t.Error("HasCollection status check error")
	}

	if !hasCollection {
		t.Error("HasCollection result check error")
	}

	// test HasCollection with collection not exist
	hasCollection, status, err = client.HasCollection("aaa")
	if err != nil {
		t.Error("HasCollection error")
	}

	if hasCollection == true {
		t.Error("HasCollection result check error")
	}

	// test DropCollection
	status, err = client.DropCollection("test_1")
	if err != nil {
		t.Error("DropCollection error")
	}

	if !status.Ok() {
		t.Error("DropCollection status check error")
	}

	hasCollection, status, err = client.HasCollection("test_1")
	if hasCollection == true {
		t.Error("DropCollection result check error")
	}

	// test DropCollection with collection not exist
	status, err = client.DropCollection("aaa")
	if err != nil {
		t.Error("DropCollection error")
	}

	if status.Ok() {
		t.Error("DropCollection status check error")
	}
}

func TestEntity(t *testing.T) {
	err := CreateCollection()
	if err != nil {
		t.Error("Create collection error")
	}
	// test insert
	var i, j int

	nb := 10000
	dimension := 128
	records := make([]milvus.Entity, nb)
	recordArray := make([][]float32, 10000)
	for i = 0; i < nb; i++ {
		recordArray[i] = make([]float32, dimension)
		for j = 0; j < dimension; j++ {
			recordArray[i][j] = float32(i % (j + 1))
		}
		records[i].FloatData = recordArray[i]
	}
	insertParam := milvus.InsertParam{TABLENAME, "", records, nil}
	ret_id_array, status, err := client.Insert(&insertParam)
	if err != nil {
		t.Error("Insert error")
		return
	}

	if ret_id_array == nil {
		t.Error("Insert vecto result check error")
	}

	if !status.Ok() {
		t.Error("Insert status check error")
	}

	// Flush
	collection_array := make([]string, 1)
	collection_array[0] = TABLENAME
	status, err = client.Flush(collection_array)
	if err != nil {
		t.Error("Flush error")
		return
	}
	if !status.Ok() {
		t.Error("Flush status check error")
	}

	// test ShowCollectionInfos
	collectionInfos, status, err := client.GetCollectionStats(TABLENAME)
	if err != nil {
		t.Error("ShowCollectionInfo error")
		return
	}

	if !status.Ok() {
		t.Error("ShowCollectionInfo status check error")
	}

	if len(collectionInfos) == 0 {
		t.Error("ShowCollectionInfo result check error")
	}

	collections := collectionInfo{}
	if err = json.Unmarshal([]byte(collectionInfos), &collections); err != nil {
		panic(err)
	}

	segmentName := collections.Partitions[0].Segments[0].Name

	// test GetEntityIds
	listIDInSegment := milvus.ListIDInSegmentParam{TABLENAME, string(segmentName)}
	entityIDs, status, err := client.ListIDInSegment(listIDInSegment)
	if err != nil {
		t.Error("GetEntityIDs error")
		return
	}

	if len(entityIDs) == 0 {
		t.Error("GetEntityIDs result check error")
	}

	// test GetEntityById
	rowRecord, status, err := client.GetEntityByID(TABLENAME, entityIDs[0:10])
	if err != nil {
		t.Error("GetEntityByID error")
		return
	}
	if !status.Ok() {
		t.Error("GetEntitiesByID status check error: ", status.GetMessage())
	}
	if len(rowRecord) == 0 {
		t.Error("GetEntityiesByID result check error: ", len(rowRecord))
	}

	// test DeleteByID
	id_array := make([]int64, 1)
	id_array[0] = entityIDs[0]
	status, err = client.DeleteEntityByID(TABLENAME, id_array)
	if err != nil {
		t.Error("DeleteByID error")
		return
	}
	if !status.Ok() {
		t.Error("DeleteByID status check error")
	}
}

func TestIndex(t *testing.T) {
	extraParam := "{\"nlist\" : 16384}"
	indexParam := milvus.IndexParam{TABLENAME, milvus.IVFFLAT, extraParam}
	status, err := client.CreateIndex(&indexParam)
	if err != nil {
		t.Error("CreateIndex error")
	}
	if !status.Ok() {
		t.Error("CreateIndex status check error")
	}

	// test DescribeIndex
	indexInfo, status, err := client.GetIndexInfo(TABLENAME)
	if err != nil {
		t.Error("DescribeIndex error")
		return
	}
	if !status.Ok() {
		t.Error("DescribeIndex status check error")
	}
	if indexInfo.CollectionName != TABLENAME || indexInfo.IndexType != milvus.IVFFLAT {
		t.Error("DescribeIndex result chck error")
	}

	// test DropIndex
	status, err = client.DropIndex(TABLENAME)
	if err != nil {
		t.Error("DropIndex error")
		return
	}

	if !status.Ok() {
		t.Error("DropIndex status check erro")
	}

	status, err = client.CreateIndex(&indexParam)
	if err != nil {
		t.Error("CreateIndex error")
	}
	if !status.Ok() {
		t.Error("CreateIndex status check error")
	}
}

func TestSearch(t *testing.T) {
	var i, j int
	//Construct query entities
	nq := 10
	dimension := 128
	topk := 10
	queryRecords := make([]milvus.Entity, nq)
	queryEntitys := make([][]float32, nq)
	for i = 0; i < nq; i++ {
		queryEntitys[i] = make([]float32, dimension)
		for j = 0; j < dimension; j++ {
			queryEntitys[i][j] = float32(i % (j + 1))
		}
		queryRecords[i].FloatData = queryEntitys[i]
	}

	var topkQueryResult milvus.TopkQueryResult
	extraParam := "{\"nprobe\" : 32}"
	searchParam := milvus.SearchParam{TABLENAME, queryRecords, int64(topk), nil, extraParam}
	topkQueryResult, status, err := client.Search(searchParam)
	if err != nil {
		t.Error(err.Error())
	}
	if !status.Ok() {
		t.Error("Search status check error")
	}
	if len(topkQueryResult.QueryResultList) != nq {
		t.Error("Search result check error")
	}
}

func TestCmd(t *testing.T) {
	// test ServerStatus
	serverStatus, status, err := client.ServerStatus()
	if err != nil {
		t.Error("ServerStatus error")
		return
	}
	if !status.Ok() {
		t.Error("ServerStatus status check error")
	}
	if serverStatus != "server alive" {
		t.Error("ServerStatus result check error: " + serverStatus)
	}

	// test ServerVersion
	serverVersion, status, err := client.ServerVersion()
	if err != nil {
		t.Error("ServerVersion error")
		return
	}
	if !status.Ok() {
		t.Error("ServerVersion status check error")
	}
	if len(serverVersion) == 0 {
		t.Error("ServerVersion result check error")
	}

	// test SetConfig and GetConfig
<<<<<<< HEAD
	nodeName := "cache.cache_size"
=======
	nodeName := "cache_config.cpu_cache_capacity"
>>>>>>> af8ea3cc1f1816f42e94a395ab9286dfceb9ceda
	nodeValue := "2"
	status, err = client.SetConfig(nodeName, nodeValue)
	if err != nil {
		t.Error("SetConfig error")
	}
	if !status.Ok() {
		t.Error("SetConfig status check error: " + status.GetMessage())
	}

	value, status, err := client.GetConfig(nodeName)
	if err != nil {
		t.Error("GetConfig error")
		return
	}
	if !status.Ok() {
		t.Error("GetConfig status check error")
	}
	if value != nodeValue {
		t.Error("GetConfig or SetConfig result check error")
	}
}

func TestPartition(t *testing.T) {
	// test CreatePartition
	partitionTag := "part_1"
	status, err := client.CreatePartition(milvus.PartitionParam{TABLENAME, partitionTag})
	if err != nil {
		t.Error("CreatePartition error")
		return
	}
	if !status.Ok() {
		t.Error("CreatePartition status check error")
	}

	// test ListPartitions
	partitionParam, status, err := client.ListPartitions(TABLENAME)
	if !status.Ok() {
		t.Error("ShowPartitions status check error")
	}
	if len(partitionParam) == 0 {
		t.Error("ShowPartitions result check error")
	}

	// test DropPartition
	status, err = client.DropPartition(milvus.PartitionParam{TABLENAME, partitionTag})
	if !status.Ok() {
		t.Error("DropPartition status check error")
	}
}

func TestFlush(t *testing.T) {
	collectionNameArray := make([]string, 1)
	collectionNameArray[0] = TABLENAME
	status, err := client.Flush(collectionNameArray)
	if err != nil {
		t.Error("Flush error")
	}
	if !status.Ok() {
		t.Error("Flush status check error")
	}
}

func TestCompact(t *testing.T) {
	status, err := client.Compact(TABLENAME)
	if err != nil {
		t.Error("Compact error")
		return
	}
	if !status.Ok() {
		t.Error("Compact status check error")
	}
}
