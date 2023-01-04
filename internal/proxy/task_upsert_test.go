// // Licensed to the LF AI & Data foundation under one
// // or more contributor license agreements. See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership. The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License. You may obtain a copy of the License at
// //
// //	http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing, software
// // distributed under the License is distributed on an "AS IS" BASIS,
// // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// // See the License for the specific language governing permissions and
// // limitations under the License.
package proxy

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/commonpbutil"
	"github.com/stretchr/testify/assert"
)

func TestUpsertTask_CheckAligned(t *testing.T) {
	var err error

	// passed NumRows is less than 0
	case1 := upsertTask{
		req: &milvuspb.UpsertRequest{
			NumRows: 0,
		},
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: internalpb.InsertRequest{},
			},
		},
	}
	case1.upsertMsg.InsertMsg.InsertRequest = internalpb.InsertRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Insert),
		),
		CollectionName: case1.req.CollectionName,
		PartitionName:  case1.req.PartitionName,
		FieldsData:     case1.req.FieldsData,
		NumRows:        uint64(case1.req.NumRows),
		Version:        internalpb.InsertDataVersion_ColumnBased,
	}

	err = case1.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// checkLengthOfFieldsData was already checked by TestUpsertTask_checkLengthOfFieldsData

	boolFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Bool}
	int8FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int8}
	int16FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int16}
	int32FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int32}
	int64FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}
	floatFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Float}
	doubleFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Double}
	floatVectorFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector}
	binaryVectorFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_BinaryVector}
	varCharFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}

	numRows := 20
	dim := 128
	case2 := upsertTask{
		req: &milvuspb.UpsertRequest{
			NumRows:    uint32(numRows),
			FieldsData: []*schemapb.FieldData{},
		},
		rowIDs:     generateInt64Array(numRows),
		timestamps: generateUint64Array(numRows),
		schema: &schemapb.CollectionSchema{
			Name:        "TestUpsertTask_checkRowNums",
			Description: "TestUpsertTask_checkRowNums",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				boolFieldSchema,
				int8FieldSchema,
				int16FieldSchema,
				int32FieldSchema,
				int64FieldSchema,
				floatFieldSchema,
				doubleFieldSchema,
				floatVectorFieldSchema,
				binaryVectorFieldSchema,
				varCharFieldSchema,
			},
		},
		upsertMsg: &msgstream.UpsertMsg{
			InsertMsg: &msgstream.InsertMsg{
				InsertRequest: internalpb.InsertRequest{},
			},
		},
	}

	// satisfied
	case2.req.FieldsData = []*schemapb.FieldData{
		newScalarFieldData(boolFieldSchema, "Bool", numRows),
		newScalarFieldData(int8FieldSchema, "Int8", numRows),
		newScalarFieldData(int16FieldSchema, "Int16", numRows),
		newScalarFieldData(int32FieldSchema, "Int32", numRows),
		newScalarFieldData(int64FieldSchema, "Int64", numRows),
		newScalarFieldData(floatFieldSchema, "Float", numRows),
		newScalarFieldData(doubleFieldSchema, "Double", numRows),
		newFloatVectorFieldData("FloatVector", numRows, dim),
		newBinaryVectorFieldData("BinaryVector", numRows, dim),
		newScalarFieldData(varCharFieldSchema, "VarChar", numRows),
	}
	case2.upsertMsg.InsertMsg.InsertRequest = internalpb.InsertRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Insert),
		),
		CollectionName: case2.req.CollectionName,
		PartitionName:  case2.req.PartitionName,
		FieldsData:     case2.req.FieldsData,
		NumRows:        uint64(case2.req.NumRows),
		RowIDs:         case2.rowIDs,
		Timestamps:     case2.timestamps,
		Version:        internalpb.InsertDataVersion_ColumnBased,
	}
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less bool data
	case2.req.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more bool data
	case2.req.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int8 data
	case2.req.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more int8 data
	case2.req.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int16 data
	case2.req.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more int16 data
	case2.req.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int32 data
	case2.req.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more int32 data
	case2.req.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int64 data
	case2.req.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more int64 data
	case2.req.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less float data
	case2.req.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more float data
	case2.req.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, nil, err)

	// less double data
	case2.req.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more double data
	case2.req.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, nil, err)

	// less float vectors
	case2.req.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows/2, dim)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more float vectors
	case2.req.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows*2, dim)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows, dim)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less binary vectors
	case2.req.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows/2, dim)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more binary vectors
	case2.req.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows*2, dim)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows, dim)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)

	// less double data
	case2.req.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows/2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// more double data
	case2.req.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows*2)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.req.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows)
	case2.upsertMsg.InsertMsg.FieldsData = case2.req.FieldsData
	err = case2.upsertMsg.InsertMsg.CheckAligned()
	assert.NoError(t, err)
}

// func TestProxyUpsertValid(t *testing.T) {
// 	var err error
// 	var wg sync.WaitGroup
// 	paramtable.Init()

// 	path := "/tmp/milvus/rocksmq" + funcutil.GenRandomStr()
// 	t.Setenv("ROCKSMQ_PATH", path)
// 	defer os.RemoveAll(path)

// 	ctx, cancel := context.WithCancel(context.Background())
// 	ctx = GetContext(ctx, "root:123456")
// 	localMsg := true
// 	factory := dependency.NewDefaultFactory(localMsg)
// 	alias := "TestProxyUpsertValid"

// 	log.Info("Initialize parameter table of Proxy")

// 	rc := runRootCoord(ctx, localMsg)
// 	log.Info("running RootCoord ...")

// 	if rc != nil {
// 		defer func() {
// 			err := rc.Stop()
// 			assert.NoError(t, err)
// 			log.Info("stop RootCoord")
// 		}()
// 	}

// 	dc := runDataCoord(ctx, localMsg)
// 	log.Info("running DataCoord ...")

// 	if dc != nil {
// 		defer func() {
// 			err := dc.Stop()
// 			assert.NoError(t, err)
// 			log.Info("stop DataCoord")
// 		}()
// 	}

// 	dn := runDataNode(ctx, localMsg, alias)
// 	log.Info("running DataNode ...")

// 	if dn != nil {
// 		defer func() {
// 			err := dn.Stop()
// 			assert.NoError(t, err)
// 			log.Info("stop DataNode")
// 		}()
// 	}

// 	qc := runQueryCoord(ctx, localMsg)
// 	log.Info("running QueryCoord ...")

// 	if qc != nil {
// 		defer func() {
// 			err := qc.Stop()
// 			assert.NoError(t, err)
// 			log.Info("stop QueryCoord")
// 		}()
// 	}

// 	qn := runQueryNode(ctx, localMsg, alias)
// 	log.Info("running QueryNode ...")

// 	if qn != nil {
// 		defer func() {
// 			err := qn.Stop()
// 			assert.NoError(t, err)
// 			log.Info("stop query node")
// 		}()
// 	}

// 	ic := runIndexCoord(ctx, localMsg)
// 	log.Info("running IndexCoord ...")

// 	if ic != nil {
// 		defer func() {
// 			err := ic.Stop()
// 			assert.NoError(t, err)
// 			log.Info("stop IndexCoord")
// 		}()
// 	}

// 	in := runIndexNode(ctx, localMsg, alias)
// 	log.Info("running IndexNode ...")

// 	if in != nil {
// 		defer func() {
// 			err := in.Stop()
// 			assert.NoError(t, err)
// 			log.Info("stop IndexNode")
// 		}()
// 	}

// 	time.Sleep(10 * time.Millisecond)

// 	proxy, err := NewProxy(ctx, factory)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, proxy)

// 	etcdcli, err := etcd.GetEtcdClient(
// 		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
// 		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
// 		Params.EtcdCfg.Endpoints.GetAsStrings(),
// 		Params.EtcdCfg.EtcdTLSCert.GetValue(),
// 		Params.EtcdCfg.EtcdTLSKey.GetValue(),
// 		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
// 		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
// 	defer etcdcli.Close()
// 	assert.NoError(t, err)
// 	proxy.SetEtcdClient(etcdcli)

// 	testServer := newProxyTestServer(proxy)
// 	wg.Add(1)

// 	base := paramtable.BaseTable{}
// 	base.Init(0)
// 	var p paramtable.GrpcServerConfig
// 	p.Init(typeutil.ProxyRole, &base)

// 	go testServer.startGrpc(ctx, &wg, &p)
// 	assert.NoError(t, testServer.waitForGrpcReady())

// 	rootCoordClient, err := rcc.NewClient(ctx, Params.EtcdCfg.MetaRootPath.GetValue(), etcdcli)
// 	assert.NoError(t, err)
// 	err = rootCoordClient.Init()
// 	assert.NoError(t, err)
// 	err = funcutil.WaitForComponentHealthy(ctx, rootCoordClient, typeutil.RootCoordRole, attempts, sleepDuration)
// 	assert.NoError(t, err)
// 	proxy.SetRootCoordClient(rootCoordClient)
// 	log.Info("Proxy set root coordinator client")

// 	dataCoordClient, err := grpcdatacoordclient2.NewClient(ctx, Params.EtcdCfg.MetaRootPath.GetValue(), etcdcli)
// 	assert.NoError(t, err)
// 	err = dataCoordClient.Init()
// 	assert.NoError(t, err)
// 	err = funcutil.WaitForComponentHealthy(ctx, dataCoordClient, typeutil.DataCoordRole, attempts, sleepDuration)
// 	assert.NoError(t, err)
// 	proxy.SetDataCoordClient(dataCoordClient)
// 	log.Info("Proxy set data coordinator client")

// 	queryCoordClient, err := grpcquerycoordclient.NewClient(ctx, Params.EtcdCfg.MetaRootPath.GetValue(), etcdcli)
// 	assert.NoError(t, err)
// 	err = queryCoordClient.Init()
// 	assert.NoError(t, err)
// 	err = funcutil.WaitForComponentHealthy(ctx, queryCoordClient, typeutil.QueryCoordRole, attempts, sleepDuration)
// 	assert.NoError(t, err)
// 	proxy.SetQueryCoordClient(queryCoordClient)
// 	log.Info("Proxy set query coordinator client")

// 	indexCoordClient, err := grpcindexcoordclient.NewClient(ctx, Params.EtcdCfg.MetaRootPath.GetValue(), etcdcli)
// 	assert.NoError(t, err)
// 	err = indexCoordClient.Init()
// 	assert.NoError(t, err)
// 	err = funcutil.WaitForComponentHealthy(ctx, indexCoordClient, typeutil.IndexCoordRole, attempts, sleepDuration)
// 	assert.NoError(t, err)
// 	proxy.SetIndexCoordClient(indexCoordClient)
// 	log.Info("Proxy set index coordinator client")

// 	proxy.UpdateStateCode(commonpb.StateCode_Initializing)
// 	err = proxy.Init()
// 	assert.NoError(t, err)

// 	err = proxy.Start()
// 	assert.NoError(t, err)
// 	assert.Equal(t, commonpb.StateCode_Healthy, proxy.stateCode.Load().(commonpb.StateCode))

// 	// register proxy
// 	err = proxy.Register()
// 	assert.NoError(t, err)
// 	log.Info("Register proxy done")
// 	defer func() {
// 		err := proxy.Stop()
// 		assert.NoError(t, err)
// 	}()

// 	prefix := "test_proxy_"
// 	partitionPrefix := "test_proxy_partition_"
// 	dbName := ""
// 	collectionName := prefix + funcutil.GenRandomStr()
// 	otherCollectionName := collectionName + "_other_" + funcutil.GenRandomStr()
// 	partitionName := partitionPrefix + funcutil.GenRandomStr()
// 	// otherPartitionName := partitionPrefix + "_other_" + funcutil.GenRandomStr()
// 	shardsNum := int32(2)
// 	int64Field := "int64"
// 	floatVecField := "fVec"
// 	dim := 128
// 	rowNum := 30
// 	// indexName := "_default"
// 	// nlist := 10
// 	// nprobe := 10
// 	// topk := 10
// 	// add a test parameter
// 	// roundDecimal := 6
// 	// nq := 10
// 	// expr := fmt.Sprintf("%s > 0", int64Field)
// 	// var segmentIDs []int64

// 	constructCollectionSchema := func() *schemapb.CollectionSchema {
// 		pk := &schemapb.FieldSchema{
// 			FieldID:      0,
// 			Name:         int64Field,
// 			IsPrimaryKey: true,
// 			Description:  "",
// 			DataType:     schemapb.DataType_Int64,
// 			TypeParams:   nil,
// 			IndexParams:  nil,
// 			AutoID:       false,
// 		}
// 		fVec := &schemapb.FieldSchema{
// 			FieldID:      0,
// 			Name:         floatVecField,
// 			IsPrimaryKey: false,
// 			Description:  "",
// 			DataType:     schemapb.DataType_FloatVector,
// 			TypeParams: []*commonpb.KeyValuePair{
// 				{
// 					Key:   "dim",
// 					Value: strconv.Itoa(dim),
// 				},
// 			},
// 			IndexParams: nil,
// 			AutoID:      false,
// 		}
// 		return &schemapb.CollectionSchema{
// 			Name:        collectionName,
// 			Description: "",
// 			AutoID:      false,
// 			Fields: []*schemapb.FieldSchema{
// 				pk,
// 				fVec,
// 			},
// 		}
// 	}
// 	schema := constructCollectionSchema()

// 	constructCreateCollectionRequest := func() *milvuspb.CreateCollectionRequest {
// 		bs, err := proto.Marshal(schema)
// 		assert.NoError(t, err)
// 		return &milvuspb.CreateCollectionRequest{
// 			Base:           nil,
// 			DbName:         dbName,
// 			CollectionName: collectionName,
// 			Schema:         bs,
// 			ShardsNum:      shardsNum,
// 		}
// 	}
// 	createCollectionReq := constructCreateCollectionRequest()

// 	constructPartitionReqUpsertRequestValid := func() *milvuspb.UpsertRequest {
// 		pkFieldData := newScalarFieldData(schema.Fields[0], int64Field, rowNum)
// 		fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
// 		hashKeys := generateHashKeys(rowNum)
// 		return &milvuspb.UpsertRequest{
// 			Base:           nil,
// 			DbName:         dbName,
// 			CollectionName: collectionName,
// 			PartitionName:  partitionName,
// 			FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn},
// 			HashKeys:       hashKeys,
// 			NumRows:        uint32(rowNum),
// 		}
// 	}

// 	constructCollectionUpsertRequestValid := func() *milvuspb.UpsertRequest {
// 		pkFieldData := newScalarFieldData(schema.Fields[0], int64Field, rowNum)
// 		fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
// 		hashKeys := generateHashKeys(rowNum)
// 		return &milvuspb.UpsertRequest{
// 			Base:           nil,
// 			DbName:         dbName,
// 			CollectionName: collectionName,
// 			PartitionName:  partitionName,
// 			FieldsData:     []*schemapb.FieldData{pkFieldData, fVecColumn},
// 			HashKeys:       hashKeys,
// 			NumRows:        uint32(rowNum),
// 		}
// 	}

// 	wg.Add(1)
// 	t.Run("create collection upsert valid", func(t *testing.T) {
// 		defer wg.Done()
// 		req := createCollectionReq
// 		resp, err := proxy.CreateCollection(ctx, req)
// 		assert.NoError(t, err)
// 		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

// 		reqInvalidField := constructCreateCollectionRequest()
// 		schema := constructCollectionSchema()
// 		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
// 			Name:     "StringField",
// 			DataType: schemapb.DataType_String,
// 		})
// 		bs, err := proto.Marshal(schema)
// 		assert.NoError(t, err)
// 		reqInvalidField.CollectionName = "invalid_field_coll_upsert_valid"
// 		reqInvalidField.Schema = bs

// 		resp, err = proxy.CreateCollection(ctx, reqInvalidField)
// 		assert.NoError(t, err)
// 		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

// 	})

// 	wg.Add(1)
// 	t.Run("create partition", func(t *testing.T) {
// 		defer wg.Done()
// 		resp, err := proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
// 			Base:           nil,
// 			DbName:         dbName,
// 			CollectionName: collectionName,
// 			PartitionName:  partitionName,
// 		})
// 		assert.NoError(t, err)
// 		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

// 		// create partition with non-exist collection -> fail
// 		resp, err = proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
// 			Base:           nil,
// 			DbName:         dbName,
// 			CollectionName: otherCollectionName,
// 			PartitionName:  partitionName,
// 		})
// 		assert.NoError(t, err)
// 		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
// 	})

// 	wg.Add(1)
// 	t.Run("upsert partition", func(t *testing.T) {
// 		defer wg.Done()
// 		req := constructPartitionReqUpsertRequestValid()

// 		resp, err := proxy.Upsert(ctx, req)
// 		assert.NoError(t, err)
// 		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
// 		assert.Equal(t, rowNum, len(resp.SuccIndex))
// 		assert.Equal(t, 0, len(resp.ErrIndex))
// 		assert.Equal(t, int64(rowNum), resp.UpsertCnt)
// 	})

// 	wg.Add(1)
// 	t.Run("upsert when autoID == false", func(t *testing.T) {
// 		defer wg.Done()
// 		req := constructCollectionUpsertRequestValid()

// 		resp, err := proxy.Upsert(ctx, req)
// 		assert.NoError(t, err)
// 		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
// 		assert.Equal(t, rowNum, len(resp.SuccIndex))
// 		assert.Equal(t, 0, len(resp.ErrIndex))
// 		assert.Equal(t, int64(rowNum), resp.UpsertCnt)
// 	})

// 	proxy.UpdateStateCode(commonpb.StateCode_Abnormal)

// 	wg.Add(1)
// 	t.Run("Upsert fail, unhealthy", func(t *testing.T) {
// 		defer wg.Done()
// 		resp, err := proxy.Upsert(ctx, &milvuspb.UpsertRequest{})
// 		assert.NoError(t, err)
// 		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
// 	})

// 	dmParallelism := proxy.sched.dmQueue.getMaxTaskNum()
// 	proxy.sched.dmQueue.setMaxTaskNum(0)

// 	wg.Add(1)
// 	t.Run("Upsert fail, dm queue full", func(t *testing.T) {
// 		defer wg.Done()
// 		resp, err := proxy.Upsert(ctx, &milvuspb.UpsertRequest{})
// 		assert.NoError(t, err)
// 		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
// 	})
// 	proxy.sched.dmQueue.setMaxTaskNum(dmParallelism)

// 	timeout := time.Nanosecond
// 	shortCtx, shortCancel := context.WithTimeout(ctx, timeout)
// 	defer shortCancel()
// 	time.Sleep(timeout)

// 	wg.Add(1)
// 	t.Run("Update fail, timeout", func(t *testing.T) {
// 		defer wg.Done()
// 		resp, err := proxy.Upsert(shortCtx, &milvuspb.UpsertRequest{})
// 		assert.NoError(t, err)
// 		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
// 	})

// 	testServer.gracefulStop()
// 	wg.Wait()
// 	cancel()
// }
