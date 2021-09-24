package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"github.com/milvus-io/milvus/internal/util/distance"

	"github.com/milvus-io/milvus/internal/proto/proxypb"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"github.com/milvus-io/milvus/internal/proto/commonpb"

	"github.com/milvus-io/milvus/internal/proto/internalpb"

	grpcindexcoordclient "github.com/milvus-io/milvus/internal/distributed/indexcoord/client"

	grpcquerycoordclient "github.com/milvus-io/milvus/internal/distributed/querycoord/client"

	grpcdatacoordclient2 "github.com/milvus-io/milvus/internal/distributed/datacoord/client"

	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"

	rcc "github.com/milvus-io/milvus/internal/distributed/rootcoord/client"

	grpcindexnode "github.com/milvus-io/milvus/internal/distributed/indexnode"

	grpcindexcoord "github.com/milvus-io/milvus/internal/distributed/indexcoord"

	grpcdatacoordclient "github.com/milvus-io/milvus/internal/distributed/datacoord"
	grpcdatanode "github.com/milvus-io/milvus/internal/distributed/datanode"

	grpcquerynode "github.com/milvus-io/milvus/internal/distributed/querynode"

	grpcquerycoord "github.com/milvus-io/milvus/internal/distributed/querycoord"

	grpcrootcoord "github.com/milvus-io/milvus/internal/distributed/rootcoord"

	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/datanode"
	"github.com/milvus-io/milvus/internal/indexcoord"
	"github.com/milvus-io/milvus/internal/indexnode"
	"github.com/milvus-io/milvus/internal/querynode"

	"github.com/milvus-io/milvus/internal/querycoord"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/logutil"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/rootcoord"
)

const (
	attempts      = 1000000
	sleepDuration = time.Millisecond * 200
)

func newMsgFactory(localMsg bool) msgstream.Factory {
	if localMsg {
		return msgstream.NewRmsFactory()
	}
	return msgstream.NewPmsFactory()
}

func runRootCoord(ctx context.Context, localMsg bool) *grpcrootcoord.Server {
	var rc *grpcrootcoord.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		rootcoord.Params.Init()

		if !localMsg {
			logutil.SetupLogger(&rootcoord.Params.Log)
			defer log.Sync()
		}

		factory := newMsgFactory(localMsg)
		rc, err := grpcrootcoord.NewServer(ctx, factory)
		if err != nil {
			panic(err)
		}
		wg.Done()
		err = rc.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterRootCoord()
	return rc
}

func runQueryCoord(ctx context.Context, localMsg bool) *grpcquerycoord.Server {
	var qs *grpcquerycoord.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		querycoord.Params.Init()

		if !localMsg {
			logutil.SetupLogger(&querycoord.Params.Log)
			defer log.Sync()
		}

		factory := newMsgFactory(localMsg)
		var err error
		qs, err = grpcquerycoord.NewServer(ctx, factory)
		if err != nil {
			panic(err)
		}
		wg.Done()
		err = qs.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterQueryCoord()
	return qs
}

func runQueryNode(ctx context.Context, localMsg bool, alias string) *grpcquerynode.Server {
	var qn *grpcquerynode.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		querynode.Params.InitAlias(alias)
		querynode.Params.Init()

		if !localMsg {
			logutil.SetupLogger(&querynode.Params.Log)
			defer log.Sync()
		}

		factory := newMsgFactory(localMsg)
		var err error
		qn, err = grpcquerynode.NewServer(ctx, factory)
		if err != nil {
			panic(err)
		}
		wg.Done()
		err = qn.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterQueryNode()
	return qn
}

func runDataCoord(ctx context.Context, localMsg bool) *grpcdatacoordclient.Server {
	var ds *grpcdatacoordclient.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		datacoord.Params.Init()

		if !localMsg {
			logutil.SetupLogger(&datacoord.Params.Log)
			defer log.Sync()
		}

		factory := newMsgFactory(localMsg)
		var err error
		ds, err = grpcdatacoordclient.NewServer(ctx, factory)
		if err != nil {
			panic(err)
		}
		wg.Done()
		err = ds.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterDataCoord()
	return ds
}

func runDataNode(ctx context.Context, localMsg bool, alias string) *grpcdatanode.Server {
	var dn *grpcdatanode.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		datanode.Params.InitAlias(alias)
		datanode.Params.Init()

		if !localMsg {
			logutil.SetupLogger(&datanode.Params.Log)
			defer log.Sync()
		}

		factory := newMsgFactory(localMsg)
		var err error
		dn, err = grpcdatanode.NewServer(ctx, factory)
		if err != nil {
			panic(err)
		}
		wg.Done()
		err = dn.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterDataNode()
	return dn
}

func runIndexCoord(ctx context.Context, localMsg bool) *grpcindexcoord.Server {
	var is *grpcindexcoord.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		indexcoord.Params.Init()

		if !localMsg {
			logutil.SetupLogger(&indexcoord.Params.Log)
			defer log.Sync()
		}

		var err error
		is, err = grpcindexcoord.NewServer(ctx)
		if err != nil {
			panic(err)
		}
		wg.Done()
		err = is.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterIndexCoord()
	return is
}

func runIndexNode(ctx context.Context, localMsg bool, alias string) *grpcindexnode.Server {
	var in *grpcindexnode.Server
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		indexnode.Params.InitAlias(alias)
		indexnode.Params.Init()

		if !localMsg {
			logutil.SetupLogger(&indexnode.Params.Log)
			defer log.Sync()
		}

		var err error
		in, err = grpcindexnode.NewServer(ctx)
		if err != nil {
			panic(err)
		}
		wg.Done()
		err = in.Run()
		if err != nil {
			panic(err)
		}
	}()
	wg.Wait()

	metrics.RegisterIndexNode()
	return in
}

func TestProxy(t *testing.T) {
	var err error

	err = os.Setenv("ROCKSMQ_PATH", "/tmp/milvus/rocksmq")
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	localMsg := true
	factory := newMsgFactory(localMsg)
	alias := "TestProxy"

	rc := runRootCoord(ctx, localMsg)
	log.Info("running root coordinator ...")

	if rc != nil {
		defer func() {
			err := rc.Stop()
			assert.NoError(t, err)
			log.Info("stop root coordinator")
		}()
	}

	dc := runDataCoord(ctx, localMsg)
	log.Info("running data coordinator ...")

	if dc != nil {
		defer func() {
			err := dc.Stop()
			assert.NoError(t, err)
			log.Info("stop data coordinator")
		}()
	}

	dn := runDataNode(ctx, localMsg, alias)
	log.Info("running data node ...")

	if dn != nil {
		defer func() {
			err := dn.Stop()
			assert.NoError(t, err)
			log.Info("stop data node")
		}()
	}

	qc := runQueryCoord(ctx, localMsg)
	log.Info("running query coordinator ...")

	if qc != nil {
		defer func() {
			err := qc.Stop()
			assert.NoError(t, err)
			log.Info("stop query coordinator")
		}()
	}

	qn := runQueryNode(ctx, localMsg, alias)
	log.Info("running query node ...")

	if qn != nil {
		defer func() {
			err := qn.Stop()
			assert.NoError(t, err)
			log.Info("stop query node")
		}()
	}

	ic := runIndexCoord(ctx, localMsg)
	log.Info("running index coordinator ...")

	if ic != nil {
		defer func() {
			err := ic.Stop()
			assert.NoError(t, err)
			log.Info("stop index coordinator")
		}()
	}

	in := runIndexNode(ctx, localMsg, alias)
	log.Info("running index node ...")

	if in != nil {
		defer func() {
			err := in.Stop()
			assert.NoError(t, err)
			log.Info("stop index node")
		}()
	}

	time.Sleep(10 * time.Millisecond)

	proxy, err := NewProxy(ctx, factory)
	assert.NoError(t, err)
	assert.NotNil(t, proxy)
	Params.Init()
	log.Info("Initialize parameter table of proxy")

	// register proxy
	err = proxy.Register()
	assert.NoError(t, err)
	log.Info("Register proxy done")

	rootCoordClient, err := rcc.NewClient(ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	assert.NoError(t, err)
	err = rootCoordClient.Init()
	assert.NoError(t, err)
	err = funcutil.WaitForComponentHealthy(ctx, rootCoordClient, typeutil.RootCoordRole, attempts, sleepDuration)
	assert.NoError(t, err)
	proxy.SetRootCoordClient(rootCoordClient)
	log.Info("Proxy set root coordinator client")

	dataCoordClient, err := grpcdatacoordclient2.NewClient(ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	assert.NoError(t, err)
	err = dataCoordClient.Init()
	assert.NoError(t, err)
	err = funcutil.WaitForComponentHealthy(ctx, dataCoordClient, typeutil.DataCoordRole, attempts, sleepDuration)
	assert.NoError(t, err)
	proxy.SetDataCoordClient(dataCoordClient)
	log.Info("Proxy set data coordinator client")

	queryCoordClient, err := grpcquerycoordclient.NewClient(ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	assert.NoError(t, err)
	err = queryCoordClient.Init()
	assert.NoError(t, err)
	err = funcutil.WaitForComponentHealthy(ctx, queryCoordClient, typeutil.QueryCoordRole, attempts, sleepDuration)
	assert.NoError(t, err)
	proxy.SetQueryCoordClient(queryCoordClient)
	log.Info("Proxy set query coordinator client")

	indexCoordClient, err := grpcindexcoordclient.NewClient(ctx, Params.MetaRootPath, Params.EtcdEndpoints)
	assert.NoError(t, err)
	err = indexCoordClient.Init()
	assert.NoError(t, err)
	err = funcutil.WaitForComponentHealthy(ctx, indexCoordClient, typeutil.IndexCoordRole, attempts, sleepDuration)
	assert.NoError(t, err)
	proxy.SetIndexCoordClient(indexCoordClient)
	log.Info("Proxy set index coordinator client")

	proxy.UpdateStateCode(internalpb.StateCode_Initializing)

	err = proxy.Init()
	assert.NoError(t, err)

	err = proxy.Start()
	assert.NoError(t, err)
	assert.Equal(t, internalpb.StateCode_Healthy, proxy.stateCode.Load().(internalpb.StateCode))
	defer func() {
		err := proxy.Stop()
		assert.NoError(t, err)
	}()

	t.Run("get component states", func(t *testing.T) {
		states, err := proxy.GetComponentStates(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, states.Status.ErrorCode)
		assert.Equal(t, Params.ProxyID, states.State.NodeID)
		assert.Equal(t, typeutil.ProxyRole, states.State.Role)
		assert.Equal(t, proxy.stateCode.Load().(internalpb.StateCode), states.State.StateCode)
	})

	t.Run("get statistics channel", func(t *testing.T) {
		resp, err := proxy.GetStatisticsChannel(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, "", resp.Value)
	})

	prefix := "test_proxy_"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	otherCollectionName := collectionName + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()
	otherPartitionName := partitionName + funcutil.GenRandomStr()
	shardsNum := int32(2)
	int64Field := "int64"
	floatVecField := "fVec"
	dim := 128
	rowNum := 3000
	indexName := "_default"
	nlist := 10
	nprobe := 10
	topk := 10
	nq := 10
	expr := fmt.Sprintf("%s > 0", int64Field)
	var segmentIDs []int64

	// an int64 field (pk) & a float vector field
	constructCollectionSchema := func() *schemapb.CollectionSchema {
		pk := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         int64Field,
			IsPrimaryKey: true,
			Description:  "",
			DataType:     schemapb.DataType_Int64,
			TypeParams:   nil,
			IndexParams:  nil,
			AutoID:       true,
		}
		fVec := &schemapb.FieldSchema{
			FieldID:      0,
			Name:         floatVecField,
			IsPrimaryKey: false,
			Description:  "",
			DataType:     schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: strconv.Itoa(dim),
				},
			},
			IndexParams: nil,
			AutoID:      false,
		}
		return &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				pk,
				fVec,
			},
		}
	}
	schema := constructCollectionSchema()

	constructCreateCollectionRequest := func() *milvuspb.CreateCollectionRequest {
		bs, err := proto.Marshal(schema)
		assert.NoError(t, err)
		return &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         bs,
			ShardsNum:      shardsNum,
		}
	}
	createCollectionReq := constructCreateCollectionRequest()

	constructInsertRequest := func() *milvuspb.InsertRequest {
		fVecColumn := newFloatVectorFieldData(floatVecField, rowNum, dim)
		hashKeys := generateHashKeys(rowNum)
		return &milvuspb.InsertRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  "",
			FieldsData:     []*schemapb.FieldData{fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		}
	}

	constructCreateIndexRequest := func() *milvuspb.CreateIndexRequest {
		return &milvuspb.CreateIndexRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      floatVecField,
			ExtraParams: []*commonpb.KeyValuePair{
				{
					Key:   "dim",
					Value: strconv.Itoa(dim),
				},
				{
					Key:   MetricTypeKey,
					Value: distance.L2,
				},
				{
					Key:   "index_type",
					Value: "IVF_FLAT",
				},
				{
					Key:   "nlist",
					Value: strconv.Itoa(nlist),
				},
			},
		}
	}

	constructPlaceholderGroup := func() *milvuspb.PlaceholderGroup {
		values := make([][]byte, 0, nq)
		for i := 0; i < nq; i++ {
			bs := make([]byte, 0, dim*4)
			for j := 0; j < dim; j++ {
				var buffer bytes.Buffer
				f := rand.Float32()
				err := binary.Write(&buffer, binary.LittleEndian, f)
				assert.NoError(t, err)
				bs = append(bs, buffer.Bytes()...)
			}
			values = append(values, bs)
		}

		return &milvuspb.PlaceholderGroup{
			Placeholders: []*milvuspb.PlaceholderValue{
				{
					Tag:    "$0",
					Type:   milvuspb.PlaceholderType_FloatVector,
					Values: values,
				},
			},
		}
	}

	constructSearchRequest := func() *milvuspb.SearchRequest {
		params := make(map[string]string)
		params["nprobe"] = strconv.Itoa(nprobe)
		b, err := json.Marshal(params)
		assert.NoError(t, err)
		plg := constructPlaceholderGroup()
		plgBs, err := proto.Marshal(plg)
		assert.NoError(t, err)

		return &milvuspb.SearchRequest{
			Base:             nil,
			DbName:           dbName,
			CollectionName:   collectionName,
			PartitionNames:   nil,
			Dsl:              expr,
			PlaceholderGroup: plgBs,
			DslType:          commonpb.DslType_BoolExprV1,
			OutputFields:     nil,
			SearchParams: []*commonpb.KeyValuePair{
				{
					Key:   MetricTypeKey,
					Value: distance.L2,
				},
				{
					Key:   SearchParamsKey,
					Value: string(b),
				},
				{
					Key:   AnnsFieldKey,
					Value: floatVecField,
				},
				{
					Key:   TopKKey,
					Value: strconv.Itoa(topk),
				},
			},
			TravelTimestamp:    0,
			GuaranteeTimestamp: 0,
		}
	}

	t.Run("create collection", func(t *testing.T) {
		req := createCollectionReq
		resp, err := proxy.CreateCollection(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// recreate -> fail
		req2 := constructCreateCollectionRequest()
		resp, err = proxy.CreateCollection(ctx, req2)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("has collection", func(t *testing.T) {
		resp, err := proxy.HasCollection(ctx, &milvuspb.HasCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			TimeStamp:      0,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.True(t, resp.Value)

		// has other collection: false
		resp, err = proxy.HasCollection(ctx, &milvuspb.HasCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			TimeStamp:      0,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.False(t, resp.Value)
	})

	t.Run("describe collection", func(t *testing.T) {
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			TimeStamp:      0,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, collectionID, resp.CollectionID)
		// TODO(dragondriver): shards num
		assert.Equal(t, len(schema.Fields), len(resp.Schema.Fields))
		// TODO(dragondriver): compare fields schema, not sure the order of fields

		// describe other collection -> fail
		resp, err = proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			CollectionID:   collectionID,
			TimeStamp:      0,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("get collection statistics", func(t *testing.T) {
		resp, err := proxy.GetCollectionStatistics(ctx, &milvuspb.GetCollectionStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		// TODO(dragondriver): check num rows

		// get statistics of other collection -> fail
		resp, err = proxy.GetCollectionStatistics(ctx, &milvuspb.GetCollectionStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("show collections", func(t *testing.T) {
		resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:            nil,
			DbName:          dbName,
			TimeStamp:       0,
			Type:            milvuspb.ShowType_All,
			CollectionNames: nil,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 1, len(resp.CollectionNames))
	})

	t.Run("create partition", func(t *testing.T) {
		resp, err := proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// recreate -> fail
		resp, err = proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// create partition with non-exist collection -> fail
		resp, err = proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("has partition", func(t *testing.T) {
		resp, err := proxy.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.True(t, resp.Value)

		resp, err = proxy.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  otherPartitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.False(t, resp.Value)

		// non-exist collection -> fail
		resp, err = proxy.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("get partition statistics", func(t *testing.T) {
		resp, err := proxy.GetPartitionStatistics(ctx, &milvuspb.GetPartitionStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// non-exist partition -> fail
		resp, err = proxy.GetPartitionStatistics(ctx, &milvuspb.GetPartitionStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  otherPartitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// non-exist collection -> fail
		resp, err = proxy.GetPartitionStatistics(ctx, &milvuspb.GetPartitionStatisticsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("show partitions", func(t *testing.T) {
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionNames: nil,
			Type:           milvuspb.ShowType_All,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		// default partition
		assert.Equal(t, 2, len(resp.PartitionNames))

		// non-exist collection -> fail
		resp, err = proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			CollectionID:   collectionID + 1,
			PartitionNames: nil,
			Type:           milvuspb.ShowType_All,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("insert", func(t *testing.T) {
		req := constructInsertRequest()

		resp, err := proxy.Insert(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, rowNum, len(resp.SuccIndex))
		assert.Equal(t, 0, len(resp.ErrIndex))
		assert.Equal(t, int64(rowNum), resp.InsertCnt)
	})

	// TODO(dragondriver): proxy.Delete()

	flushed := true // fortunately, no task depends on this state, maybe CreateIndex?
	t.Run("flush", func(t *testing.T) {
		resp, err := proxy.Flush(ctx, &milvuspb.FlushRequest{
			Base:            nil,
			DbName:          dbName,
			CollectionNames: []string{collectionName},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		segmentIDs = resp.CollSegIDs[collectionName].Data

		f := func() bool {
			states := make(map[int64]commonpb.SegmentState) // segment id -> segment state
			for _, id := range segmentIDs {
				states[id] = commonpb.SegmentState_Sealed
			}
			resp, err := proxy.GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{
				Base:           nil,
				DbName:         dbName,
				CollectionName: collectionName,
			})
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
			for _, info := range resp.Infos {
				states[info.SegmentID] = info.State
			}
			for id, state := range states {
				if state != commonpb.SegmentState_Flushed {
					log.Debug("waiting for segment to be flushed",
						zap.Int64("segment id", id),
						zap.Any("state", state))
					return false
				}
			}
			return true
		}

		// waiting for flush operation to be done
		counter := 0
		for !f() {
			if counter > 10 {
				flushed = false
				break
			}
			// avoid too frequent rpc call
			time.Sleep(100 * time.Millisecond)
			counter++
		}
	})
	if !flushed {
		log.Warn("flush operation was not sure to be done")
	}

	t.Run("create index", func(t *testing.T) {
		req := constructCreateIndexRequest()

		resp, err := proxy.CreateIndex(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("describe index", func(t *testing.T) {
		resp, err := proxy.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      floatVecField,
			IndexName:      "",
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		indexName = resp.IndexDescriptions[0].IndexName
	})

	t.Run("get index build progress", func(t *testing.T) {
		resp, err := proxy.GetIndexBuildProgress(ctx, &milvuspb.GetIndexBuildProgressRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      floatVecField,
			IndexName:      indexName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("get index state", func(t *testing.T) {
		resp, err := proxy.GetIndexState(ctx, &milvuspb.GetIndexStateRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      floatVecField,
			IndexName:      indexName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	loaded := true
	t.Run("load collection", func(t *testing.T) {
		resp, err := proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// load other collection -> fail
		resp, err = proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		f := func() bool {
			resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
				Base:            nil,
				DbName:          dbName,
				TimeStamp:       0,
				Type:            milvuspb.ShowType_InMemory,
				CollectionNames: []string{collectionName},
			})
			assert.NoError(t, err)
			assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

			for idx, name := range resp.CollectionNames {
				if name == collectionName && resp.InMemoryPercentages[idx] == 100 {
					return true
				}
			}

			return false
		}

		// waiting for collection to be loaded
		counter := 0
		for !f() {
			if counter > 10 {
				loaded = false
				break
			}
			// avoid too frequent rpc call
			time.Sleep(100 * time.Millisecond)
			counter++
		}
	})
	if !loaded {
		log.Warn("load operation was not sure to be done")
	}

	t.Run("show in-memory collections", func(t *testing.T) {
		resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:            nil,
			DbName:          dbName,
			TimeStamp:       0,
			Type:            milvuspb.ShowType_InMemory,
			CollectionNames: nil,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 1, len(resp.CollectionNames))

		// get in-memory percentage
		resp, err = proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:            nil,
			DbName:          dbName,
			TimeStamp:       0,
			Type:            milvuspb.ShowType_InMemory,
			CollectionNames: []string{collectionName},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 1, len(resp.CollectionNames))
		assert.Equal(t, 1, len(resp.InMemoryPercentages))

		// get in-memory percentage of  not loaded collection -> fail
		resp, err = proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:            nil,
			DbName:          dbName,
			TimeStamp:       0,
			Type:            milvuspb.ShowType_InMemory,
			CollectionNames: []string{otherCollectionName},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	if loaded {
		t.Run("search", func(t *testing.T) {
			req := constructSearchRequest()

			//resp, err := proxy.Search(ctx, req)
			_, err := proxy.Search(ctx, req)
			assert.NoError(t, err)
			// FIXME(dragondriver)
			// assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
			// TODO(dragondriver): compare search result
		})

		t.Run("query", func(t *testing.T) {
			//resp, err := proxy.Query(ctx, &milvuspb.QueryRequest{
			_, err := proxy.Query(ctx, &milvuspb.QueryRequest{
				Base:               nil,
				DbName:             dbName,
				CollectionName:     collectionName,
				Expr:               expr,
				OutputFields:       nil,
				PartitionNames:     nil,
				TravelTimestamp:    0,
				GuaranteeTimestamp: 0,
			})
			assert.NoError(t, err)
			// FIXME(dragondriver)
			// assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
			// TODO(dragondriver): compare query result
		})
	}

	t.Run("calculate distance", func(t *testing.T) {
		opLeft := &milvuspb.VectorsArray{
			Array: &milvuspb.VectorsArray_DataArray{
				DataArray: &schemapb.VectorField{
					Dim: int64(dim),
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: generateFloatVectors(nq, dim),
						},
					},
				},
			},
		}

		opRight := &milvuspb.VectorsArray{
			Array: &milvuspb.VectorsArray_DataArray{
				DataArray: &schemapb.VectorField{
					Dim: int64(dim),
					Data: &schemapb.VectorField_FloatVector{
						FloatVector: &schemapb.FloatArray{
							Data: generateFloatVectors(nq, dim),
						},
					},
				},
			},
		}

		//resp, err := proxy.CalcDistance(ctx, &milvuspb.CalcDistanceRequest{
		_, err := proxy.CalcDistance(ctx, &milvuspb.CalcDistanceRequest{
			Base:    nil,
			OpLeft:  opLeft,
			OpRight: opRight,
			Params: []*commonpb.KeyValuePair{
				{
					Key:   MetricTypeKey,
					Value: distance.L2,
				},
			},
		})
		assert.NoError(t, err)
		// assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		// TODO(dragondriver): compare distance

		// TODO(dragondriver): use primary key to calculate distance
	})

	t.Run("get dd channel", func(t *testing.T) {
		f := func() {
			_, _ = proxy.GetDdChannel(ctx, &internalpb.GetDdChannelRequest{})
		}
		assert.Panics(t, f)
	})

	t.Run("get persistent segment info", func(t *testing.T) {
		resp, err := proxy.GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("get query segment info", func(t *testing.T) {
		resp, err := proxy.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	// TODO(dragondriver): dummy

	t.Run("register link", func(t *testing.T) {
		resp, err := proxy.RegisterLink(ctx, &milvuspb.RegisterLinkRequest{})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("get metrics", func(t *testing.T) {
		req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		assert.NoError(t, err)
		resp, err := proxy.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// get from cache
		resp, err = proxy.GetMetrics(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// failed to parse metric type
		resp, err = proxy.GetMetrics(ctx, &milvuspb.GetMetricsRequest{
			Base:    &commonpb.MsgBase{},
			Request: "not in json format",
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// not implemented metric
		notImplemented, err := metricsinfo.ConstructRequestByMetricType("not implemented")
		assert.NoError(t, err)
		resp, err = proxy.GetMetrics(ctx, notImplemented)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("release collection", func(t *testing.T) {
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// release dql message stream
		resp, err = proxy.ReleaseDQLMessageStream(ctx, &proxypb.ReleaseDQLMessageStreamRequest{
			Base:         nil,
			DbID:         0,
			CollectionID: collectionID,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("show in-memory collections after release", func(t *testing.T) {
		resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:            nil,
			DbName:          dbName,
			TimeStamp:       0,
			Type:            milvuspb.ShowType_InMemory,
			CollectionNames: nil,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 0, len(resp.CollectionNames))
	})

	t.Run("load partitions", func(t *testing.T) {
		resp, err := proxy.LoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionNames: []string{partitionName},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// non-exist partition -> fail
		resp, err = proxy.LoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionNames: []string{otherPartitionName},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// non-exist collection-> fail
		resp, err = proxy.LoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			PartitionNames: []string{partitionName},
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("show in-memory partitions", func(t *testing.T) {
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionNames: nil,
			Type:           milvuspb.ShowType_InMemory,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		// default partition?
		assert.Equal(t, 1, len(resp.PartitionNames))

		// show partition not in-memory -> fail
		resp, err = proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionNames: []string{otherPartitionName},
			Type:           milvuspb.ShowType_InMemory,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)

		// non-exist collection -> fail
		resp, err = proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			CollectionID:   collectionID,
			PartitionNames: []string{partitionName},
			Type:           milvuspb.ShowType_InMemory,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("release partition", func(t *testing.T) {
		resp, err := proxy.ReleasePartitions(ctx, &milvuspb.ReleasePartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionNames: []string{partitionName},
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("show in-memory partitions after release partition", func(t *testing.T) {
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionNames: nil,
			Type:           milvuspb.ShowType_InMemory,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		// default partition
		assert.Equal(t, 0, len(resp.PartitionNames))

		resp, err = proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionNames: []string{partitionName}, // released
			Type:           milvuspb.ShowType_InMemory,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("drop partition", func(t *testing.T) {
		resp, err := proxy.DropPartition(ctx, &milvuspb.DropPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// invalidate meta cache
		resp, err = proxy.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// drop non-exist partition -> fail

		resp, err = proxy.DropPartition(ctx, &milvuspb.DropPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		resp, err = proxy.DropPartition(ctx, &milvuspb.DropPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  otherCollectionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		resp, err = proxy.DropPartition(ctx, &milvuspb.DropPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: otherCollectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("has partition after drop partition", func(t *testing.T) {
		resp, err := proxy.HasPartition(ctx, &milvuspb.HasPartitionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.False(t, resp.Value)
	})

	t.Run("show partitions after drop partition", func(t *testing.T) {
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			CollectionID:   collectionID,
			PartitionNames: nil,
			Type:           milvuspb.ShowType_All,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		// default partition
		assert.Equal(t, 1, len(resp.PartitionNames))
	})

	t.Run("drop index", func(t *testing.T) {
		resp, err := proxy.DropIndex(ctx, &milvuspb.DropIndexRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			FieldName:      floatVecField,
			IndexName:      indexName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("Delete", func(t *testing.T) {
		_, err := proxy.Delete(ctx, &milvuspb.DeleteRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
			Expr:           "",
		})
		assert.NoError(t, err)
	})

	t.Run("drop collection", func(t *testing.T) {
		collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
		assert.NoError(t, err)

		resp, err := proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// invalidate meta cache
		resp, err = proxy.InvalidateCollectionMetaCache(ctx, &proxypb.InvalidateCollMetaCacheRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		// release dql stream
		resp, err = proxy.ReleaseDQLMessageStream(ctx, &proxypb.ReleaseDQLMessageStreamRequest{
			Base:         nil,
			DbID:         0,
			CollectionID: collectionID,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("has collection after drop collection", func(t *testing.T) {
		resp, err := proxy.HasCollection(ctx, &milvuspb.HasCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			TimeStamp:      0,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.False(t, resp.Value)
	})

	t.Run("show all collections after drop collection", func(t *testing.T) {
		resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:            nil,
			DbName:          dbName,
			TimeStamp:       0,
			Type:            milvuspb.ShowType_All,
			CollectionNames: nil,
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
		assert.Equal(t, 0, len(resp.CollectionNames))
	})

	// proxy unhealthy
	//
	//notStateCode := "not state code"
	//proxy.stateCode.Store(notStateCode)
	//
	//t.Run("GetComponentStates fail", func(t *testing.T) {
	//	_, err := proxy.GetComponentStates(ctx)
	//	assert.Error(t, err)
	//})

	proxy.UpdateStateCode(internalpb.StateCode_Abnormal)

	t.Run("ReleaseDQLMessageStream fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.ReleaseDQLMessageStream(ctx, &proxypb.ReleaseDQLMessageStreamRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("CreateCollection fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DropCollection fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("HasCollection fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("LoadCollection fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleaseCollection fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DescribeCollection fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetCollectionStatistics fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.GetCollectionStatistics(ctx, &milvuspb.GetCollectionStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("ShowCollections fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreatePartition fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DropPartition fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("HasPartition fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("LoadPartitions fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.LoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleasePartitions fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.ReleasePartitions(ctx, &milvuspb.ReleasePartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetPartitionStatistics fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.GetPartitionStatistics(ctx, &milvuspb.GetPartitionStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("ShowPartitions fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreateIndex fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DescribeIndex fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("DropIndex fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.DropIndex(ctx, &milvuspb.DropIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetIndexBuildProgress fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.GetIndexBuildProgress(ctx, &milvuspb.GetIndexBuildProgressRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetIndexState fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.GetIndexState(ctx, &milvuspb.GetIndexStateRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Insert fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.Insert(ctx, &milvuspb.InsertRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Delete fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.Delete(ctx, &milvuspb.DeleteRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Search fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.Search(ctx, &milvuspb.SearchRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Flush fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.Flush(ctx, &milvuspb.FlushRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Query fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.Query(ctx, &milvuspb.QueryRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetPersistentSegmentInfo fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.GetPersistentSegmentInfo(ctx, &milvuspb.GetPersistentSegmentInfoRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetQuerySegmentInfo fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("RegisterLink fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.RegisterLink(ctx, &milvuspb.RegisterLinkRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetMetrics fail, unhealthy", func(t *testing.T) {
		resp, err := proxy.GetMetrics(ctx, &milvuspb.GetMetricsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	proxy.UpdateStateCode(internalpb.StateCode_Healthy)

	// queue full

	ddParallel := proxy.sched.ddQueue.getMaxTaskNum()
	proxy.sched.ddQueue.setMaxTaskNum(0)

	t.Run("CreateCollection fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DropCollection fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.DropCollection(ctx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("HasCollection fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.HasCollection(ctx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("LoadCollection fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleaseCollection fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DescribeCollection fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetCollectionStatistics fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.GetCollectionStatistics(ctx, &milvuspb.GetCollectionStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("ShowCollections fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreatePartition fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DropPartition fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.DropPartition(ctx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("HasPartition fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.HasPartition(ctx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("LoadPartitions fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.LoadPartitions(ctx, &milvuspb.LoadPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleasePartitions fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.ReleasePartitions(ctx, &milvuspb.ReleasePartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetPartitionStatistics fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.GetPartitionStatistics(ctx, &milvuspb.GetPartitionStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("ShowPartitions fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreateIndex fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DescribeIndex fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("DropIndex fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.DropIndex(ctx, &milvuspb.DropIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetIndexBuildProgress fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.GetIndexBuildProgress(ctx, &milvuspb.GetIndexBuildProgressRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetIndexState fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.GetIndexState(ctx, &milvuspb.GetIndexStateRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Flush fail, dd queue full", func(t *testing.T) {
		resp, err := proxy.Flush(ctx, &milvuspb.FlushRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	proxy.sched.ddQueue.setMaxTaskNum(ddParallel)

	dmParallelism := proxy.sched.dmQueue.getMaxTaskNum()
	proxy.sched.dmQueue.setMaxTaskNum(0)

	t.Run("Insert fail, dm queue full", func(t *testing.T) {
		resp, err := proxy.Insert(ctx, &milvuspb.InsertRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Delete fail, dm queue full", func(t *testing.T) {
		resp, err := proxy.Delete(ctx, &milvuspb.DeleteRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	proxy.sched.dmQueue.setMaxTaskNum(dmParallelism)

	dqParallelism := proxy.sched.dqQueue.getMaxTaskNum()
	proxy.sched.dqQueue.setMaxTaskNum(0)

	t.Run("Search fail, dq queue full", func(t *testing.T) {
		resp, err := proxy.Search(ctx, &milvuspb.SearchRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Query fail, dq queue full", func(t *testing.T) {
		resp, err := proxy.Query(ctx, &milvuspb.QueryRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	proxy.sched.dqQueue.setMaxTaskNum(dqParallelism)

	// timeout

	timeout := time.Nanosecond
	shortCtx, shortCancel := context.WithTimeout(ctx, timeout)
	defer shortCancel()
	time.Sleep(timeout)

	t.Run("CreateCollection, timeout", func(t *testing.T) {
		resp, err := proxy.CreateCollection(shortCtx, &milvuspb.CreateCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DropCollection fail, timeout", func(t *testing.T) {
		resp, err := proxy.DropCollection(shortCtx, &milvuspb.DropCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("HasCollection fail, timeout", func(t *testing.T) {
		resp, err := proxy.HasCollection(shortCtx, &milvuspb.HasCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("LoadCollection fail, timeout", func(t *testing.T) {
		resp, err := proxy.LoadCollection(shortCtx, &milvuspb.LoadCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleaseCollection fail, timeout", func(t *testing.T) {
		resp, err := proxy.ReleaseCollection(shortCtx, &milvuspb.ReleaseCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DescribeCollection fail, timeout", func(t *testing.T) {
		resp, err := proxy.DescribeCollection(shortCtx, &milvuspb.DescribeCollectionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetCollectionStatistics fail, timeout", func(t *testing.T) {
		resp, err := proxy.GetCollectionStatistics(shortCtx, &milvuspb.GetCollectionStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("ShowCollections fail, timeout", func(t *testing.T) {
		resp, err := proxy.ShowCollections(shortCtx, &milvuspb.ShowCollectionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreatePartition fail, timeout", func(t *testing.T) {
		resp, err := proxy.CreatePartition(shortCtx, &milvuspb.CreatePartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DropPartition fail, timeout", func(t *testing.T) {
		resp, err := proxy.DropPartition(shortCtx, &milvuspb.DropPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("HasPartition fail, timeout", func(t *testing.T) {
		resp, err := proxy.HasPartition(shortCtx, &milvuspb.HasPartitionRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("LoadPartitions fail, timeout", func(t *testing.T) {
		resp, err := proxy.LoadPartitions(shortCtx, &milvuspb.LoadPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("ReleasePartitions fail, timeout", func(t *testing.T) {
		resp, err := proxy.ReleasePartitions(shortCtx, &milvuspb.ReleasePartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetPartitionStatistics fail, timeout", func(t *testing.T) {
		resp, err := proxy.GetPartitionStatistics(shortCtx, &milvuspb.GetPartitionStatisticsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("ShowPartitions fail, timeout", func(t *testing.T) {
		resp, err := proxy.ShowPartitions(shortCtx, &milvuspb.ShowPartitionsRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("CreateIndex fail, timeout", func(t *testing.T) {
		resp, err := proxy.CreateIndex(shortCtx, &milvuspb.CreateIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("DescribeIndex fail, timeout", func(t *testing.T) {
		resp, err := proxy.DescribeIndex(shortCtx, &milvuspb.DescribeIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("DropIndex fail, timeout", func(t *testing.T) {
		resp, err := proxy.DropIndex(shortCtx, &milvuspb.DropIndexRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	t.Run("GetIndexBuildProgress fail, timeout", func(t *testing.T) {
		resp, err := proxy.GetIndexBuildProgress(shortCtx, &milvuspb.GetIndexBuildProgressRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("GetIndexState fail, timeout", func(t *testing.T) {
		resp, err := proxy.GetIndexState(shortCtx, &milvuspb.GetIndexStateRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Flush fail, timeout", func(t *testing.T) {
		_, err := proxy.Flush(shortCtx, &milvuspb.FlushRequest{})
		assert.NoError(t, err)
		// FIXME(dragondriver)
		// assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Insert fail, timeout", func(t *testing.T) {
		resp, err := proxy.Insert(shortCtx, &milvuspb.InsertRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Delete fail, timeout", func(t *testing.T) {
		resp, err := proxy.Delete(shortCtx, &milvuspb.DeleteRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Search fail, timeout", func(t *testing.T) {
		resp, err := proxy.Search(shortCtx, &milvuspb.SearchRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("Query fail, dq queue full", func(t *testing.T) {
		resp, err := proxy.Query(shortCtx, &milvuspb.QueryRequest{})
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, resp.Status.ErrorCode)
	})

	t.Run("create alias", func(t *testing.T) {
		req := createCollectionReq
		resp, err := proxy.CreateCollection(ctx, req)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		// create alias
		aliasReq := &milvuspb.CreateAliasRequest{
			Base:           nil,
			CollectionName: collectionName,
			Alias:          "alias",
		}
		resp, err = proxy.CreateAlias(ctx, aliasReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
		sameAliasReq := &milvuspb.CreateAliasRequest{
			Base:           nil,
			CollectionName: collectionName,
			Alias:          "alias",
		}

		resp, err = proxy.CreateAlias(ctx, sameAliasReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
	})

	t.Run("alter alias", func(t *testing.T) {
		// alter alias
		alterReq := &milvuspb.AlterAliasRequest{
			Base:           nil,
			CollectionName: collectionName,
			Alias:          "alias",
		}
		resp, err := proxy.AlterAlias(ctx, alterReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		nonExistingCollName := "coll_name_random_zarathustra"
		faultyAlterReq := &milvuspb.AlterAliasRequest{
			Base:           nil,
			CollectionName: nonExistingCollName,
			Alias:          "alias",
		}
		resp, err = proxy.AlterAlias(ctx, faultyAlterReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)
	})

	t.Run("drop alias", func(t *testing.T) {
		// drop alias
		resp, err := proxy.DropAlias(ctx, &milvuspb.DropAliasRequest{
			Base:  nil,
			Alias: "alias",
		})
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)

		sameDropReq := &milvuspb.DropAliasRequest{
			Base:  nil,
			Alias: "alias",
		}

		// Can't drop non-existing alias
		resp, err = proxy.DropAlias(ctx, sameDropReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_UnexpectedError, resp.ErrorCode)

		dropReq := &milvuspb.DropCollectionRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		}
		resp, err = proxy.DropCollection(ctx, dropReq)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, resp.ErrorCode)
	})

	cancel()
}
