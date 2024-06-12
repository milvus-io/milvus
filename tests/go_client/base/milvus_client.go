package base

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/log"

	"go.uber.org/zap"

	"google.golang.org/grpc"

	clientv2 "github.com/milvus-io/milvus/client/v2"
	"github.com/milvus-io/milvus/client/v2/index"
)

func LoggingUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		maxLogLength := 300
		_method := strings.Split(method, "/")
		_methodShotName := _method[len(_method)-1]
		// Marshal req to json str
		reqJSON, err := json.Marshal(req)
		if err != nil {
			log.Error("Failed to marshal request", zap.Error(err))
			reqJSON = []byte("could not marshal request")
		}
		reqStr := string(reqJSON)
		if len(reqStr) > maxLogLength {
			reqStr = reqStr[:maxLogLength] + "..."
		}

		// log before
		log.Info("Request", zap.String("method", _methodShotName), zap.Any("reqs", reqStr))

		// invoker
		start := time.Now()
		errResp := invoker(ctx, method, req, reply, cc, opts...)
		cost := time.Since(start)

		// Marshal reply to json str
		respJSON, err := json.Marshal(reply)
		if err != nil {
			log.Error("Failed to marshal response", zap.Error(err))
			respJSON = []byte("could not marshal response")
		}
		respStr := string(respJSON)
		if len(respStr) > maxLogLength {
			respStr = respStr[:maxLogLength] + "..."
		}

		// log after
		log.Info("Response", zap.String("method", _methodShotName), zap.Any("resp", respStr))
		log.Debug("Cost", zap.String("method", _methodShotName), zap.Duration("cost", cost))
		return errResp
	}
}

type MilvusClient struct {
	mClient *clientv2.Client
}

func NewMilvusClient(ctx context.Context, cfg *clientv2.ClientConfig) (*MilvusClient, error) {
	cfg.DialOptions = append(cfg.DialOptions, grpc.WithUnaryInterceptor(LoggingUnaryInterceptor()))
	mClient, err := clientv2.New(ctx, cfg)
	return &MilvusClient{
		mClient,
	}, err
}

func (mc *MilvusClient) Close(ctx context.Context) error {
	err := mc.mClient.Close(ctx)
	return err
}

// -- database --

// UsingDatabase list all database in milvus cluster.
func (mc *MilvusClient) UsingDatabase(ctx context.Context, option clientv2.UsingDatabaseOption) error {
	err := mc.mClient.UsingDatabase(ctx, option)
	return err
}

// ListDatabases list all database in milvus cluster.
func (mc *MilvusClient) ListDatabases(ctx context.Context, option clientv2.ListDatabaseOption, callOptions ...grpc.CallOption) ([]string, error) {
	databaseNames, err := mc.mClient.ListDatabase(ctx, option, callOptions...)
	return databaseNames, err
}

// CreateDatabase create database with the given name.
func (mc *MilvusClient) CreateDatabase(ctx context.Context, option clientv2.CreateDatabaseOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.CreateDatabase(ctx, option, callOptions...)
	return err
}

// DropDatabase drop database with the given db name.
func (mc *MilvusClient) DropDatabase(ctx context.Context, option clientv2.DropDatabaseOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.DropDatabase(ctx, option, callOptions...)
	return err
}

// -- collection --

// CreateCollection Create Collection
func (mc *MilvusClient) CreateCollection(ctx context.Context, option clientv2.CreateCollectionOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.CreateCollection(ctx, option, callOptions...)
	return err
}

// ListCollections Create Collection
func (mc *MilvusClient) ListCollections(ctx context.Context, option clientv2.ListCollectionOption, callOptions ...grpc.CallOption) ([]string, error) {
	collectionNames, err := mc.mClient.ListCollections(ctx, option, callOptions...)
	return collectionNames, err
}

//DescribeCollection Describe collection
func (mc *MilvusClient) DescribeCollection(ctx context.Context, option clientv2.DescribeCollectionOption, callOptions ...grpc.CallOption) (*entity.Collection, error) {
	collection, err := mc.mClient.DescribeCollection(ctx, option, callOptions...)
	return collection, err
}

// HasCollection Has collection
func (mc *MilvusClient) HasCollection(ctx context.Context, option clientv2.HasCollectionOption, callOptions ...grpc.CallOption) (bool, error) {
	has, err := mc.mClient.HasCollection(ctx, option, callOptions...)
	return has, err
}

// DropCollection Drop Collection
func (mc *MilvusClient) DropCollection(ctx context.Context, option clientv2.DropCollectionOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.DropCollection(ctx, option, callOptions...)
	return err
}

// -- partition --

// CreatePartition Create Partition
func (mc *MilvusClient) CreatePartition(ctx context.Context, option clientv2.CreatePartitionOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.CreatePartition(ctx, option, callOptions...)
	return err
}

// DropPartition Drop Partition
func (mc *MilvusClient) DropPartition(ctx context.Context, option clientv2.DropPartitionOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.DropPartition(ctx, option, callOptions...)
	return err
}

// HasPartition Has Partition
func (mc *MilvusClient) HasPartition(ctx context.Context, option clientv2.HasPartitionOption, callOptions ...grpc.CallOption) (bool, error) {
	has, err := mc.mClient.HasPartition(ctx, option, callOptions...)
	return has, err
}

// ListPartitions List Partitions
func (mc *MilvusClient) ListPartitions(ctx context.Context, option clientv2.ListPartitionsOption, callOptions ...grpc.CallOption) ([]string, error) {
	partitionNames, err := mc.mClient.ListPartitions(ctx, option, callOptions...)
	return partitionNames, err
}

// LoadPartitions Load Partitions into memory
func (mc *MilvusClient) LoadPartitions(ctx context.Context, option clientv2.LoadPartitionsOption, callOptions ...grpc.CallOption) (clientv2.LoadTask, error) {
	loadTask, err := mc.mClient.LoadPartitions(ctx, option, callOptions...)
	return loadTask, err
}

// -- index --

// CreateIndex Create Index
func (mc *MilvusClient) CreateIndex(ctx context.Context, option clientv2.CreateIndexOption, callOptions ...grpc.CallOption) (*clientv2.CreateIndexTask, error) {
	createIndexTask, err := mc.mClient.CreateIndex(ctx, option, callOptions...)
	return createIndexTask, err
}

// ListIndexes List Indexes
func (mc *MilvusClient) ListIndexes(ctx context.Context, option clientv2.ListIndexOption, callOptions ...grpc.CallOption) ([]string, error) {
	indexes, err := mc.mClient.ListIndexes(ctx, option, callOptions...)
	return indexes, err
}

// DescribeIndex Describe Index
func (mc *MilvusClient) DescribeIndex(ctx context.Context, option clientv2.DescribeIndexOption, callOptions ...grpc.CallOption) (index.Index, error) {
	index, err := mc.mClient.DescribeIndex(ctx, option, callOptions...)
	return index, err
}

// DropIndex Drop Index
func (mc *MilvusClient) DropIndex(ctx context.Context, option clientv2.DropIndexOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.DropIndex(ctx, option, callOptions...)
	return err
}

// -- write --

// Insert insert data
func (mc *MilvusClient) Insert(ctx context.Context, option clientv2.InsertOption, callOptions ...grpc.CallOption) (clientv2.InsertResult, error) {
	insertRes, err := mc.mClient.Insert(ctx, option, callOptions...)
	if err == nil{
		log.Info("Insert", zap.Any("result", insertRes))
	}
	return insertRes, err
}

// Flush flush data
func (mc *MilvusClient) Flush(ctx context.Context, option clientv2.FlushOption, callOptions ...grpc.CallOption) (*clientv2.FlushTask, error) {
	flushTask, err := mc.mClient.Flush(ctx, option, callOptions...)
	return flushTask, err
}

// Delete deletes data
func (mc *MilvusClient) Delete(ctx context.Context, option clientv2.DeleteOption, callOptions ...grpc.CallOption) (clientv2.DeleteResult, error) {
	deleteRes, err := mc.mClient.Delete(ctx, option, callOptions...)
	return deleteRes, err
}

// Upsert upsert data
func (mc *MilvusClient) Upsert(ctx context.Context, option clientv2.UpsertOption, callOptions ...grpc.CallOption) (clientv2.UpsertResult, error) {
	upsertRes, err := mc.mClient.Upsert(ctx, option, callOptions...)
	return upsertRes, err
}

// -- read --

// LoadCollection Load Collection
func (mc *MilvusClient) LoadCollection(ctx context.Context, option clientv2.LoadCollectionOption, callOptions ...grpc.CallOption) (clientv2.LoadTask, error) {
	loadTask, err := mc.mClient.LoadCollection(ctx, option, callOptions...)
	return loadTask, err
}

// Search search from collection
func (mc *MilvusClient) Search(ctx context.Context, option clientv2.SearchOption, callOptions ...grpc.CallOption) ([]clientv2.ResultSet, error) {
	resultSets, err := mc.mClient.Search(ctx, option, callOptions...)
	return resultSets, err
}

// Query query from collection
func (mc *MilvusClient) Query(ctx context.Context, option clientv2.QueryOption, callOptions ...grpc.CallOption) (clientv2.ResultSet, error) {
	resultSet, err := mc.mClient.Query(ctx, option, callOptions...)
	return resultSet, err
}
