package base

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/log"
)

func LoggingUnaryInterceptor() grpc.UnaryClientInterceptor {
	// Limit debug logging for these methods
	rateLogMethods := map[string]struct{}{
		"GetFlushState":      {},
		"GetLoadingProgress": {},
		"DescribeIndex":      {},
	}

	logWithRateLimit := func(_methodShortName string, logFunc func(msg string, fields ...zap.Field),
		logRateFunc func(cost float64, msg string, fields ...zap.Field) bool,
		msg string, fields ...zap.Field,
	) {
		if _, exists := rateLogMethods[_methodShortName]; exists {
			logRateFunc(10, msg, fields...)
		} else {
			logFunc(msg, fields...)
		}
	}

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		const maxLogLength = 300
		_method := strings.Split(method, "/")
		_methodShortName := _method[len(_method)-1]

		// Marshal request
		marshalWithFallback := func(v interface{}, fallbackMsg string) string {
			dataJSON, err := json.Marshal(v)
			if err != nil {
				log.Error("Failed to marshal", zap.Error(err))
				return fallbackMsg
			}
			dataStr := string(dataJSON)
			if len(dataStr) > maxLogLength {
				return dataStr[:maxLogLength] + "......"
			}
			return dataStr
		}

		reqStr := marshalWithFallback(req, "could not marshal request")
		logWithRateLimit(_methodShortName, log.Info, log.RatedInfo, "Request", zap.String("method", _methodShortName), zap.String("reqs", reqStr))

		// Ike the actual method
		start := time.Now()
		errResp := invoker(ctx, method, req, reply, cc, opts...)
		cost := time.Since(start)

		// Marshal response
		respStr := marshalWithFallback(reply, "could not marshal response")
		logWithRateLimit(_methodShortName, log.Info, log.RatedInfo, "Response", zap.String("method", _methodShortName), zap.String("resp", respStr))
		logWithRateLimit(_methodShortName, log.Debug, log.RatedDebug, "Cost", zap.String("method", _methodShortName), zap.Duration("cost", cost))

		return errResp
	}
}

type MilvusClient struct {
	mClient *client.Client
}

func NewMilvusClient(ctx context.Context, cfg *client.ClientConfig) (*MilvusClient, error) {
	cfg.DialOptions = append(cfg.DialOptions, grpc.WithUnaryInterceptor(LoggingUnaryInterceptor()))
	mClient, err := client.New(ctx, cfg)
	return &MilvusClient{
		mClient,
	}, err
}

func (mc *MilvusClient) Close(ctx context.Context) error {
	err := mc.mClient.Close(ctx)
	return err
}

// -- database --

// UseDatabase list all database in milvus cluster.
func (mc *MilvusClient) UseDatabase(ctx context.Context, option client.UseDatabaseOption) error {
	err := mc.mClient.UseDatabase(ctx, option)
	return err
}

// ListDatabase list all database in milvus cluster.
func (mc *MilvusClient) ListDatabase(ctx context.Context, option client.ListDatabaseOption, callOptions ...grpc.CallOption) ([]string, error) {
	databaseNames, err := mc.mClient.ListDatabase(ctx, option, callOptions...)
	return databaseNames, err
}

// CreateDatabase create database with the given name.
func (mc *MilvusClient) CreateDatabase(ctx context.Context, option client.CreateDatabaseOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.CreateDatabase(ctx, option, callOptions...)
	return err
}

// DropDatabase drop database with the given db name.
func (mc *MilvusClient) DropDatabase(ctx context.Context, option client.DropDatabaseOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.DropDatabase(ctx, option, callOptions...)
	return err
}

// DescribeDatabase describe database with the given db name.
func (mc *MilvusClient) DescribeDatabase(ctx context.Context, option client.DescribeDatabaseOption, callOptions ...grpc.CallOption) (*entity.Database, error) {
	database, err := mc.mClient.DescribeDatabase(ctx, option, callOptions...)
	return database, err
}

// AlterDatabaseProperties alter database properties
func (mc *MilvusClient) AlterDatabaseProperties(ctx context.Context, option client.AlterDatabasePropertiesOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.AlterDatabaseProperties(ctx, option, callOptions...)
	return err
}

// DropDatabaseProperties drop database properties
func (mc *MilvusClient) DropDatabaseProperties(ctx context.Context, option client.DropDatabasePropertiesOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.AlterDatabaseProperties(ctx, option, callOptions...)
	return err
}

// -- collection --

// CreateCollection Create Collection
func (mc *MilvusClient) CreateCollection(ctx context.Context, option client.CreateCollectionOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.CreateCollection(ctx, option, callOptions...)
	return err
}

// ListCollections Create Collection
func (mc *MilvusClient) ListCollections(ctx context.Context, option client.ListCollectionOption, callOptions ...grpc.CallOption) ([]string, error) {
	collectionNames, err := mc.mClient.ListCollections(ctx, option, callOptions...)
	return collectionNames, err
}

// DescribeCollection Describe collection
func (mc *MilvusClient) DescribeCollection(ctx context.Context, option client.DescribeCollectionOption, callOptions ...grpc.CallOption) (*entity.Collection, error) {
	collection, err := mc.mClient.DescribeCollection(ctx, option, callOptions...)
	return collection, err
}

// HasCollection Has collection
func (mc *MilvusClient) HasCollection(ctx context.Context, option client.HasCollectionOption, callOptions ...grpc.CallOption) (bool, error) {
	has, err := mc.mClient.HasCollection(ctx, option, callOptions...)
	return has, err
}

// DropCollection Drop Collection
func (mc *MilvusClient) DropCollection(ctx context.Context, option client.DropCollectionOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.DropCollection(ctx, option, callOptions...)
	return err
}

// -- partition --

// CreatePartition Create Partition
func (mc *MilvusClient) CreatePartition(ctx context.Context, option client.CreatePartitionOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.CreatePartition(ctx, option, callOptions...)
	return err
}

// DropPartition Drop Partition
func (mc *MilvusClient) DropPartition(ctx context.Context, option client.DropPartitionOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.DropPartition(ctx, option, callOptions...)
	return err
}

// HasPartition Has Partition
func (mc *MilvusClient) HasPartition(ctx context.Context, option client.HasPartitionOption, callOptions ...grpc.CallOption) (bool, error) {
	has, err := mc.mClient.HasPartition(ctx, option, callOptions...)
	return has, err
}

// ListPartitions List Partitions
func (mc *MilvusClient) ListPartitions(ctx context.Context, option client.ListPartitionsOption, callOptions ...grpc.CallOption) ([]string, error) {
	partitionNames, err := mc.mClient.ListPartitions(ctx, option, callOptions...)
	return partitionNames, err
}

// LoadPartitions Load Partitions into memory
func (mc *MilvusClient) LoadPartitions(ctx context.Context, option client.LoadPartitionsOption, callOptions ...grpc.CallOption) (client.LoadTask, error) {
	loadTask, err := mc.mClient.LoadPartitions(ctx, option, callOptions...)
	return loadTask, err
}

// ReleasePartitions Release Partitions from memory
func (mc *MilvusClient) ReleasePartitions(ctx context.Context, option client.ReleasePartitionsOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.ReleasePartitions(ctx, option, callOptions...)
	return err
}

// -- index --

// CreateIndex Create Index
func (mc *MilvusClient) CreateIndex(ctx context.Context, option client.CreateIndexOption, callOptions ...grpc.CallOption) (*client.CreateIndexTask, error) {
	createIndexTask, err := mc.mClient.CreateIndex(ctx, option, callOptions...)
	return createIndexTask, err
}

// ListIndexes List Indexes
func (mc *MilvusClient) ListIndexes(ctx context.Context, option client.ListIndexOption, callOptions ...grpc.CallOption) ([]string, error) {
	indexes, err := mc.mClient.ListIndexes(ctx, option, callOptions...)
	return indexes, err
}

// DescribeIndex Describe Index
func (mc *MilvusClient) DescribeIndex(ctx context.Context, option client.DescribeIndexOption, callOptions ...grpc.CallOption) (client.IndexDescription, error) {
	idxDesc, err := mc.mClient.DescribeIndex(ctx, option, callOptions...)
	return idxDesc, err
}

// DropIndex Drop Index
func (mc *MilvusClient) DropIndex(ctx context.Context, option client.DropIndexOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.DropIndex(ctx, option, callOptions...)
	return err
}

// -- write --

// Insert insert data
func (mc *MilvusClient) Insert(ctx context.Context, option client.InsertOption, callOptions ...grpc.CallOption) (client.InsertResult, error) {
	insertRes, err := mc.mClient.Insert(ctx, option, callOptions...)
	if err == nil {
		log.Info("Insert", zap.Any("result", insertRes))
	}
	return insertRes, err
}

// Flush flush data
func (mc *MilvusClient) Flush(ctx context.Context, option client.FlushOption, callOptions ...grpc.CallOption) (*client.FlushTask, error) {
	flushTask, err := mc.mClient.Flush(ctx, option, callOptions...)
	return flushTask, err
}

// Delete deletes data
func (mc *MilvusClient) Delete(ctx context.Context, option client.DeleteOption, callOptions ...grpc.CallOption) (client.DeleteResult, error) {
	deleteRes, err := mc.mClient.Delete(ctx, option, callOptions...)
	return deleteRes, err
}

// Upsert upsert data
func (mc *MilvusClient) Upsert(ctx context.Context, option client.UpsertOption, callOptions ...grpc.CallOption) (client.UpsertResult, error) {
	upsertRes, err := mc.mClient.Upsert(ctx, option, callOptions...)
	return upsertRes, err
}

// -- read --

// LoadCollection Load Collection
func (mc *MilvusClient) LoadCollection(ctx context.Context, option client.LoadCollectionOption, callOptions ...grpc.CallOption) (client.LoadTask, error) {
	loadTask, err := mc.mClient.LoadCollection(ctx, option, callOptions...)
	return loadTask, err
}

// ReleaseCollection Release Collection
func (mc *MilvusClient) ReleaseCollection(ctx context.Context, option client.ReleaseCollectionOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.ReleaseCollection(ctx, option, callOptions...)
	return err
}

// Search search from collection
func (mc *MilvusClient) Search(ctx context.Context, option client.SearchOption, callOptions ...grpc.CallOption) ([]client.ResultSet, error) {
	resultSets, err := mc.mClient.Search(ctx, option, callOptions...)
	return resultSets, err
}

// Query query from collection
func (mc *MilvusClient) Query(ctx context.Context, option client.QueryOption, callOptions ...grpc.CallOption) (client.ResultSet, error) {
	resultSet, err := mc.mClient.Query(ctx, option, callOptions...)
	return resultSet, err
}

// Get get from collection
func (mc *MilvusClient) Get(ctx context.Context, option client.QueryOption, callOptions ...grpc.CallOption) (client.ResultSet, error) {
	resultSet, err := mc.mClient.Get(ctx, option, callOptions...)
	return resultSet, err
}
