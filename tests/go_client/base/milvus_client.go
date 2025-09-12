package base

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func LoggingUnaryInterceptor() grpc.UnaryClientInterceptor {
	// Limit debug logging for these methods
	ratedLogMethods := typeutil.NewSet("GetFlushState", "GetLoadingProgress", "DescribeIndex")

	logWithRateLimit := func(methodShortName string, logFunc func(msg string, fields ...zap.Field),
		logRateFunc func(cost float64, msg string, fields ...zap.Field) bool,
		msg string, fields ...zap.Field,
	) {
		if ratedLogMethods.Contain(methodShortName) {
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

		// Invoke the actual method
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
	*client.Client
}

func NewMilvusClient(ctx context.Context, cfg *client.ClientConfig) (*MilvusClient, error) {
	cfg.DialOptions = append(cfg.DialOptions, grpc.WithUnaryInterceptor(LoggingUnaryInterceptor()))
	mClient, err := client.New(ctx, cfg)
	return &MilvusClient{
		Client: mClient,
	}, err
}

func (mc *MilvusClient) Close(ctx context.Context) error {
	err := mc.Client.Close(ctx)
	return err
}

func (mc *MilvusClient) Compact(ctx context.Context, option client.CompactOption, callOptions ...grpc.CallOption) (int64, error) {
	compactID, err := mc.Client.Compact(ctx, option, callOptions...)
	return compactID, err
}

func (mc *MilvusClient) GetCompactionState(ctx context.Context, option client.GetCompactionStateOption, callOptions ...grpc.CallOption) (entity.CompactionState, error) {
	state, err := mc.Client.GetCompactionState(ctx, option, callOptions...)
	return state, err
}

// -- snapshot --

// CreateSnapshot creates a snapshot for the specified collection
func (mc *MilvusClient) CreateSnapshot(ctx context.Context, option client.CreateSnapshotOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.CreateSnapshot(ctx, option, callOptions...)
	return err
}

// DropSnapshot drops a snapshot by name
func (mc *MilvusClient) DropSnapshot(ctx context.Context, option client.DropSnapshotOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.DropSnapshot(ctx, option, callOptions...)
	return err
}

// ListSnapshots lists all snapshots for the specified collection or all snapshots if no collection is specified
func (mc *MilvusClient) ListSnapshots(ctx context.Context, option client.ListSnapshotsOption, callOptions ...grpc.CallOption) ([]string, error) {
	snapshots, err := mc.mClient.ListSnapshots(ctx, option, callOptions...)
	return snapshots, err
}

// DescribeSnapshot describes a snapshot by name
func (mc *MilvusClient) DescribeSnapshot(ctx context.Context, option client.DescribeSnapshotOption, callOptions ...grpc.CallOption) (*milvuspb.DescribeSnapshotResponse, error) {
	resp, err := mc.mClient.DescribeSnapshot(ctx, option, callOptions...)
	return resp, err
}

// RestoreSnapshot restores a snapshot to a target collection
func (mc *MilvusClient) RestoreSnapshot(ctx context.Context, option client.RestoreSnapshotOption, callOptions ...grpc.CallOption) error {
	err := mc.mClient.RestoreSnapshot(ctx, option, callOptions...)
	return err
}
