package base

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func LoggingUnaryInterceptor() grpc.UnaryClientInterceptor {
	// Limit debug logging for these methods
	ratedLogMethods := typeutil.NewSet("GetFlushState", "GetLoadingProgress", "DescribeIndex")

	logWithRateLimit := func(ctx context.Context, methodShortName string, logFunc func(context.Context, string, ...mlog.Field),
		logRateFunc func(context.Context, rate.Limit, string, ...mlog.Field),
		msg string, fields ...mlog.Field,
	) {
		if ratedLogMethods.Contain(methodShortName) {
			logRateFunc(ctx, rate.Limit(10), msg, fields...)
		} else {
			logFunc(ctx, msg, fields...)
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
				mlog.Error(ctx, "Failed to marshal", mlog.Err(err))
				return fallbackMsg
			}
			dataStr := string(dataJSON)
			if len(dataStr) > maxLogLength {
				return dataStr[:maxLogLength] + "......"
			}
			return dataStr
		}

		reqStr := marshalWithFallback(req, "could not marshal request")
		logWithRateLimit(ctx, _methodShortName, mlog.Info, mlog.RatedInfo, "Request", mlog.String("method", _methodShortName), mlog.String("reqs", reqStr))

		// Invoke the actual method
		start := time.Now()
		errResp := invoker(ctx, method, req, reply, cc, opts...)
		cost := time.Since(start)

		// Marshal response
		respStr := marshalWithFallback(reply, "could not marshal response")
		logWithRateLimit(ctx, _methodShortName, mlog.Info, mlog.RatedInfo, "Response", mlog.String("method", _methodShortName), mlog.String("resp", respStr))
		logWithRateLimit(ctx, _methodShortName, mlog.Debug, mlog.RatedDebug, "Cost", mlog.String("method", _methodShortName), mlog.Duration("cost", cost))

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
	if mc.Client == nil {
		return nil
	}
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
	err := mc.Client.CreateSnapshot(ctx, option, callOptions...)
	return err
}

// DropSnapshot drops a snapshot by name
func (mc *MilvusClient) DropSnapshot(ctx context.Context, option client.DropSnapshotOption, callOptions ...grpc.CallOption) error {
	err := mc.Client.DropSnapshot(ctx, option, callOptions...)
	return err
}

// ListSnapshots lists all snapshots for the specified collection or all snapshots if no collection is specified
func (mc *MilvusClient) ListSnapshots(ctx context.Context, option client.ListSnapshotsOption, callOptions ...grpc.CallOption) ([]string, error) {
	snapshots, err := mc.Client.ListSnapshots(ctx, option, callOptions...)
	return snapshots, err
}

// DescribeSnapshot describes a snapshot by name
func (mc *MilvusClient) DescribeSnapshot(ctx context.Context, option client.DescribeSnapshotOption, callOptions ...grpc.CallOption) (*milvuspb.DescribeSnapshotResponse, error) {
	resp, err := mc.Client.DescribeSnapshot(ctx, option, callOptions...)
	return resp, err
}

// RestoreSnapshot restores a snapshot to a target collection
func (mc *MilvusClient) RestoreSnapshot(ctx context.Context, option client.RestoreSnapshotOption, callOptions ...grpc.CallOption) (int64, error) {
	return mc.Client.RestoreSnapshot(ctx, option, callOptions...)
}

// GetRestoreSnapshotState gets the state of a restore snapshot job
func (mc *MilvusClient) GetRestoreSnapshotState(ctx context.Context, option client.GetRestoreSnapshotStateOption, callOptions ...grpc.CallOption) (*milvuspb.RestoreSnapshotInfo, error) {
	return mc.Client.GetRestoreSnapshotState(ctx, option, callOptions...)
}

// ListRestoreSnapshotJobs lists all restore snapshot jobs
func (mc *MilvusClient) ListRestoreSnapshotJobs(ctx context.Context, option client.ListRestoreSnapshotJobsOption, callOptions ...grpc.CallOption) ([]*milvuspb.RestoreSnapshotInfo, error) {
	return mc.Client.ListRestoreSnapshotJobs(ctx, option, callOptions...)
}
