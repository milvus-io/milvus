package base

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

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
