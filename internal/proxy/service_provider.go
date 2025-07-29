package proxy

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type Request interface {
	GetDbName() string
	GetCollectionName() string
}

type Response interface {
	GetStatus() *commonpb.Status
}

type ServiceInterceptor[Req Request, Resp Response] interface {
	Call(ctx context.Context, request Req) (Resp, error)
}

func NewInterceptor[Req Request, Resp Response](proxy *Proxy, method string) (*InterceptorImpl[Req, Resp], error) {
	var provider milvuspb.MilvusServiceServer
	cached := paramtable.Get().ProxyCfg.EnableCachedServiceProvider.GetAsBool()
	if cached {
		provider = &CachedProxyServiceProvider{Proxy: proxy}
	} else {
		provider = &RemoteProxyServiceProvider{Proxy: proxy}
	}
	switch method {
	case "DescribeCollection":
		interceptor := &InterceptorImpl[*milvuspb.DescribeCollectionRequest, *milvuspb.DescribeCollectionResponse]{
			proxy:  proxy,
			method: method,
			onCall: provider.DescribeCollection,
			onError: func(err error) (*milvuspb.DescribeCollectionResponse, error) {
				return &milvuspb.DescribeCollectionResponse{
					Status: merr.Status(err),
				}, nil
			},
		}
		return interface{}(interceptor).(*InterceptorImpl[Req, Resp]), nil
	default:
		return nil, fmt.Errorf("method %s not supported", method)
	}
}

type InterceptorImpl[Req Request, Resp Response] struct {
	proxy   *Proxy
	method  string
	onCall  func(ctx context.Context, request Req) (Resp, error)
	onError func(err error) (Resp, error)
}

func (i *InterceptorImpl[Req, Resp]) Call(ctx context.Context, request Req,
) (Resp, error) {
	if err := merr.CheckHealthy(i.proxy.GetStateCode()); err != nil {
		return i.onError(err)
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, fmt.Sprintf("Proxy-%s", i.method))
	defer sp.End()
	tr := timerecord.NewTimeRecorder(i.method)
	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), i.method,
		metrics.TotalLabel, request.GetDbName(), request.GetCollectionName()).Inc()

	resp, err := i.onCall(ctx, request)
	if err != nil {
		return i.onError(err)
	}

	metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), i.method,
		metrics.SuccessLabel, request.GetDbName(), request.GetCollectionName()).Inc()
	metrics.ProxyReqLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), i.method).Observe(float64(tr.ElapseSpan().Milliseconds()))

	return resp, err
}

type CachedProxyServiceProvider struct {
	*Proxy
}

func (node *CachedProxyServiceProvider) DescribeCollection(ctx context.Context,
	request *milvuspb.DescribeCollectionRequest,
) (resp *milvuspb.DescribeCollectionResponse, err error) {
	resp = &milvuspb.DescribeCollectionResponse{
		Status:         merr.Success(),
		CollectionName: request.CollectionName,
		DbName:         request.DbName,
	}

	wrapErrorStatus := func(err error) *commonpb.Status {
		status := &commonpb.Status{}
		if errors.Is(err, merr.ErrCollectionNotFound) {
			// nolint
			status.ErrorCode = commonpb.ErrorCode_CollectionNotExists
			// nolint
			status.Reason = fmt.Sprintf("can't find collection[database=%s][collection=%s]", request.DbName, request.CollectionName)
			status.ExtraInfo = map[string]string{merr.InputErrorFlagKey: "true"}
		} else {
			status = merr.Status(err)
		}
		return status
	}

	if request.CollectionName == "" && request.CollectionID > 0 {
		collName, err := globalMetaCache.GetCollectionName(ctx, request.DbName, request.CollectionID)
		if err != nil {
			resp.Status = wrapErrorStatus(err)
			return resp, nil
		}
		request.CollectionName = collName
	}

	// validate collection name, ref describeCollectionTask.PreExecute
	if err = validateCollectionName(request.CollectionName); err != nil {
		resp.Status = wrapErrorStatus(err)
		return resp, nil
	}

	request.CollectionID, err = globalMetaCache.GetCollectionID(ctx, request.DbName, request.CollectionName)
	if err != nil {
		resp.Status = wrapErrorStatus(err)
		return resp, nil
	}

	c, err := globalMetaCache.GetCollectionInfo(ctx, request.DbName, request.CollectionName, request.CollectionID)
	if err != nil {
		resp.Status = wrapErrorStatus(err)
		return resp, nil
	}

	// skip dynamic fields, see describeCollectionTask.Execute
	resp.Schema = &schemapb.CollectionSchema{
		Name:        c.schema.CollectionSchema.Name,
		Description: c.schema.CollectionSchema.Description,
		AutoID:      c.schema.CollectionSchema.AutoID,
		Fields: lo.Filter(c.schema.CollectionSchema.Fields, func(field *schemapb.FieldSchema, _ int) bool {
			return !field.IsDynamic
		}),
		StructArrayFields:  c.schema.CollectionSchema.StructArrayFields,
		EnableDynamicField: c.schema.CollectionSchema.EnableDynamicField,
		Properties:         c.schema.CollectionSchema.Properties,
		Functions:          c.schema.CollectionSchema.Functions,
		DbName:             c.schema.CollectionSchema.DbName,
	}
	resp.CollectionID = c.collID
	resp.UpdateTimestamp = c.updateTimestamp
	resp.UpdateTimestampStr = fmt.Sprintf("%d", c.updateTimestamp)
	resp.CreatedTimestamp = c.createdTimestamp
	resp.CreatedUtcTimestamp = c.createdUtcTimestamp
	resp.ConsistencyLevel = c.consistencyLevel
	resp.VirtualChannelNames = c.vChannels
	resp.PhysicalChannelNames = c.pChannels
	resp.NumPartitions = c.numPartitions
	resp.ShardsNum = c.shardsNum
	resp.Aliases = c.aliases
	resp.Properties = c.properties

	return resp, nil
}

type RemoteProxyServiceProvider struct {
	*Proxy
}

func (node *RemoteProxyServiceProvider) DescribeCollection(ctx context.Context,
	request *milvuspb.DescribeCollectionRequest,
) (*milvuspb.DescribeCollectionResponse, error) {
	dct := &describeCollectionTask{
		ctx:                       ctx,
		Condition:                 NewTaskCondition(ctx),
		DescribeCollectionRequest: request,
		mixCoord:                  node.mixCoord,
	}

	log := log.Ctx(ctx).With(
		zap.String("role", typeutil.ProxyRole),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName))

	method := "DescribeCollection"
	log.Debug("DescribeCollection received")

	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		log.Warn("DescribeCollection failed to enqueue",
			zap.Error(err))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.AbandonLabel, request.GetDbName(), request.GetCollectionName()).Inc()
		return nil, err
	}

	log.Debug("DescribeCollection enqueued",
		zap.Uint64("BeginTS", dct.BeginTs()),
		zap.Uint64("EndTS", dct.EndTs()))

	if err := dct.WaitToFinish(); err != nil {
		log.Warn("DescribeCollection failed to WaitToFinish",
			zap.Error(err),
			zap.Uint64("BeginTS", dct.BeginTs()),
			zap.Uint64("EndTS", dct.EndTs()))

		metrics.ProxyFunctionCall.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), method,
			metrics.FailLabel, request.GetDbName(), request.GetCollectionName()).Inc()

		return nil, err
	}

	log.Debug("DescribeCollection done",
		zap.Uint64("BeginTS", dct.BeginTs()),
		zap.Uint64("EndTS", dct.EndTs()),
		zap.String("db", request.DbName),
		zap.String("collection", request.CollectionName),
	)

	return dct.result, nil
}
