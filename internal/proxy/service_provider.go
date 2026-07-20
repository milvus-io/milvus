package proxy

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v3/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

// serviceProviderMetricError lets a provider preserve a non-failure metric
// outcome, such as "abandon" for a request that could not be enqueued, while
// keeping the original error identity and merr code intact for the response.
type serviceProviderMetricError struct {
	err    error
	status string
	cause  string
}

func (e *serviceProviderMetricError) Error() string {
	return e.err.Error()
}

func (e *serviceProviderMetricError) Unwrap() error {
	return e.err
}

func withServiceProviderMetric(err error, status, cause string) error {
	return &serviceProviderMetricError{err: err, status: status, cause: cause}
}

func serviceCallMetricLabel(err error) (string, string) {
	var metricErr *serviceProviderMetricError
	if errors.As(err, &metricErr) {
		return metricErr.status, metricErr.cause
	}
	return failMetricLabel(err)
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
			proxy:      proxy,
			method:     method,
			onCall:     provider.DescribeCollection,
			onResponse: finalizeDescribeCollectionResponse,
			onError: func(err error) (*milvuspb.DescribeCollectionResponse, error) {
				return &milvuspb.DescribeCollectionResponse{
					Status: merr.Status(err),
				}, nil
			},
		}
		return interface{}(interceptor).(*InterceptorImpl[Req, Resp]), nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("method %s not supported", method)
	}
}

type InterceptorImpl[Req Request, Resp Response] struct {
	proxy      *Proxy
	method     string
	onCall     func(ctx context.Context, request Req) (Resp, error)
	onResponse func(resp Resp) error
	onError    func(err error) (Resp, error)
}

func (i *InterceptorImpl[Req, Resp]) Call(ctx context.Context, request Req,
) (Resp, error) {
	if err := merr.CheckHealthy(i.proxy.GetStateCode()); err != nil {
		return i.onError(err)
	}

	ctx, sp := otel.Tracer(typeutil.ProxyRole).Start(ctx, fmt.Sprintf("Proxy-%s", i.method))
	defer sp.End()
	tr := timerecord.NewTimeRecorder(i.method)
	nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
	dbName := request.GetDbName()
	collectionName := request.GetCollectionName()
	metrics.ProxyFunctionCall.WithLabelValues(nodeID, i.method,
		metrics.TotalLabel, metrics.CauseNA, dbName, collectionName).Inc()
	defer func() {
		metrics.ProxyReqLatency.WithLabelValues(nodeID, i.method).
			Observe(float64(tr.ElapseSpan().Milliseconds()))
	}()

	resp, err := i.onCall(ctx, request)
	if err != nil {
		status, cause := serviceCallMetricLabel(err)
		metrics.ProxyFunctionCall.WithLabelValues(nodeID, i.method,
			status, cause, dbName, collectionName).Inc()
		return i.onError(err)
	}

	if i.onResponse != nil && merr.Ok(resp.GetStatus()) {
		if err := i.onResponse(resp); err != nil {
			status, cause := serviceCallMetricLabel(err)
			metrics.ProxyFunctionCall.WithLabelValues(nodeID, i.method,
				status, cause, dbName, collectionName).Inc()
			return i.onError(err)
		}
	}

	if !merr.Ok(resp.GetStatus()) {
		status, cause := failMetricLabel(merr.Error(resp.GetStatus()))
		metrics.ProxyFunctionCall.WithLabelValues(nodeID, i.method,
			status, cause, dbName, collectionName).Inc()
		return resp, nil
	}

	metrics.ProxyFunctionCall.WithLabelValues(nodeID, i.method,
		metrics.SuccessLabel, metrics.CauseNA, dbName, collectionName).Inc()

	return resp, nil
}

type CachedProxyServiceProvider struct {
	*Proxy
}

func describeCollectionErrorStatus(err error, database, collectionName string) *commonpb.Status {
	// A user-facing DescribeCollection miss is an input error, but keep the
	// precise source code: a missing database is ErrDatabaseNotFound while a
	// missing collection is ErrCollectionNotFound. The sentinels remain system
	// errors globally because internal refresh/retry paths also use them. The
	// deprecated ErrorCode enum has no database-not-found value, so Code=800 is
	// authoritative there and ErrorCode keeps merr's standard UnexpectedError
	// compatibility fallback.
	err = merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	status := merr.Status(err)
	if errors.Is(err, merr.ErrCollectionNotFound) {
		// Preserve the established SDK-visible collection-not-found message while
		// letting merr.Status own both the typed Code and legacy ErrorCode mapping.
		reason := fmt.Sprintf("can't find collection[database=%s][collection=%s]", database, collectionName)
		status.Reason = reason
		status.Detail = reason
	}
	return status
}

func needsTimestamptzDefaultProjection(field *schemapb.FieldSchema) bool {
	if field.GetDataType() != schemapb.DataType_Timestamptz || field.GetDefaultValue() == nil {
		return false
	}
	_, ok := field.GetDefaultValue().GetData().(*schemapb.ValueField_TimestamptzData)
	return ok
}

// projectDescribeCollectionSchema defines the public DescribeCollection schema
// shape shared by cached and remote providers. sourceShared is true for
// MetaCache-owned schemas: only fields that the public TIMESTAMPTZ rewrite will
// mutate are cloned, keeping the canonical cached int64 representation intact.
func projectDescribeCollectionSchema(source *schemapb.CollectionSchema, sourceShared bool) (*schemapb.CollectionSchema, error) {
	if source == nil {
		return nil, merr.WrapErrServiceInternalMsg("describe collection returned a nil collection schema")
	}

	projected := &schemapb.CollectionSchema{
		Name:               source.GetName(),
		Description:        source.GetDescription(),
		AutoID:             source.GetAutoID(),
		Fields:             make([]*schemapb.FieldSchema, 0, len(source.GetFields())),
		EnableDynamicField: source.GetEnableDynamicField(),
		Properties:         append([]*commonpb.KeyValuePair(nil), source.GetProperties()...),
		Functions:          append([]*schemapb.FunctionSchema(nil), source.GetFunctions()...),
		DbName:             source.GetDbName(),
		StructArrayFields:  make([]*schemapb.StructArrayFieldSchema, 0, len(source.GetStructArrayFields())),
		Version:            source.GetVersion(),
		ExternalSource:     source.GetExternalSource(),
		ExternalSpec:       source.GetExternalSpec(),
		EnableNamespace:    source.GetEnableNamespace(),
	}

	for _, field := range source.GetFields() {
		if field.GetIsDynamic() || field.GetName() == common.NamespaceFieldName ||
			field.GetFieldID() < common.StartOfUserFieldID {
			continue
		}

		outputField := field
		if sourceShared && needsTimestamptzDefaultProjection(field) {
			outputField = proto.Clone(field).(*schemapb.FieldSchema)
		}
		projected.Fields = append(projected.Fields, outputField)
	}

	// Struct field names are restored in place, so these messages must always be
	// detached from both RootCoord's response and MetaCache's canonical schema.
	for _, field := range source.GetStructArrayFields() {
		projected.StructArrayFields = append(projected.StructArrayFields,
			proto.Clone(field).(*schemapb.StructArrayFieldSchema))
	}

	if err := restoreStructFieldNames(projected); err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "failed to restore struct field names")
	}
	return projected, nil
}

func finalizeDescribeCollectionResponse(resp *milvuspb.DescribeCollectionResponse) error {
	if resp == nil || !merr.Ok(resp.GetStatus()) {
		return nil
	}
	if resp.GetSchema() == nil {
		return merr.WrapErrServiceInternalMsg("describe collection returned a nil projected schema")
	}
	if err := timestamptz.RewriteTimestampTzDefaultValueToString(resp.Schema); err != nil {
		return merr.WrapErrServiceInternalErr(err, "failed to project TIMESTAMPTZ default value")
	}
	resp.Schema.ExternalSpec = externalspec.RedactExternalSpec(resp.Schema.GetExternalSpec())
	return nil
}

func (node *CachedProxyServiceProvider) DescribeCollection(ctx context.Context,
	request *milvuspb.DescribeCollectionRequest,
) (resp *milvuspb.DescribeCollectionResponse, err error) {
	log := mlog.With(
		mlog.String("role", typeutil.ProxyRole),
		mlog.String("db", request.GetDbName()),
		mlog.String("collection", request.GetCollectionName()),
		mlog.FieldCollectionID(request.GetCollectionID()),
		mlog.Uint64("timestamp", request.GetTimeStamp()),
	)

	log.Debug(ctx, "DescribeCollection received")

	resp = &milvuspb.DescribeCollectionResponse{
		Status:         merr.Success(),
		CollectionName: request.CollectionName,
		DbName:         request.DbName,
	}

	// Resolve identifiers on local copies instead of mutating the request: the
	// access log interceptor and the success metric labels read the request
	// after the handler returns, and must see what the client actually sent.
	collectionName := request.CollectionName
	collectionID := request.CollectionID
	var c *collectionInfo

	resolvedNameByID := collectionName == "" && collectionID > 0
	if resolvedNameByID {
		// Resolve the complete entry by id once. Calling GetCollectionName first
		// discarded the returned collectionInfo and forced a second describe on a
		// rolling-upgrade old RootCoord response that omitted DbName (such entries
		// are deliberately returned uncached because their database is unknown).
		c, err = globalMetaCache.GetCollectionInfo(ctx, request.DbName, "", collectionID)
		if err != nil {
			resp.Status = describeCollectionErrorStatus(err, request.DbName, collectionName)
			return resp, nil
		}
		collectionName = c.schema.GetName()
	}

	// validate collection name, ref describeCollectionTask.PreExecute
	if err = validateCollectionName(collectionName); err != nil {
		resp.Status = describeCollectionErrorStatus(err, request.DbName, collectionName)
		return resp, nil
	}

	// Resolve the id and complete entry from the name only when the caller did
	// not provide an id. The id-only path already has the complete entry above.
	if !resolvedNameByID {
		collectionID, err = globalMetaCache.GetCollectionID(ctx, request.DbName, collectionName)
		if err != nil {
			resp.Status = describeCollectionErrorStatus(err, request.DbName, collectionName)
			return resp, nil
		}
		c, err = globalMetaCache.GetCollectionInfo(ctx, request.DbName, collectionName, collectionID)
		if err != nil {
			resp.Status = describeCollectionErrorStatus(err, request.DbName, collectionName)
			return resp, nil
		}
	}

	// The base logger was built from the raw request, whose name is empty for
	// id-only requests; surface the resolved name so downstream log lines stay
	// traceable. The request itself is left untouched -- the access log and
	// metric labels read it as the client sent it.
	if resolvedNameByID {
		log = log.With(mlog.String("resolvedCollection", collectionName))
	}

	if resp.CollectionName == "" {
		resp.CollectionName = c.schema.Name
	}

	resp.Schema, err = projectDescribeCollectionSchema(c.schema.CollectionSchema, true)
	if err != nil {
		log.Error(ctx, "failed to project collection schema", mlog.Err(err))
		return nil, err
	}

	// prefer the actual database resolved by the coordinator and carried in the
	// cache, the request db name may be empty/default when querying by collection id
	if c.dbName != "" {
		resp.DbName = c.dbName
	}
	resp.DbId = c.dbID
	resp.CollectionID = c.collID
	resp.UpdateTimestamp = c.updateTimestamp
	resp.UpdateTimestampStr = strconv.FormatUint(c.updateTimestamp, 10)
	resp.CreatedTimestamp = c.createdTimestamp
	resp.CreatedUtcTimestamp = c.createdUtcTimestamp
	resp.ConsistencyLevel = c.consistencyLevel
	resp.VirtualChannelNames = c.vChannels
	resp.PhysicalChannelNames = c.pChannels
	resp.NumPartitions = c.numPartitions
	resp.ShardsNum = c.shardsNum
	resp.Aliases = c.aliases
	resp.Properties = c.properties
	log.Debug(ctx, "DescribeCollection done",
		mlog.FieldCollectionID(resp.GetCollectionID()),
		mlog.Int("fieldCount", len(resp.GetSchema().GetFields())),
		mlog.Int("schemaVersion", int(resp.GetSchema().GetVersion())),
	)
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

	log := mlog.With(
		mlog.String("role", typeutil.ProxyRole),
		mlog.String("db", request.DbName),
		mlog.String("collection", request.CollectionName))

	log.Debug(ctx, "DescribeCollection received")

	if err := node.sched.ddQueue.Enqueue(dct); err != nil {
		log.Warn(ctx, "DescribeCollection failed to enqueue",
			mlog.Err(err))

		return nil, withServiceProviderMetric(err, metrics.AbandonLabel, metrics.CauseNA)
	}

	log.Debug(ctx, "DescribeCollection enqueued",
		mlog.Uint64("BeginTS", dct.BeginTs()),
		mlog.Uint64("EndTS", dct.EndTs()))

	if err := dct.WaitToFinish(); err != nil {
		log.Warn(ctx, "DescribeCollection failed to WaitToFinish",
			mlog.Err(err),
			mlog.Uint64("BeginTS", dct.BeginTs()),
			mlog.Uint64("EndTS", dct.EndTs()))

		return nil, err
	}

	log.Debug(ctx, "DescribeCollection done",
		mlog.Uint64("BeginTS", dct.BeginTs()),
		mlog.Uint64("EndTS", dct.EndTs()),
	)

	return dct.result, nil
}
