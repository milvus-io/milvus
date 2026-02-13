package proxy

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/exprutil"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/timestamptz"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	WithCache    = true
	WithoutCache = false
)

const (
	RetrieveTaskName = "RetrieveTask"
	QueryTaskName    = "QueryTask"
)

type queryTask struct {
	baseTask
	Condition
	*internalpb.RetrieveRequest

	ctx            context.Context
	result         *milvuspb.QueryResults
	request        *milvuspb.QueryRequest
	mixCoord       types.MixCoordClient
	ids            *schemapb.IDs
	collectionName string
	queryParams    *queryParams
	schema         *schemaInfo

	translatedOutputFields []string
	userOutputFields       []string
	userDynamicFields      []string
	userAggregates         []agg.AggregateBase

	resultBuf *typeutil.ConcurrentSet[*internalpb.RetrieveResults]

	plan             *planpb.PlanNode
	partitionKeyMode bool
	shardclientMgr   shardclient.ShardClientMgr
	lb               shardclient.LBPolicy
	channelsMvcc     map[string]Timestamp
	fastSkip         bool

	reQuery              bool
	allQueryCnt          int64
	totalRelatedDataSize int64
	mustUsePartitionKey  bool
	resolvedTimezoneStr  string
	storageCost          segcore.StorageCost
	aggregationFieldMap  *agg.AggregationFieldMap
}

func (t *queryTask) getQueryLabel() string {
	if label := t.RetrieveRequest.GetQueryLabel(); label != "" {
		return label
	}
	return metrics.QueryLabel
}

type queryParams struct {
	limit             int64
	offset            int64
	reduceType        reduce.IReduceType
	isIterator        bool
	collectionID      int64
	groupByFields     []string
	orderByFields     []string // NEW: ORDER BY field specifications (e.g., "price:desc")
	timezone          string
	extractTimeFields []string
}

func isSupportedGroupByFieldType(dt schemapb.DataType) bool {
	switch dt {
	case schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_VarChar,
		schemapb.DataType_Timestamptz:
		return true
	default:
		return false
	}
}

func validateGroupByFieldSchema(field *schemapb.FieldSchema) error {
	if field == nil {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("group by field schema is nil"))
	}
	if !isSupportedGroupByFieldType(field.GetDataType()) {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg(
			"group by field %s has unsupported data type %s", field.GetName(), field.GetDataType().String(),
		))
	}
	return nil
}

func translateGroupByFieldIds(groupByFieldNames []string, schema *schemapb.CollectionSchema) ([]UniqueID, error) {
	if len(groupByFieldNames) == 0 {
		return nil, nil
	}

	fieldNameToSchema := make(map[string]*schemapb.FieldSchema, len(schema.Fields))
	for _, field := range schema.Fields {
		fieldNameToSchema[field.Name] = field
	}

	groupByFieldIds := make([]UniqueID, 0, len(groupByFieldNames))
	for _, groupByField := range groupByFieldNames {
		groupByField = strings.TrimSpace(groupByField)
		fieldSchema, found := fieldNameToSchema[groupByField]
		if !found {
			return nil, fmt.Errorf("field %s not exist", groupByField)
		}
		if err := validateGroupByFieldSchema(fieldSchema); err != nil {
			return nil, err
		}
		groupByFieldIds = append(groupByFieldIds, fieldSchema.GetFieldID())
	}
	return groupByFieldIds, nil
}

// validateOrderByFieldsWithGroupBy validates that ORDER BY fields are compatible with GROUP BY.
// When GROUP BY is used, ORDER BY can only reference columns in the GROUP BY clause.
// ORDER BY on aggregate expressions (e.g., count(*)) is not yet supported and is
// explicitly rejected. This restriction may be lifted in a future release.
func validateOrderByFieldsWithGroupBy(
	orderByFieldSpecs []string,
	groupByFields []string,
	aggregates []agg.AggregateBase,
) error {
	if len(orderByFieldSpecs) == 0 {
		return nil
	}

	// If no GROUP BY and no aggregates, any field is valid for ORDER BY
	hasGroupBy := len(groupByFields) > 0 || len(aggregates) > 0
	if !hasGroupBy {
		return nil
	}

	// Build set of valid ORDER BY targets (GROUP BY columns only)
	validTargets := make(map[string]bool)

	// Add GROUP BY fields as valid targets
	for _, field := range groupByFields {
		validTargets[strings.ToLower(strings.TrimSpace(field))] = true
	}

	// Validate each ORDER BY field
	for _, spec := range orderByFieldSpecs {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}

		// Extract field name (remove direction suffix like ":desc").
		// Only colon-separated format is supported (e.g., "price:desc").
		// Space-separated format (e.g., "price desc") is NOT supported.
		parts := strings.Split(spec, ":")
		fieldName := strings.ToLower(strings.TrimSpace(parts[0]))

		// Reject aggregate expressions — not yet supported
		if isAgg, _, _ := agg.MatchAggregationExpression(fieldName); isAgg {
			return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg(
				"ORDER BY on aggregate expression '%s' is not yet supported",
				fieldName,
			))
		}

		if !validTargets[fieldName] {
			return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg(
				"ORDER BY field '%s' is not valid: when using GROUP BY or aggregates, "+
					"ORDER BY can only reference GROUP BY columns. "+
					"Valid targets are: %v",
				fieldName, getValidTargetList(groupByFields, aggregates),
			))
		}
	}

	return nil
}

// getValidTargetList returns a formatted list of valid ORDER BY targets for error messages.
// The aggregates parameter is currently unused because ORDER BY on aggregate expressions
// (e.g., count(*)) is not yet supported. When enabled in the future, aggregate original
// names should be appended to the target list.
func getValidTargetList(groupByFields []string, _ []agg.AggregateBase) []string {
	targets := make([]string, 0, len(groupByFields))
	targets = append(targets, groupByFields...)
	return targets
}

// translateOrderByFields converts ORDER BY field specifications to planpb.OrderByField messages.
// Delegates parsing to orderby.ParseOrderByFields to ensure consistent behavior
// (direction validation, nullsFirst defaults) between C++ segcore and Go proxy pipeline.
func translateOrderByFields(orderByFieldSpecs []string, schema *schemapb.CollectionSchema) ([]*planpb.OrderByField, error) {
	parsed, err := orderby.ParseOrderByFields(orderByFieldSpecs, schema)
	if err != nil {
		return nil, merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg(err.Error()))
	}
	if len(parsed) == 0 {
		return nil, nil
	}

	result := make([]*planpb.OrderByField, len(parsed))
	for i, f := range parsed {
		result[i] = &planpb.OrderByField{
			FieldId:    f.FieldID,
			Ascending:  f.Ascending,
			NullsFirst: f.NullsFirst,
		}
	}
	return result, nil
}

// translateToOutputFieldIDs translates output fields name to output fields id.
// If no output fields specified, return only pk field
func translateToOutputFieldIDs(outputFields []string, schema *schemapb.CollectionSchema) ([]UniqueID, error) {
	outputFieldIDs := make([]UniqueID, 0, len(outputFields)+1)
	if len(outputFields) == 0 {
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				outputFieldIDs = append(outputFieldIDs, field.FieldID)
			}
		}
	} else {
		var pkFieldID UniqueID
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				pkFieldID = field.FieldID
			}
		}
		for _, reqField := range outputFields {
			var fieldFound bool
			for _, field := range schema.Fields {
				if reqField == field.Name {
					outputFieldIDs = append(outputFieldIDs, field.FieldID)
					fieldFound = true
					break
				}
			}

			if !fieldFound {
			structFieldLoop:
				for _, structField := range schema.StructArrayFields {
					for _, field := range structField.Fields {
						if reqField == field.Name {
							outputFieldIDs = append(outputFieldIDs, field.FieldID)
							fieldFound = true
							break structFieldLoop
						}
					}
				}
			}

			if !fieldFound {
				return nil, fmt.Errorf("field %s not exist", reqField)
			}
		}

		// pk field needs to be in output field list
		var pkFound bool
		for _, outputField := range outputFieldIDs {
			if outputField == pkFieldID {
				pkFound = true
				break
			}
		}

		if !pkFound {
			outputFieldIDs = append(outputFieldIDs, pkFieldID)
		}
	}
	return outputFieldIDs, nil
}

func filterSystemFields(outputFieldIDs []UniqueID) []UniqueID {
	filtered := make([]UniqueID, 0, len(outputFieldIDs))
	for _, outputFieldID := range outputFieldIDs {
		if !common.IsSystemField(outputFieldID) {
			filtered = append(filtered, outputFieldID)
		}
	}
	return filtered
}

// parseQueryParams get limit and offset from queryParamsPair, both are optional.
func parseQueryParams(queryParamsPair []*commonpb.KeyValuePair, largeTopKEnabled bool) (*queryParams, error) {
	var (
		limit             int64
		offset            int64
		reduceStopForBest bool
		isIterator        bool
		err               error
		collectionID      int64
		timezone          string
		extractTimeFields []string
	)
	reduceStopForBestStr, err := funcutil.GetAttrByKeyFromRepeatedKV(ReduceStopForBestKey, queryParamsPair)
	// if reduce_stop_for_best is provided
	if err == nil {
		reduceStopForBest, err = strconv.ParseBool(reduceStopForBestStr)
		if err != nil {
			return nil, merr.WrapErrParameterInvalid("true or false", reduceStopForBestStr,
				"value for reduce_stop_for_best is invalid")
		}
	}

	isIteratorStr, err := funcutil.GetAttrByKeyFromRepeatedKV(IteratorField, queryParamsPair)
	// if reduce_stop_for_best is provided
	if err == nil {
		isIterator, err = strconv.ParseBool(isIteratorStr)
		if err != nil {
			return nil, merr.WrapErrParameterInvalid("true or false", isIteratorStr,
				"value for iterator field is invalid")
		}
	}

	collectionIdStr, err := funcutil.GetAttrByKeyFromRepeatedKV(CollectionID, queryParamsPair)
	if err == nil {
		collectionID, err = strconv.ParseInt(collectionIdStr, 0, 64)
		if err != nil {
			return nil, merr.WrapErrParameterInvalid("int value for collection_id", CollectionID,
				"value for collection id is invalid")
		}
	}

	reduceType := reduce.IReduceNoOrder
	if isIterator {
		if reduceStopForBest {
			reduceType = reduce.IReduceInOrderForBest
		} else {
			reduceType = reduce.IReduceInOrder
		}
	}

	limit = typeutil.Unlimited
	isLimitProvided := false
	limitStr, err := funcutil.GetAttrByKeyFromRepeatedKV(LimitKey, queryParamsPair)
	// if limit is provided
	if err == nil {
		isLimitProvided = true
		limit, err = strconv.ParseInt(limitStr, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("%s [%s] is invalid", LimitKey, limitStr)
		}
	}
	if isLimitProvided {
		offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, queryParamsPair)
		// if offset is provided
		if err == nil {
			offset, err = strconv.ParseInt(offsetStr, 0, 64)
			if err != nil {
				return nil, fmt.Errorf("%s [%s] is invalid", OffsetKey, offsetStr)
			}
		}
		// validate max result window.
		if err = validateMaxQueryResultWindow(offset, limit, largeTopKEnabled); err != nil {
			return nil, fmt.Errorf("invalid max query result window, %w", err)
		}
	}

	timezone, _ = funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, queryParamsPair)
	if (timezone != "") && !timestamptz.IsTimezoneValid(timezone) {
		return nil, merr.WrapErrParameterInvalidMsg("unknown or invalid IANA Time Zone ID: %s", timezone)
	}

	extractTimeFieldsStr, err := funcutil.GetAttrByKeyFromRepeatedKV(TimefieldsKey, queryParamsPair)
	if err == nil {
		extractTimeFields = strings.FieldsFunc(extractTimeFieldsStr, func(r rune) bool {
			return r == ',' || r == ' '
		})
	}

	// parse group by fields
	groupByFieldsStr, err := funcutil.GetAttrByKeyFromRepeatedKV(QueryGroupByFieldsKey, queryParamsPair)
	var groupByFields []string
	if err == nil {
		splitFields := strings.Split(groupByFieldsStr, ",")
		for _, field := range splitFields {
			trimmed := strings.TrimSpace(field)
			if trimmed != "" {
				groupByFields = append(groupByFields, trimmed)
			}
		}
	}

	// parse order by fields (e.g., "price:desc,rating:asc"). Only colon-separated format is supported.
	orderByFieldsStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OrderByFieldsKey, queryParamsPair)
	var orderByFields []string
	if err == nil {
		splitFields := strings.Split(orderByFieldsStr, ",")
		for _, field := range splitFields {
			trimmed := strings.TrimSpace(field)
			if trimmed != "" {
				orderByFields = append(orderByFields, trimmed)
			}
		}
	}

	return &queryParams{
		limit:             limit,
		offset:            offset,
		reduceType:        reduceType,
		isIterator:        isIterator,
		collectionID:      collectionID,
		groupByFields:     groupByFields,
		orderByFields:     orderByFields,
		timezone:          timezone,
		extractTimeFields: extractTimeFields,
	}, nil
}

func matchCountRule(outputs []string) bool {
	return len(outputs) == 1 && strings.ToLower(strings.TrimSpace(outputs[0])) == "count(*)"
}

func createCntPlan(expr string, schemaHelper *typeutil.SchemaHelper, exprTemplateValues map[string]*schemapb.TemplateValue) (*planpb.PlanNode, error) {
	if expr == "" {
		return &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: nil,
					IsCount:    true,
				},
			},
		}, nil
	}
	start := time.Now()
	plan, err := planparserv2.CreateRetrievePlan(schemaHelper, expr, exprTemplateValues)
	if err != nil {
		metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel, metrics.FailLabel).Observe(float64(time.Since(start).Milliseconds()))
		return nil, merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("failed to create query plan: %v", err))
	}
	metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel, metrics.SuccessLabel).Observe(float64(time.Since(start).Milliseconds()))
	plan.Node.(*planpb.PlanNode_Query).Query.IsCount = true

	return plan, nil
}

func (t *queryTask) createPlan(ctx context.Context) error {
	return t.createPlanArgs(ctx, &planparserv2.ParserVisitorArgs{})
}

func (t *queryTask) createPlanArgs(ctx context.Context, visitorArgs *planparserv2.ParserVisitorArgs) error {
	schema := t.schema
	var err error
	if t.plan == nil {
		start := time.Now()
		t.plan, err = planparserv2.CreateRetrievePlanArgs(schema.schemaHelper, t.request.Expr, t.request.GetExprTemplateValues(), visitorArgs)
		if err != nil {
			metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel, metrics.FailLabel).Observe(float64(time.Since(start).Milliseconds()))
			return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("failed to create query plan: %v", err))
		}
		metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel, metrics.SuccessLabel).Observe(float64(time.Since(start).Milliseconds()))
	}
	// parse output fields names
	originalOuputFields := t.request.GetOutputFields()
	t.translatedOutputFields, t.userOutputFields, t.userDynamicFields, t.userAggregates, _, err = translateOutputFields(t.request.GetOutputFields(), t.schema, false)
	if err != nil {
		return err
	}

	// parse aggregates
	t.plan.GetQuery().Aggregates = agg.AggregatesToPB(t.userAggregates)
	t.RetrieveRequest.Aggregates = t.plan.GetQuery().GetAggregates()
	// parse group by field ids
	groupByFieldsIDs, err := translateGroupByFieldIds(t.queryParams.groupByFields, t.schema.CollectionSchema)
	if err != nil {
		return err
	}
	t.plan.GetQuery().GroupByFieldIds = groupByFieldsIDs
	t.RetrieveRequest.GroupByFieldIds = groupByFieldsIDs

	// Validate ORDER BY fields compatibility with GROUP BY
	// When GROUP BY is used, ORDER BY can only reference groupBy columns or aggregate results
	if err := validateOrderByFieldsWithGroupBy(
		t.queryParams.orderByFields,
		t.queryParams.groupByFields,
		t.userAggregates,
	); err != nil {
		return err
	}

	// parse order by fields
	orderByFields, err := translateOrderByFields(t.queryParams.orderByFields, t.schema.CollectionSchema)
	if err != nil {
		return err
	}
	t.plan.GetQuery().OrderByFields = orderByFields
	// Also populate on RetrieveRequest so QN/Delegator can read directly
	// without re-parsing serialized_expr_plan.
	t.RetrieveRequest.OrderByFields = orderByFields

	hasAgg := len(t.RetrieveRequest.GroupByFieldIds) > 0 || len(t.RetrieveRequest.Aggregates) > 0
	// parse output field ids
	if hasAgg {
		emptyOutputFields := make([]UniqueID, 0)
		t.RetrieveRequest.OutputFieldsId = emptyOutputFields
		t.plan.OutputFieldIds = emptyOutputFields
		aggFieldMap, err := agg.NewAggregationFieldMap(originalOuputFields, t.queryParams.groupByFields, t.userAggregates)
		if err != nil {
			return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg(err.Error()))
		}
		t.aggregationFieldMap = aggFieldMap
	} else {
		outputFieldIDs, err := translateToOutputFieldIDs(t.translatedOutputFields, schema.CollectionSchema)
		if err != nil {
			return err
		}
		outputFieldIDs = append(outputFieldIDs, common.TimeStampField)
		t.RetrieveRequest.OutputFieldsId = outputFieldIDs
		t.plan.OutputFieldIds = outputFieldIDs
		t.plan.DynamicFields = t.userDynamicFields
	}

	log.Ctx(ctx).Debug("translate output fields to field ids",
		zap.Int64s("OutputFieldsID", t.OutputFieldsId),
		zap.String("requestType", t.getQueryLabel()))
	return nil
}

func (t *queryTask) hasCountStar() bool {
	for _, agg := range t.userAggregates {
		if agg.Name() == "count" && agg.FieldID() == 0 {
			return true
		}
	}
	return false
}

func (t *queryTask) CanSkipAllocTimestamp() bool {
	var consistencyLevel commonpb.ConsistencyLevel
	useDefaultConsistency := t.request.GetUseDefaultConsistency()
	if !useDefaultConsistency {
		// legacy SDK & resultful behavior
		if t.request.GetConsistencyLevel() == commonpb.ConsistencyLevel_Strong && t.request.GetGuaranteeTimestamp() > 0 {
			return true
		}
		consistencyLevel = t.request.GetConsistencyLevel()
	} else {
		collID, err := globalMetaCache.GetCollectionID(context.Background(), t.request.GetDbName(), t.request.GetCollectionName())
		if err != nil { // err is not nil if collection not exists
			log.Ctx(t.ctx).Warn("query task get collectionID failed, can't skip alloc timestamp",
				zap.String("collectionName", t.request.GetCollectionName()), zap.Error(err))
			return false
		}

		collectionInfo, err2 := globalMetaCache.GetCollectionInfo(context.Background(), t.request.GetDbName(), t.request.GetCollectionName(), collID)
		if err2 != nil {
			log.Ctx(t.ctx).Warn("query task get collection info failed, can't skip alloc timestamp",
				zap.String("collectionName", t.request.GetCollectionName()), zap.Error(err))
			return false
		}
		consistencyLevel = collectionInfo.consistencyLevel
	}
	return consistencyLevel != commonpb.ConsistencyLevel_Strong
}

func (t *queryTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_Retrieve
	t.Base.SourceID = paramtable.GetNodeID()

	collectionName := t.request.CollectionName
	t.collectionName = collectionName

	log := log.Ctx(ctx).With(zap.String("collectionName", collectionName),
		zap.Strings("partitionNames", t.request.GetPartitionNames()),
		zap.String("requestType", t.getQueryLabel()))

	if err := validateCollectionName(collectionName); err != nil {
		log.Warn("Invalid collectionName.")
		return err
	}
	log.Debug("Validate collectionName.")

	collID, err := globalMetaCache.GetCollectionID(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		log.Warn("Failed to get collection id.", zap.String("collectionName", collectionName), zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}
	t.CollectionID = collID

	colInfo, err := globalMetaCache.GetCollectionInfo(ctx, t.request.GetDbName(), collectionName, t.CollectionID)
	if err != nil {
		log.Warn("Failed to get collection info.", zap.String("collectionName", collectionName),
			zap.Int64("collectionID", t.CollectionID), zap.Error(err))
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}
	log.Debug("Get collection ID by name", zap.Int64("collectionID", t.CollectionID))

	schema, err := globalMetaCache.GetCollectionSchema(ctx, t.request.GetDbName(), t.collectionName)
	if err != nil {
		log.Warn("get collection schema failed", zap.Error(err))
		return err
	}
	t.schema = schema
	err = common.CheckNamespace(t.schema.CollectionSchema, t.request.Namespace)
	if err != nil {
		return err
	}

	t.partitionKeyMode, err = isPartitionKeyMode(ctx, t.request.GetDbName(), collectionName)
	if err != nil {
		log.Warn("check partition key mode failed", zap.Int64("collectionID", t.CollectionID), zap.Error(err))
		return err
	}
	if t.partitionKeyMode && len(t.request.GetPartitionNames()) != 0 {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("not support manually specifying the partition names if partition key mode is used"))
	}
	if t.mustUsePartitionKey && !t.partitionKeyMode {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("must use partition key in the query request " +
			"because the mustUsePartitionKey config is true"))
	}

	for _, tag := range t.request.PartitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			log.Warn("invalid partition name", zap.String("partition name", tag))
			return err
		}
	}
	log.Debug("Validate partition names.")

	// fetch search_growing from query param
	if t.RetrieveRequest.IgnoreGrowing, err = isIgnoreGrowing(t.request.GetQueryParams()); err != nil {
		return err
	}
	queryParams, err := parseQueryParams(t.request.GetQueryParams(), colInfo.queryMode == common.QueryModeLargeTopK)
	if err != nil {
		return err
	}
	if queryParams.collectionID > 0 && queryParams.collectionID != t.GetCollectionID() {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("Input collection id is not consistent to collectionID in the context," +
			"alias or database may have changed"))
	}
	if queryParams.reduceType == reduce.IReduceInOrderForBest {
		t.RetrieveRequest.ReduceStopForBest = true
	}
	t.RetrieveRequest.ReduceType = int32(queryParams.reduceType)

	t.queryParams = queryParams

	// ORDER BY requires explicit limit to prevent segment-level OOM.
	// SortBuffer loads all matching rows into memory for sorting;
	// MaxOutputSize only guards proxy reduce, not segment sorting.
	if len(queryParams.orderByFields) > 0 && queryParams.limit == typeutil.Unlimited {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("ORDER BY requires explicit limit"))
	}

	// ORDER BY with iterator is not yet supported. Current iterator relies on
	// PK-ordered pagination (plain query pipeline), while ORDER BY uses a
	// different pipeline that sorts by arbitrary fields. Future work will
	// enable iterator with user-specified ORDER BY fields.
	if len(queryParams.orderByFields) > 0 && queryParams.isIterator {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("ORDER BY with iterator is not supported"))
	}

	t.RetrieveRequest.Limit = queryParams.limit + queryParams.offset

	if t.ids != nil {
		pkField := ""
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				pkField = field.Name
			}
		}
		t.request.Expr = IDs2Expr(pkField, t.ids)
	}

	if t.queryParams.timezone != "" {
		// validated in queryParams, no need to validate again
		t.resolvedTimezoneStr = t.queryParams.timezone
		log.Debug("determine timezone from request", zap.String("user defined timezone", t.resolvedTimezoneStr))
	} else {
		t.resolvedTimezoneStr = getColTimezone(colInfo)
		log.Debug("determine timezone from collection", zap.Any("collection timezone", t.resolvedTimezoneStr))
	}

	if err := t.createPlanArgs(ctx, &planparserv2.ParserVisitorArgs{Timezone: t.resolvedTimezoneStr}); err != nil {
		return err
	}
	t.plan.GetQuery().Limit = t.RetrieveRequest.Limit

	// Aggregation queries have bounded result sizes:
	// - global aggregation (no GROUP BY) returns exactly one row
	// - GROUP BY aggregation returns at most one row per distinct group value
	// Both are safe without a limit, so exempt them from the limit requirement.
	hasAgg := len(t.userAggregates) > 0

	if planparserv2.IsAlwaysTruePlan(t.plan) && t.RetrieveRequest.Limit == typeutil.Unlimited && !hasAgg {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("empty expression should be used with limit"))
	}

	// convert partition names only when requery is false
	if !t.reQuery {
		partitionNames := t.request.GetPartitionNames()
		if t.partitionKeyMode {
			expr, err := exprutil.ParseExprFromPlan(t.plan)
			if err != nil {
				return err
			}
			partitionKeys := exprutil.ParseKeys(expr, exprutil.PartitionKey)
			hashedPartitionNames, err := assignPartitionKeys(ctx, t.request.GetDbName(), t.request.CollectionName, partitionKeys)
			if err != nil {
				return err
			}

			partitionNames = append(partitionNames, hashedPartitionNames...)
		}
		t.RetrieveRequest.PartitionIDs, err = getPartitionIDs(ctx, t.request.GetDbName(), t.request.CollectionName, partitionNames)
		if err != nil {
			return err
		}
	}

	// count(*) without GROUP BY is a single-value result, pagination is meaningless.
	// But count(*) with GROUP BY + limit is valid (limits the number of groups returned).
	if t.hasCountStar() && t.queryParams.limit != typeutil.Unlimited && len(t.GetGroupByFieldIds()) == 0 {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("count entities with pagination is not allowed"))
	}
	t.plan.Namespace = t.request.Namespace

	t.RetrieveRequest.SerializedExprPlan, err = proto.Marshal(t.plan)
	if err != nil {
		return err
	}

	// Set username for this query request,
	if username, _ := GetCurUserFromContext(ctx); username != "" {
		t.RetrieveRequest.Username = username
	}

	collectionInfo, err2 := globalMetaCache.GetCollectionInfo(ctx, t.request.GetDbName(), collectionName, t.CollectionID)
	if err2 != nil {
		log.Warn("Proxy::queryTask::PreExecute failed to GetCollectionInfo from cache",
			zap.String("collectionName", collectionName), zap.Int64("collectionID", t.CollectionID),
			zap.Error(err2))
		return err2
	}

	guaranteeTs := t.request.GetGuaranteeTimestamp()
	var consistencyLevel commonpb.ConsistencyLevel
	useDefaultConsistency := t.request.GetUseDefaultConsistency()
	t.RetrieveRequest.ConsistencyLevel = t.request.GetConsistencyLevel()
	if useDefaultConsistency {
		consistencyLevel = collectionInfo.consistencyLevel
		guaranteeTs = parseGuaranteeTsFromConsistency(guaranteeTs, t.BeginTs(), consistencyLevel)
	} else {
		consistencyLevel = t.request.GetConsistencyLevel()
		// Compatibility logic, parse guarantee timestamp
		if consistencyLevel == 0 && guaranteeTs > 0 {
			guaranteeTs = parseGuaranteeTs(guaranteeTs, t.BeginTs())
		} else {
			// parse from guarantee timestamp and user input consistency level
			guaranteeTs = parseGuaranteeTsFromConsistency(guaranteeTs, t.BeginTs(), consistencyLevel)
		}
	}

	// update actual consistency level
	accesslog.SetActualConsistencyLevel(ctx, consistencyLevel)

	// use collection schema updated timestamp if it's greater than calculate guarantee timestamp
	// this make query view updated happens before new read request happens
	// see also schema change design
	if collectionInfo.updateTimestamp > guaranteeTs {
		guaranteeTs = collectionInfo.updateTimestamp
	}

	t.GuaranteeTimestamp = guaranteeTs
	// Extract physical time for entity-level TTL (issue #47413)
	physicalTimeMs, _ := tsoutil.ParseHybridTs(guaranteeTs)
	t.EntityTtlPhysicalTime = uint64(physicalTimeMs * 1000)
	// need modify mvccTs and guaranteeTs for iterator specially
	if t.queryParams.isIterator && t.request.GetGuaranteeTimestamp() > 0 {
		t.MvccTimestamp = t.request.GetGuaranteeTimestamp()
		t.GuaranteeTimestamp = t.request.GetGuaranteeTimestamp()
	}
	t.RetrieveRequest.IsIterator = queryParams.isIterator

	if collectionInfo.collectionTTL != 0 {
		physicalTime := tsoutil.PhysicalTime(t.GetBase().GetTimestamp())
		expireTime := physicalTime.Add(-time.Duration(collectionInfo.collectionTTL))
		t.CollectionTtlTimestamps = tsoutil.ComposeTSByTime(expireTime, 0)
		// preventing overflow, abort
		if t.CollectionTtlTimestamps > t.GetBase().GetTimestamp() {
			return merr.WrapErrServiceInternal(fmt.Sprintf("ttl timestamp overflow, base timestamp: %d, ttl duration %v", t.GetBase().GetTimestamp(), collectionInfo.collectionTTL))
		}
	}
	deadline, ok := t.TraceCtx().Deadline()
	if ok {
		t.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	t.DbID = 0 // TODO
	log.Debug("Query PreExecute done.",
		zap.Uint64("guarantee_ts", guaranteeTs),
		zap.Uint64("mvcc_ts", t.GetMvccTimestamp()),
		zap.Uint64("timeout_ts", t.GetTimeoutTimestamp()),
		zap.Uint64("collection_ttl_timestamps", t.CollectionTtlTimestamps))
	return nil
}

func (t *queryTask) Execute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute query %d", t.ID()))
	defer tr.CtxElapse(ctx, "done")
	log := log.Ctx(ctx).With(zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.String("requestType", t.getQueryLabel()))

	t.resultBuf = typeutil.NewConcurrentSet[*internalpb.RetrieveResults]()
	err := t.lb.Execute(ctx, shardclient.CollectionWorkLoad{
		Db:             t.request.GetDbName(),
		CollectionID:   t.CollectionID,
		CollectionName: t.collectionName,
		Nq:             1,
		Exec:           t.queryShard,
	})
	if err != nil {
		log.Warn("fail to execute query", zap.Error(err))
		return errors.Wrap(err, "failed to query")
	}

	log.Debug("Query Execute done.")
	return nil
}

func (t *queryTask) PostExecute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder("queryTask PostExecute")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	log := log.Ctx(ctx).With(zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.String("requestType", t.getQueryLabel()))

	var err error

	toReduceResults := make([]*internalpb.RetrieveResults, 0)
	t.allQueryCnt = 0
	t.totalRelatedDataSize = 0
	t.storageCost = segcore.StorageCost{}
	select {
	case <-t.TraceCtx().Done():
		log.Warn("proxy", zap.Int64("Query: wait to finish failed, timeout!, msgID:", t.ID()))
		return nil
	default:
		log.Debug("all queries are finished or canceled")
		t.resultBuf.Range(func(res *internalpb.RetrieveResults) bool {
			toReduceResults = append(toReduceResults, res)
			t.allQueryCnt += res.GetAllRetrieveCount()
			t.storageCost.ScannedRemoteBytes += res.GetScannedRemoteBytes()
			t.storageCost.ScannedTotalBytes += res.GetScannedTotalBytes()
			t.totalRelatedDataSize += res.GetCostAggregation().GetTotalRelatedDataSize()
			log.Debug("proxy receives one query result", zap.Int64("sourceID", res.GetBase().GetSourceID()))
			return true
		})
	}

	metrics.ProxyDecodeResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), t.getQueryLabel()).Observe(0.0)
	tr.CtxRecord(ctx, "reduceResultStart")

	// Parse ORDER BY fields if present
	var orderByFields []*orderby.OrderByField
	if len(t.queryParams.orderByFields) > 0 {
		orderByFields, err = orderby.ParseOrderByFields(t.queryParams.orderByFields, t.schema.CollectionSchema)
		if err != nil {
			log.Warn("fail to parse order by fields", zap.Error(err))
			return err
		}
	}

	primaryFieldSchema, err := t.schema.GetPkField()
	if err != nil {
		log.Warn("failed to get primary field schema", zap.Error(err))
		return err
	}

	pipeline, err := NewQueryPipeline(
		t.schema.CollectionSchema,
		t.queryParams.limit,
		t.queryParams.offset,
		t.queryParams.reduceType,
		orderByFields,
		t.RetrieveRequest.GetGroupByFieldIds(),
		t.RetrieveRequest.GetAggregates(),
		t.aggregationFieldMap,
		filterSystemFields(t.RetrieveRequest.GetOutputFieldsId()),
	)
	if err != nil {
		log.Warn("fail to create query pipeline", zap.Error(err))
		return err
	}
	t.result, err = pipeline.Execute(ctx, toReduceResults)
	if err != nil {
		log.Warn("fail to reduce query result", zap.Error(err))
		return err
	}

	// FieldName/Type/IsDynamic setting and timestamp column removal are now
	// handled by complementFieldOperator in the pipeline (for non-aggregation queries).
	// Only geometry WKB→WKT conversion still needs to happen here.
	for i, fieldData := range t.result.FieldsData {
		if fieldData.Type == schemapb.DataType_Geometry {
			if err := validateGeometryFieldSearchResult(&t.result.FieldsData[i]); err != nil {
				log.Warn("fail to validate geometry field search result", zap.Error(err))
				return err
			}
		}
		if fieldData.Type == schemapb.DataType_Mol {
			if err := validateMOLFieldSearchResult(&t.result.FieldsData[i]); err != nil {
				log.Warn("fail to validate MOL field search result", zap.Error(err))
				return err
			}
		}
	}
	t.result.OutputFields = t.userOutputFields
	if !t.reQuery {
		reconstructStructFieldDataForQuery(t.result, t.schema.CollectionSchema)
	}

	t.result.CollectionName = t.collectionName
	t.result.PrimaryFieldName = primaryFieldSchema.GetName()
	metrics.ProxyReduceResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), t.getQueryLabel()).Observe(float64(tr.RecordSpan().Milliseconds()))

	if t.queryParams.isIterator && t.request.GetGuaranteeTimestamp() == 0 {
		// first page for iteration, need to set up sessionTs for iterator
		t.result.SessionTs = getMaxMvccTsFromChannels(t.channelsMvcc, t.BeginTs())
	}
	if !t.reQuery {
		if len(t.queryParams.extractTimeFields) > 0 {
			log.Debug("extracting fields for timestamptz", zap.Strings("fields", t.queryParams.extractTimeFields))
			err = extractFieldsFromResults(t.result.GetFieldsData(), t.resolvedTimezoneStr, t.queryParams.extractTimeFields)
			if err != nil {
				log.Warn("fail to extract fields for timestamptz", zap.Error(err))
				return err
			}
		} else {
			log.Debug("translate timestamp to ISO string", zap.String("user define timezone", t.queryParams.timezone))
			err = timestamptzUTC2IsoStr(t.result.GetFieldsData(), t.resolvedTimezoneStr)
			if err != nil {
				log.Warn("fail to translate timestamp", zap.Error(err))
				return err
			}
		}
	}
	log.Debug("Query PostExecute done")
	return nil
}

func (t *queryTask) IsSubTask() bool {
	return t.reQuery
}

func (t *queryTask) queryShard(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
	needOverrideMvcc := false
	mvccTs := t.MvccTimestamp
	if len(t.channelsMvcc) > 0 {
		mvccTs, needOverrideMvcc = t.channelsMvcc[channel]
		// In fast mode, if there is no corresponding channel in channelsMvcc, quickly skip this query.
		if !needOverrideMvcc && t.fastSkip {
			return nil
		}
	}

	retrieveReq := typeutil.Clone(t.RetrieveRequest)
	retrieveReq.GetBase().TargetID = nodeID
	if needOverrideMvcc && mvccTs > 0 {
		retrieveReq.MvccTimestamp = mvccTs
		retrieveReq.GuaranteeTimestamp = mvccTs
	}
	retrieveReq.ConsistencyLevel = t.ConsistencyLevel
	req := &querypb.QueryRequest{
		Req:         retrieveReq,
		DmlChannels: []string{channel},
		Scope:       querypb.DataScope_All,
	}

	log := log.Ctx(ctx).With(zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.Int64("nodeID", nodeID),
		zap.String("channel", channel))

	result, err := qn.Query(ctx, req)
	if err != nil {
		log.Warn("QueryNode query return error", zap.Error(err))
		t.shardclientMgr.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Warn("QueryNode is not shardLeader")
		t.shardclientMgr.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		return merr.Error(result.GetStatus())
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("QueryNode query result error", zap.Any("errorCode", result.GetStatus().GetErrorCode()), zap.String("reason", result.GetStatus().GetReason()))
		return errors.Wrapf(merr.Error(result.GetStatus()), "fail to Query on QueryNode %d", nodeID)
	}

	log.Debug("get query result")
	t.resultBuf.Insert(result)
	t.lb.UpdateCostMetrics(nodeID, result.CostAggregation)
	return nil
}

// IDs2Expr converts ids slices to bool expresion with specified field name
func IDs2Expr(fieldName string, ids *schemapb.IDs) string {
	var idsStr string
	switch ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		idsStr = strings.Trim(strings.Join(strings.Fields(fmt.Sprint(ids.GetIntId().GetData())), ", "), "[]")
	case *schemapb.IDs_StrId:
		strs := lo.Map(ids.GetStrId().GetData(), func(str string, _ int) string {
			return fmt.Sprintf("\"%s\"", str)
		})
		idsStr = strings.Trim(strings.Join(strs, ", "), "[]")
	}

	return fieldName + " in [ " + idsStr + " ]"
}

func reduceRetrieveResults(ctx context.Context, retrieveResults []*internalpb.RetrieveResults, queryParams *queryParams) (*milvuspb.QueryResults, error) {
	log.Ctx(ctx).Debug("reduceInternalRetrieveResults", zap.Int("len(retrieveResults)", len(retrieveResults)))
	var (
		ret     = &milvuspb.QueryResults{}
		loopEnd int
	)

	// Detect if this is an element-level query
	isElementLevel := len(retrieveResults) > 0 && retrieveResults[0].GetElementLevel()

	validRetrieveResults := []*internalpb.RetrieveResults{}
	for _, r := range retrieveResults {
		size := typeutil.GetSizeOfIDs(r.GetIds())
		if r == nil || len(r.GetFieldsData()) == 0 || size == 0 {
			continue
		}
		// Validate element-level consistency: if any result is element-level, all must be
		if isElementLevel && !r.GetElementLevel() {
			return nil, fmt.Errorf("inconsistent element-level flag: expected all results to be element-level")
		}
		// Validate element_indices length matches ids length for element-level
		if isElementLevel && len(r.GetElementIndices()) != size {
			return nil, fmt.Errorf("element_indices length (%d) does not match ids length (%d)", len(r.GetElementIndices()), size)
		}
		validRetrieveResults = append(validRetrieveResults, r)
		loopEnd += size
	}

	if len(validRetrieveResults) == 0 {
		return ret, nil
	}

	cursors := make([]int64, len(validRetrieveResults))
	idxComputers := make([]*typeutil.FieldDataIdxComputer, len(validRetrieveResults))
	for i, vr := range validRetrieveResults {
		idxComputers[i] = typeutil.NewFieldDataIdxComputer(vr.GetFieldsData())
	}

	// Used in element-level query to limit the number of elements returned
	var elementLimit int = -1
	if queryParams != nil && queryParams.limit != typeutil.Unlimited {
		// IReduceInOrderForBest will try to get as many results as possible
		// so loopEnd in this case will be set to the sum of all results' size
		// to get as many qualified results as possible
		if reduce.ShouldUseInputLimit(queryParams.reduceType) {
			if !isElementLevel {
				loopEnd = int(queryParams.limit)
			}
			elementLimit = int(queryParams.limit)
		}
	}

	// handle offset
	if queryParams != nil && queryParams.offset > 0 {
		var skipped int64
		for skipped < queryParams.offset {
			sel, drainOneResult := typeutil.SelectMinPK(validRetrieveResults, cursors)
			if sel == -1 || (reduce.ShouldStopWhenDrained(queryParams.reduceType) && drainOneResult) {
				return ret, nil
			}
			if isElementLevel {
				elemIndices := validRetrieveResults[sel].GetElementIndices()[cursors[sel]]
				indicesCount := int64(len(elemIndices.GetIndices()))
				if skipped+indicesCount > queryParams.offset {
					elemIndices.Indices = elemIndices.Indices[queryParams.offset-skipped:]
					break
				} else {
					skipped += indicesCount
				}
			} else {
				skipped++
			}
			cursors[sel]++
		}
	}

	ret.FieldsData = typeutil.PrepareResultFieldData(validRetrieveResults[0].GetFieldsData(), int64(loopEnd))
	var retSize int64
	var availableCount int // for element-level: element count; for doc-level: doc count
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	for j := 0; j < loopEnd && (elementLimit == -1 || availableCount < elementLimit); j++ {
		sel, drainOneResult := typeutil.SelectMinPK(validRetrieveResults, cursors)
		if sel == -1 || (reduce.ShouldStopWhenDrained(queryParams.reduceType) && drainOneResult) {
			break
		}

		// Get element indices for element-level query
		var elemCount int = 1 // default for doc-level
		if isElementLevel {
			elemIndices := validRetrieveResults[sel].GetElementIndices()[cursors[sel]]
			elemCount = len(elemIndices.GetIndices())
			ret.ElementIndices = append(ret.ElementIndices, convertInternalElementIndicesToMilvus(elemIndices))
		}

		fieldIdxs := idxComputers[sel].Compute(cursors[sel])
		retSize += typeutil.AppendFieldData(ret.FieldsData, validRetrieveResults[sel].GetFieldsData(), cursors[sel], fieldIdxs...)
		availableCount += elemCount

		// limit retrieve result to avoid oom
		if retSize > maxOutputSize {
			return nil, fmt.Errorf("query results exceed the maxOutputSize Limit %d", maxOutputSize)
		}

		cursors[sel]++
	}

	return ret, nil
}

// convertInternalElementIndicesToMilvus converts internalpb.ElementIndices (int32) to milvuspb.ElementIndices (int64)
func convertInternalElementIndicesToMilvus(src *internalpb.ElementIndices) *milvuspb.ElementIndices {
	if src == nil {
		return nil
	}
	indices := src.GetIndices()
	data := make([]int64, len(indices))
	for i, v := range indices {
		data[i] = int64(v)
	}
	return &milvuspb.ElementIndices{
		Indices: &schemapb.LongArray{Data: data},
	}
}

func (t *queryTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *queryTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *queryTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *queryTask) Name() string {
	return RetrieveTaskName
}

func (t *queryTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *queryTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *queryTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *queryTask) SetTs(ts Timestamp) {
	if t.reQuery && t.Base.Timestamp != 0 {
		return
	}
	t.Base.Timestamp = ts
}

func (t *queryTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_Retrieve
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}
