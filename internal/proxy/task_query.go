package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
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
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/exprutil"
	"github.com/milvus-io/milvus/internal/util/reduce"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
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
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// FromSourceDefinition defines data source configuration for from parameter
type FromSourceDefinition struct {
	Alias   string      `json:"alias"`             // alias name, e.g., "a", "b"
	Source  string      `json:"source"`            // source type: collection, external
	Filter  string      `json:"filter,omitempty"`  // filter condition, e.g., "id IN [1,2,3]"
	Vectors [][]float32 `json:"vectors,omitempty"` // external vector data
}

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

	resultBuf *typeutil.ConcurrentSet[*internalpb.RetrieveResults]

	plan             *planpb.PlanNode
	partitionKeyMode bool
	lb               LBPolicy
	channelsMvcc     map[string]Timestamp
	fastSkip         bool

	reQuery              bool
	allQueryCnt          int64
	totalRelatedDataSize int64
	mustUsePartitionKey  bool

	// distance query extensions
	isDistanceQuery bool
	fromSources     []*planpb.QueryFromSource
	outputAliases   []string
	aliasMap        map[string]*schemaInfo
	externalVectors map[string]*planpb.ExternalVectorData
}

type queryParams struct {
	limit        int64
	offset       int64
	reduceType   reduce.IReduceType
	isIterator   bool
	collectionID int64
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
				// check if it's a distance expression (simple check)
				if strings.Contains(reqField, "distance(") && strings.Contains(reqField, " as ") {
					// this is a distance expression, no need to validate field existence
					// distance expressions will be processed in later stages
					continue
				}
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

// translateToOutputFieldIDsForDistanceQuery translates output fields for distance queries (supports alias syntax)
func translateToOutputFieldIDsForDistanceQuery(outputFields []string, schema *schemapb.CollectionSchema, isDistanceQuery bool) ([]UniqueID, error) {
	if !isDistanceQuery {
		// for non-distance queries, use original logic
		return translateToOutputFieldIDs(outputFields, schema)
	}

	// special handling for distance queries
	outputFieldIDs := make([]UniqueID, 0, len(outputFields)+1)
	
	if len(outputFields) == 0 {
		// if no output fields specified, return primary key field
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				outputFieldIDs = append(outputFieldIDs, field.FieldID)
			}
		}
	} else {
		for _, reqField := range outputFields {
			var fieldFound bool
			
			// check if it's an alias field (e.g., "a.id as id1", "_distance")
			if isAliasField(reqField) {
				// for alias fields, skip validation temporarily and let backend handle
				// this includes "_distance" and fields like "a.id as id1"
				fieldFound = true
			} else {
				// for regular fields, use original logic
				for _, field := range schema.Fields {
					if reqField == field.Name {
						outputFieldIDs = append(outputFieldIDs, field.FieldID)
						fieldFound = true
						break
					}
				}
			}
			
			if !fieldFound {
				return nil, fmt.Errorf("field %s not exist", reqField)
			}
		}

		// pk field needs to be in output field list (but not applicable to distance queries)
		// distance queries may have special output formats, primary key not mandatory
	}

	return outputFieldIDs, nil
}

// isAliasField checks if field is an alias field
func isAliasField(fieldName string) bool {
	// check distance field
	if fieldName == "_distance" {
		return true
	}
	
	// check distance function expression: e.g., "distance(vector, vector, 'L2') as _distance"
	if strings.HasPrefix(fieldName, "distance(") {
		return true
	}
	
	// check alias syntax: e.g., "a.id as id1", "b.vector as vec"
	if strings.Contains(fieldName, " as ") {
		return true
	}
	
	// check simple alias reference: e.g., "a.id", "b.vector"
	if strings.Contains(fieldName, ".") {
		return true
	}
	
	// check common computed field patterns
	computedFields := []string{"_distance", "_score", "_rank"}
	for _, computed := range computedFields {
		if fieldName == computed {
			return true
		}
	}
	
	return false
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
func parseQueryParams(queryParamsPair []*commonpb.KeyValuePair) (*queryParams, error) {
	var (
		limit             int64
		offset            int64
		reduceStopForBest bool
		isIterator        bool
		err               error
		collectionID      int64
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

	limitStr, err := funcutil.GetAttrByKeyFromRepeatedKV(LimitKey, queryParamsPair)
	// if limit is not provided
	if err != nil {
		return &queryParams{limit: typeutil.Unlimited, reduceType: reduceType, isIterator: isIterator}, nil
	}
	limit, err = strconv.ParseInt(limitStr, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("%s [%s] is invalid", LimitKey, limitStr)
	}

	offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, queryParamsPair)
	// if offset is provided
	if err == nil {
		offset, err = strconv.ParseInt(offsetStr, 0, 64)
		if err != nil {
			return nil, fmt.Errorf("%s [%s] is invalid", OffsetKey, offsetStr)
		}
	}

	// validate max result window.
	if err = validateMaxQueryResultWindow(offset, limit); err != nil {
		return nil, fmt.Errorf("invalid max query result window, %w", err)
	}

	return &queryParams{
		limit:        limit,
		offset:       offset,
		reduceType:   reduceType,
		isIterator:   isIterator,
		collectionID: collectionID,
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
		metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "query", metrics.FailLabel).Observe(float64(time.Since(start).Milliseconds()))
		return nil, merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("failed to create query plan: %v", err))
	}
	metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "query", metrics.SuccessLabel).Observe(float64(time.Since(start).Milliseconds()))
	plan.Node.(*planpb.PlanNode_Query).Query.IsCount = true

	return plan, nil
}

func (t *queryTask) createPlan(ctx context.Context) error {
	schema := t.schema

	cntMatch := matchCountRule(t.request.GetOutputFields())
	if cntMatch {
		var err error
		t.plan, err = createCntPlan(t.request.GetExpr(), schema.schemaHelper, t.request.GetExprTemplateValues())
		t.userOutputFields = []string{"count(*)"}
		return err
	}

	var err error
	if t.plan == nil {
		start := time.Now()

		// choose different plan generator based on whether it's a distance query
		if t.isDistanceQuery {
			t.plan, err = planparserv2.CreateDistanceQueryPlan(
				schema.schemaHelper,
				t.request.Expr,
				t.fromSources,
				t.outputAliases,
				t.request.GetExprTemplateValues(),
			)
		} else {
			// regular query, use standard plan generator
			t.plan, err = planparserv2.CreateRetrievePlan(schema.schemaHelper, t.request.Expr, t.request.GetExprTemplateValues())
		}

		if err != nil {
			metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "query", metrics.FailLabel).Observe(float64(time.Since(start).Milliseconds()))
			return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("failed to create query plan: %v", err))
		}
		metrics.ProxyParseExpressionLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), "query", metrics.SuccessLabel).Observe(float64(time.Since(start).Milliseconds()))
	}

	// for distance queries, use special field translation function to handle aliases
	if t.isDistanceQuery {
		t.translatedOutputFields, t.userOutputFields, t.userDynamicFields, _, err = translateOutputFieldsForDistanceQuery(t.request.OutputFields, t.schema, false)
	} else {
		t.translatedOutputFields, t.userOutputFields, t.userDynamicFields, _, err = translateOutputFields(t.request.OutputFields, t.schema, false)
	}
	if err != nil {
		return err
	}

	outputFieldIDs, err := translateToOutputFieldIDsForDistanceQuery(t.translatedOutputFields, schema.CollectionSchema, t.isDistanceQuery)
	if err != nil {
		log.Ctx(ctx).Info("Field translation result",
			zap.Strings("outputFields", t.translatedOutputFields),
			zap.Bool("isDistanceQuery", t.isDistanceQuery),
			zap.Error(err))
		return err
	}
	outputFieldIDs = append(outputFieldIDs, common.TimeStampField)
	t.RetrieveRequest.OutputFieldsId = outputFieldIDs
	t.plan.OutputFieldIds = outputFieldIDs
	t.plan.DynamicFields = t.userDynamicFields
	log.Ctx(ctx).Debug("translate output fields to field ids",
		zap.Int64s("OutputFieldsID", t.OutputFieldsId),
		zap.String("requestType", "query"))

	return nil
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
		zap.String("requestType", "query"))

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
	log.Debug("Get collection ID by name", zap.Int64("collectionID", t.CollectionID))

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

	queryParams, err := parseQueryParams(t.request.GetQueryParams())
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
	t.RetrieveRequest.Limit = queryParams.limit + queryParams.offset

	schema, err := globalMetaCache.GetCollectionSchema(ctx, t.request.GetDbName(), t.collectionName)
	if err != nil {
		log.Warn("get collection schema failed", zap.Error(err))
		return err
	}
	t.schema = schema

	if t.ids != nil {
		pkField := ""
		for _, field := range schema.Fields {
			if field.IsPrimaryKey {
				pkField = field.Name
			}
		}
		t.request.Expr = IDs2Expr(pkField, t.ids)
	}

	// detect and process distance queries
	if err := t.processDistanceQuery(ctx); err != nil {
		return err
	}

	if err := t.createPlan(ctx); err != nil {
		return err
	}
	t.plan.Node.(*planpb.PlanNode_Query).Query.Limit = t.RetrieveRequest.Limit

	if planparserv2.IsAlwaysTruePlan(t.plan) && t.RetrieveRequest.Limit == typeutil.Unlimited {
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

	// count with pagination
	if t.plan.GetQuery().GetIsCount() && t.queryParams.limit != typeutil.Unlimited {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("count entities with pagination is not allowed"))
	}

	t.RetrieveRequest.IsCount = t.plan.GetQuery().GetIsCount()
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

	// use collection schema updated timestamp if it's greater than calculate guarantee timestamp
	// this make query view updated happens before new read request happens
	// see also schema change design
	if collectionInfo.updateTimestamp > guaranteeTs {
		guaranteeTs = collectionInfo.updateTimestamp
	}

	t.GuaranteeTimestamp = guaranteeTs
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
		zap.String("requestType", "query"))

	t.resultBuf = typeutil.NewConcurrentSet[*internalpb.RetrieveResults]()
	err := t.lb.Execute(ctx, CollectionWorkLoad{
		db:             t.request.GetDbName(),
		collectionID:   t.CollectionID,
		collectionName: t.collectionName,
		nq:             1,
		exec:           t.queryShard,
	})
	if err != nil {
		log.Warn("fail to execute query", zap.Error(err))
		return errors.Wrap(err, "failed to query")
	}

	log.Debug("Query Execute done.")
	return nil
}

// FieldsData in results are flattened, so we need to reconstruct the struct fields
func reconstructStructFieldData(results *milvuspb.QueryResults, schema *schemapb.CollectionSchema) {
	if len(results.OutputFields) == 1 && results.OutputFields[0] == "count(*)" {
		return
	}

	if len(schema.StructArrayFields) == 0 {
		return
	}

	regularFieldIDs := make(map[int64]interface{})
	subFieldToStructMap := make(map[int64]int64)
	groupedStructFields := make(map[int64][]*schemapb.FieldData)
	structFieldNames := make(map[int64]string)
	reconstructedOutputFields := make([]string, 0, len(results.FieldsData))

	// record all regular field IDs
	for _, field := range schema.Fields {
		regularFieldIDs[field.GetFieldID()] = nil
	}

	// build the mapping from sub-field ID to struct field ID
	for _, structField := range schema.StructArrayFields {
		for _, subField := range structField.GetFields() {
			subFieldToStructMap[subField.GetFieldID()] = structField.GetFieldID()
		}
		structFieldNames[structField.GetFieldID()] = structField.GetName()
	}

	fieldsData := make([]*schemapb.FieldData, 0, len(results.FieldsData))
	for _, field := range results.FieldsData {
		fieldID := field.GetFieldId()
		if _, ok := regularFieldIDs[fieldID]; ok {
			fieldsData = append(fieldsData, field)
			reconstructedOutputFields = append(reconstructedOutputFields, field.GetFieldName())
		} else {
			structFieldID := subFieldToStructMap[fieldID]
			groupedStructFields[structFieldID] = append(groupedStructFields[structFieldID], field)
		}
	}

	for structFieldID, fields := range groupedStructFields {
		fieldData := &schemapb.FieldData{
			FieldName: structFieldNames[structFieldID],
			FieldId:   structFieldID,
			Type:      schemapb.DataType_ArrayOfStruct,
			Field:     &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: fields}},
		}
		fieldsData = append(fieldsData, fieldData)
		reconstructedOutputFields = append(reconstructedOutputFields, structFieldNames[structFieldID])
	}

	results.FieldsData = fieldsData
	results.OutputFields = reconstructedOutputFields
}

func (t *queryTask) PostExecute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder("queryTask PostExecute")
	defer func() {
		tr.CtxElapse(ctx, "done")
	}()

	log := log.Ctx(ctx).With(zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.String("requestType", "query"))

	var err error

	toReduceResults := make([]*internalpb.RetrieveResults, 0)
	t.allQueryCnt = 0
	t.totalRelatedDataSize = 0
	select {
	case <-t.TraceCtx().Done():
		log.Warn("proxy", zap.Int64("Query: wait to finish failed, timeout!, msgID:", t.ID()))
		return nil
	default:
		log.Debug("all queries are finished or canceled")
		t.resultBuf.Range(func(res *internalpb.RetrieveResults) bool {
			toReduceResults = append(toReduceResults, res)
			t.allQueryCnt += res.GetAllRetrieveCount()
			t.totalRelatedDataSize += res.GetCostAggregation().GetTotalRelatedDataSize()
			log.Debug("proxy receives one query result", zap.Int64("sourceID", res.GetBase().GetSourceID()))
			return true
		})
	}

	metrics.ProxyDecodeResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel).Observe(0.0)
	tr.CtxRecord(ctx, "reduceResultStart")

	reducer := createMilvusReducer(ctx, t.queryParams, t.RetrieveRequest, t.schema.CollectionSchema, t.plan, t.collectionName)

	t.result, err = reducer.Reduce(toReduceResults)
	if err != nil {
		log.Warn("fail to reduce query result", zap.Error(err))
		return err
	}
	t.result.OutputFields = t.userOutputFields
	reconstructStructFieldData(t.result, t.schema.CollectionSchema)

	primaryFieldSchema, err := t.schema.GetPkField()
	if err != nil {
		log.Warn("failed to get primary field schema", zap.Error(err))
		return err
	}
	t.result.PrimaryFieldName = primaryFieldSchema.GetName()
	metrics.ProxyReduceResultLatency.WithLabelValues(strconv.FormatInt(paramtable.GetNodeID(), 10), metrics.QueryLabel).Observe(float64(tr.RecordSpan().Milliseconds()))

	if t.queryParams.isIterator && t.request.GetGuaranteeTimestamp() == 0 {
		// first page for iteration, need to set up sessionTs for iterator
		t.result.SessionTs = getMaxMvccTsFromChannels(t.channelsMvcc, t.BeginTs())
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
		globalMetaCache.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Warn("QueryNode is not shardLeader")
		globalMetaCache.DeprecateShardCache(t.request.GetDbName(), t.collectionName)
		return errInvalidShardLeaders
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

	validRetrieveResults := []*internalpb.RetrieveResults{}
	for _, r := range retrieveResults {
		size := typeutil.GetSizeOfIDs(r.GetIds())
		if r == nil || len(r.GetFieldsData()) == 0 || size == 0 {
			continue
		}
		validRetrieveResults = append(validRetrieveResults, r)
		loopEnd += size
	}

	if len(validRetrieveResults) == 0 {
		return ret, nil
	}

	cursors := make([]int64, len(validRetrieveResults))

	if queryParams != nil && queryParams.limit != typeutil.Unlimited {
		// IReduceInOrderForBest will try to get as many results as possible
		// so loopEnd in this case will be set to the sum of all results' size
		// to get as many qualified results as possible
		if reduce.ShouldUseInputLimit(queryParams.reduceType) {
			loopEnd = int(queryParams.limit)
		}
	}

	// handle offset
	if queryParams != nil && queryParams.offset > 0 {
		for i := int64(0); i < queryParams.offset; i++ {
			sel, drainOneResult := typeutil.SelectMinPK(validRetrieveResults, cursors)
			if sel == -1 || (reduce.ShouldStopWhenDrained(queryParams.reduceType) && drainOneResult) {
				return ret, nil
			}
			cursors[sel]++
		}
	}

	ret.FieldsData = typeutil.PrepareResultFieldData(validRetrieveResults[0].GetFieldsData(), int64(loopEnd))
	var retSize int64
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	for j := 0; j < loopEnd; j++ {
		sel, drainOneResult := typeutil.SelectMinPK(validRetrieveResults, cursors)
		if sel == -1 || (reduce.ShouldStopWhenDrained(queryParams.reduceType) && drainOneResult) {
			break
		}
		retSize += typeutil.AppendFieldData(ret.FieldsData, validRetrieveResults[sel].GetFieldsData(), cursors[sel])

		// limit retrieve result to avoid oom
		if retSize > maxOutputSize {
			return nil, fmt.Errorf("query results exceed the maxOutputSize Limit %d", maxOutputSize)
		}

		cursors[sel]++
	}

	return ret, nil
}

func reduceRetrieveResultsAndFillIfEmpty(ctx context.Context, retrieveResults []*internalpb.RetrieveResults, queryParams *queryParams, outputFieldsID []int64, schema *schemapb.CollectionSchema) (*milvuspb.QueryResults, error) {
	result, err := reduceRetrieveResults(ctx, retrieveResults, queryParams)
	if err != nil {
		return nil, err
	}

	// filter system fields.
	filtered := filterSystemFields(outputFieldsID)
	if err := typeutil2.FillRetrieveResultIfEmpty(typeutil2.NewMilvusResult(result), filtered, schema); err != nil {
		return nil, fmt.Errorf("failed to fill retrieve results: %s", err.Error())
	}

	return result, nil
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

// processDistanceQuery detects and processes distance queries
func (t *queryTask) processDistanceQuery(ctx context.Context) error {
	log := log.Ctx(ctx).With(zap.String("collectionName", t.collectionName))

	// detect if contains distance function
	t.isDistanceQuery = t.containsDistanceFunction()

	// add debug log
	log.Info("Distance query detection result", 
		zap.String("expr", t.request.Expr),
		zap.Bool("isDistanceQuery", t.isDistanceQuery))

	if !t.isDistanceQuery {
		return nil
	}

	log.Debug("Detected distance query", zap.String("expr", t.request.Expr))

	// parse from parameter to support multi-source queries
	if err := t.parseFromSources(); err != nil {
		return err
	}

	// initialize alias mapping
	t.aliasMap = make(map[string]*schemaInfo)
	t.externalVectors = make(map[string]*planpb.ExternalVectorData)

	// set distance query flag in request
	t.RetrieveRequest.IsDistanceQuery = t.isDistanceQuery
	t.RetrieveRequest.FromSources = t.fromSources
	t.RetrieveRequest.OutputAliases = t.outputAliases

	log.Debug("Distance query processing completed",
		zap.Bool("isDistanceQuery", t.isDistanceQuery),
		zap.Int("fromSourcesCount", len(t.fromSources)))

	return nil
}

// containsDistanceFunction detects if expression contains distance function
func (t *queryTask) containsDistanceFunction() bool {
	expr := t.request.Expr
	log := log.Ctx(t.ctx).With(zap.String("method", "containsDistanceFunction"))
	
	log.Debug("Checking distance function",
		zap.String("expr", expr),
		zap.Strings("outputFields", t.request.OutputFields))
	
	// check distance function in WHERE clause
	if expr != "" && t.hasDistanceInString(expr) {
		log.Debug("Found distance function in expr")
		return true
	}

	// check distance function in output_fields
	for _, field := range t.request.OutputFields {
		if t.hasDistanceInString(field) {
			log.Debug("Found distance function in output_fields", zap.String("field", field))
			return true
		}
	}

	log.Debug("No distance function found")
	return false
}

// hasDistanceInString checks if string contains distance function
func (t *queryTask) hasDistanceInString(text string) bool {
	if text == "" {
		return false
	}

	// find all possible distance( occurrences
	distanceKeywords := []string{"distance(", "DISTANCE("}

	for _, keyword := range distanceKeywords {
		if strings.Contains(text, keyword) {
			log.Ctx(t.ctx).Debug("Found distance keyword", 
				zap.String("text", text), 
				zap.String("keyword", keyword))
			return true
		}
	}

	return false
}

// parseFromSources 解析 from 参数
func (t *queryTask) parseFromSources() error {
	logger := log.Ctx(context.Background())
	
	// 调试：检查QueryParams是否存在
	logger.Info("parseFromSources开始",
		zap.Bool("hasQueryParams", t.request.QueryParams != nil),
		zap.Int("queryParamsCount", len(t.request.QueryParams)))
	
	if t.request.QueryParams != nil {
		for i, param := range t.request.QueryParams {
			logger.Info("QueryParam详情",
				zap.Int("index", i),
				zap.String("key", param.Key),
				zap.String("value", param.Value))
		}
	}
	
	// 额外调试：打印所有参数的详细信息
	logger.Info("所有QueryParams详细信息",
		zap.Any("queryParams", t.request.QueryParams))
	
	// 检查查询参数中是否有 from 参数
	if t.request.QueryParams == nil || len(t.request.QueryParams) == 0 {
		// 没有 from 参数是正常的，使用默认数据源
		logger.Info("没有QueryParams，使用默认数据源")
		return nil
	}

	// 查找 from 参数
	var fromParam string
	for _, param := range t.request.QueryParams {
		if param.Key == "from" || param.Key == "FROM" {
			fromParam = param.Value
			break
		}
	}

	if fromParam == "" {
		// 没有找到 from 参数，使用默认行为
		return nil
	}

	logger.Debug("开始解析 from 参数",
		zap.String("fromParam", fromParam))

	// Parse JSON format from parameter with optimized parsing for large JSON
	fromSources, err := t.parseFromSourcesJSON(fromParam)
	if err != nil {
		return fmt.Errorf("failed to parse from parameter JSON: %w", err)
	}

	// Validate parsing results
	if len(fromSources) == 0 {
		return fmt.Errorf("from parameter cannot be an empty array")
	}

	// 转换为协议格式并验证
	var protobufSources []*planpb.QueryFromSource

	// 使用更高效的别名验证器
	aliasValidator := newAliasValidator()

	for i, source := range fromSources {
		// 验证必需字段
		if err := t.validateFromSource(&source, i); err != nil {
			return err
		}

		// 检查别名重复（使用高效的验证器）
		if err := aliasValidator.validateAlias(source.Alias, i); err != nil {
			return err
		}

		// 转换为协议格式
		pbSource, err := t.convertToProtobufFromSource(&source)
		if err != nil {
			return fmt.Errorf("转换数据源 %d 失败: %w", i, err)
		}

		protobufSources = append(protobufSources, pbSource)
	}

	// 获取验证摘要信息
	summary := aliasValidator.getSummary()
	logger.Debug("别名验证完成",
		zap.Int("uniqueAliases", summary.UniqueCount),
		zap.Strings("allAliases", summary.AllAliases))

	// 保存解析结果
	t.fromSources = protobufSources

	// 构建别名映射
	if err := t.buildAliasMap(); err != nil {
		return fmt.Errorf("构建别名映射失败: %w", err)
	}

	logger.Debug("from 参数解析完成",
		zap.Int("sourceCount", len(t.fromSources)),
		zap.Strings("aliases", t.getAliases()))

	return nil
}

// validateFromSource 验证数据源定义
func (t *queryTask) validateFromSource(source *FromSourceDefinition, index int) error {
	// 验证别名
	if source.Alias == "" {
		return fmt.Errorf("数据源 %d 缺少别名 (alias)", index)
	}

	// 验证别名格式 - 只允许字母、数字和下划线
	if !isValidAlias(source.Alias) {
		return fmt.Errorf("数据源 %d 别名格式无效: %s (只允许字母、数字和下划线)", index, source.Alias)
	}

	// 验证数据源类型
	if source.Source == "" {
		return fmt.Errorf("数据源 %d 缺少数据源类型 (source)", index)
	}

	switch source.Source {
	case "collection":
		// 验证集合数据源
		if err := t.validateCollectionSource(source, index); err != nil {
			return err
		}
	case "external":
		// 验证外部数据源
		if err := t.validateExternalSource(source, index); err != nil {
			return err
		}
	default:
		return fmt.Errorf("不支持的数据源类型: %s (数据源 %d, 别名: %s)", source.Source, index, source.Alias)
	}

	return nil
}

// validateCollectionSource 验证集合数据源
func (t *queryTask) validateCollectionSource(source *FromSourceDefinition, index int) error {
	// 集合数据源需要有过滤条件
	if source.Filter == "" {
		return fmt.Errorf("collection 数据源 %d (别名: %s) 缺少过滤条件 (filter)", index, source.Alias)
	}

	// 验证过滤条件格式
	if err := t.validateFilterExpression(source.Filter); err != nil {
		return fmt.Errorf("collection 数据源 %d (别名: %s) 过滤条件无效: %w", index, source.Alias, err)
	}

	// 集合数据源不应该有向量数据
	if len(source.Vectors) > 0 {
		return fmt.Errorf("collection 数据源 %d (别名: %s) 不应该包含向量数据", index, source.Alias)
	}

	return nil
}

// validateExternalSource 验证外部数据源
func (t *queryTask) validateExternalSource(source *FromSourceDefinition, index int) error {
	// 外部数据源需要有向量数据
	if len(source.Vectors) == 0 {
		return fmt.Errorf("external 数据源 %d (别名: %s) 缺少向量数据 (vectors)", index, source.Alias)
	}

	// 验证向量数量限制
	const maxVectors = 10000 // 防止内存过度使用
	if len(source.Vectors) > maxVectors {
		return fmt.Errorf("external 数据源 %d (别名: %s) 向量数量过多: %d (最大允许: %d)",
			index, source.Alias, len(source.Vectors), maxVectors)
	}

	// 验证向量维度一致性和合理性
	if len(source.Vectors) > 0 {
		expectedDim := len(source.Vectors[0])

		// 验证维度范围
		const maxDimension = 32768 // 最大维度限制
		if expectedDim == 0 {
			return fmt.Errorf("external 数据源 %d (别名: %s) 向量维度不能为0", index, source.Alias)
		}
		if expectedDim > maxDimension {
			return fmt.Errorf("external 数据源 %d (别名: %s) 向量维度过大: %d (最大允许: %d)",
				index, source.Alias, expectedDim, maxDimension)
		}

		// 验证所有向量维度一致性
		for i, vector := range source.Vectors {
			if len(vector) != expectedDim {
				return fmt.Errorf("external 数据源 %d (别名: %s) 向量 %d 维度不一致: 期望 %d, 实际 %d",
					index, source.Alias, i, expectedDim, len(vector))
			}

			// 验证向量值的有效性（不能是NaN或无穷大）
			for j, val := range vector {
				if !isValidFloat32(val) {
					return fmt.Errorf("external 数据源 %d (别名: %s) 向量 %d 位置 %d 的值无效: %f",
						index, source.Alias, i, j, val)
				}
			}
		}
	}

	// 外部数据源如果有filter，需要验证
	if source.Filter != "" {
		if err := t.validateFilterExpression(source.Filter); err != nil {
			return fmt.Errorf("external 数据源 %d (别名: %s) 过滤条件无效: %w", index, source.Alias, err)
		}
	}

	return nil
}

// validateFilterExpression 验证过滤表达式
func (t *queryTask) validateFilterExpression(filter string) error {
	// 基本的表达式验证
	if len(filter) == 0 {
		return fmt.Errorf("过滤表达式不能为空")
	}

	if len(filter) > 10000 { // 防止过长的表达式
		return fmt.Errorf("过滤表达式过长: %d 字符 (最大允许: 10000)", len(filter))
	}

	// 检查基本的语法结构
	if strings.Count(filter, "(") != strings.Count(filter, ")") {
		return fmt.Errorf("过滤表达式括号不匹配")
	}

	// 可以在这里添加更复杂的表达式解析验证
	// 目前先进行基本检查

	return nil
}

// isValidAlias 验证别名格式
func isValidAlias(alias string) bool {
	if len(alias) == 0 || len(alias) > 64 { // 限制别名长度
		return false
	}

	for i, char := range alias {
		if i == 0 {
			// 第一个字符必须是字母或下划线
			if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || char == '_') {
				return false
			}
		} else {
			// 后续字符可以是字母、数字或下划线
			if !((char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
				(char >= '0' && char <= '9') || char == '_') {
				return false
			}
		}
	}

	return true
}

// aliasValidator 高效的别名验证器
type aliasValidator struct {
	seen       map[string]int  // 别名 -> 首次出现的索引
	duplicates []duplicateInfo // 重复别名信息
}

// duplicateInfo 重复别名的详细信息
type duplicateInfo struct {
	alias   string
	indices []int
}

// newAliasValidator 创建新的别名验证器
func newAliasValidator() *aliasValidator {
	return &aliasValidator{
		seen:       make(map[string]int),
		duplicates: make([]duplicateInfo, 0),
	}
}

// validateAlias 验证别名，支持批量重复检测
func (av *aliasValidator) validateAlias(alias string, index int) error {
	if existingIndex, exists := av.seen[alias]; exists {
		// 发现重复别名

		// 检查是否已记录此重复别名
		found := false
		for i := range av.duplicates {
			if av.duplicates[i].alias == alias {
				av.duplicates[i].indices = append(av.duplicates[i].indices, index)
				found = true
				break
			}
		}

		if !found {
			// 首次发现重复，记录详细信息
			av.duplicates = append(av.duplicates, duplicateInfo{
				alias:   alias,
				indices: []int{existingIndex, index},
			})
		}

		return fmt.Errorf("重复的别名: %s (首次出现位置: %d, 当前位置: %d)",
			alias, existingIndex, index)
	}

	// 记录新别名
	av.seen[alias] = index
	return nil
}

// getSummary 获取验证摘要
func (av *aliasValidator) getSummary() aliasValidationSummary {
	allAliases := make([]string, 0, len(av.seen))
	for alias := range av.seen {
		allAliases = append(allAliases, alias)
	}

	return aliasValidationSummary{
		UniqueCount:    len(av.seen),
		DuplicateCount: len(av.duplicates),
		AllAliases:     allAliases,
		Duplicates:     av.duplicates,
	}
}

// aliasValidationSummary 别名验证摘要
type aliasValidationSummary struct {
	UniqueCount    int
	DuplicateCount int
	AllAliases     []string
	Duplicates     []duplicateInfo
}

// isValidFloat32 验证float32值是否有效
func isValidFloat32(val float32) bool {
	// 检查是否为NaN或无穷大
	if math.IsNaN(float64(val)) {
		return false
	}
	if math.IsInf(float64(val), 0) {
		return false
	}
	return true
}

// convertToProtobufFromSource 转换为协议格式
func (t *queryTask) convertToProtobufFromSource(source *FromSourceDefinition) (*planpb.QueryFromSource, error) {
	pbSource := &planpb.QueryFromSource{
		Alias:  source.Alias,
		Filter: source.Filter,
	}

	switch source.Source {
	case "collection":
		// 设置集合名称
		pbSource.Source = &planpb.QueryFromSource_CollectionName{
			CollectionName: t.request.CollectionName, // 使用当前查询的集合名
		}
	case "external":
		if len(source.Vectors) > 0 {
			// 转换外部向量数据
			externalData := &planpb.ExternalVectorData{
				Vectors:    make([]*planpb.GenericValue, len(source.Vectors)),
				Dimension:  int32(len(source.Vectors[0])), // 假设所有向量维度相同
				VectorType: planpb.VectorType_FloatVector,
			}

			for i, vector := range source.Vectors {
				// 创建向量的 Array 表示
				arrayVal := &planpb.Array{
					Array:       make([]*planpb.GenericValue, len(vector)),
					SameType:    true,
					ElementType: schemapb.DataType_Float,
				}

				// 填充向量中的每个元素
				for j, val := range vector {
					arrayVal.Array[j] = &planpb.GenericValue{
						Val: &planpb.GenericValue_FloatVal{
							FloatVal: float64(val),
						},
					}
				}

				// 将 Array 包装为 GenericValue
				externalData.Vectors[i] = &planpb.GenericValue{
					Val: &planpb.GenericValue_ArrayVal{
						ArrayVal: arrayVal,
					},
				}
			}

			pbSource.Source = &planpb.QueryFromSource_ExternalVectors{
				ExternalVectors: externalData,
			}

			// 保存到 externalVectors 映射
			if t.externalVectors == nil {
				t.externalVectors = make(map[string]*planpb.ExternalVectorData)
			}
			t.externalVectors[source.Alias] = externalData
		} else {
			return nil, fmt.Errorf("external 数据源 %s 缺少向量数据", source.Alias)
		}
	default:
		return nil, fmt.Errorf("不支持的数据源类型: %s", source.Source)
	}

	return pbSource, nil
}

// buildAliasMap 构建别名映射
func (t *queryTask) buildAliasMap() error {
	if t.aliasMap == nil {
		t.aliasMap = make(map[string]*schemaInfo)
	}

	for _, source := range t.fromSources {
		// 检查是否为集合数据源（通过 oneof source 字段）
		switch sourceType := source.GetSource().(type) {
		case *planpb.QueryFromSource_CollectionName:
			collectionName := sourceType.CollectionName
			// 获取对应集合的schema信息
			if err := t.loadSchemaForCollection(source.Alias, collectionName); err != nil {
				log.Ctx(context.Background()).Warn("获取集合schema失败，使用当前schema",
					zap.String("alias", source.Alias),
					zap.String("collectionName", collectionName),
					zap.Error(err))
				// 降级处理：使用当前查询的集合schema
				t.aliasMap[source.Alias] = t.schema
			}
		case *planpb.QueryFromSource_ExternalVectors:
			// 外部向量数据源不需要schema信息
			log.Ctx(context.Background()).Debug("外部向量数据源无需schema",
				zap.String("alias", source.Alias))
		default:
			log.Ctx(context.Background()).Warn("未知的数据源类型",
				zap.String("alias", source.Alias),
				zap.String("sourceType", fmt.Sprintf("%T", sourceType)))
		}
	}

	return nil
}

// getAliases 获取所有别名列表
func (t *queryTask) getAliases() []string {
	var aliases []string
	for _, source := range t.fromSources {
		aliases = append(aliases, source.Alias)
	}
	return aliases
}

// loadSchemaForCollection 为指定集合加载schema
func (t *queryTask) loadSchemaForCollection(alias, collectionName string) error {
	// 当前版本使用相同的数据库和集合
	if collectionName == t.request.CollectionName {
		// 同一集合，直接使用当前schema
		t.aliasMap[alias] = t.schema
		return nil
	}

	// 不同集合的情况 - 这里可以扩展支持多集合查询
	// 未来可以实现：
	// 1. 通过 MetaCache 获取其他集合的 schema
	// 2. 验证集合访问权限
	// 3. 缓存多个集合的 schema 信息

	log.Ctx(context.Background()).Debug("暂不支持跨集合查询，使用当前集合schema",
		zap.String("requestedCollection", collectionName),
		zap.String("currentCollection", t.request.CollectionName))

	// 当前降级处理：使用默认schema
	t.aliasMap[alias] = t.schema
	return nil
}

func (t *queryTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_Retrieve
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

// parseFromSourcesJSON 高效解析 from 参数 JSON
func (t *queryTask) parseFromSourcesJSON(fromParam string) ([]FromSourceDefinition, error) {
	// 简单大小估算，决定解析策略
	const largeJsonThreshold = 10 * 1024 // 10KB

	if len(fromParam) < largeJsonThreshold {
		// 小型JSON：使用快速解析
		return t.parseFromSourcesSimple(fromParam)
	} else {
		// 大型JSON：使用流式解析
		return t.parseFromSourcesStreaming(fromParam)
	}
}

// parseFromSourcesSimple 简单JSON解析
func (t *queryTask) parseFromSourcesSimple(fromParam string) ([]FromSourceDefinition, error) {
	var fromSources []FromSourceDefinition
	if err := json.Unmarshal([]byte(fromParam), &fromSources); err != nil {
		return nil, err
	}
	return fromSources, nil
}

// parseFromSourcesStreaming 流式JSON解析（适用于大型JSON）
func (t *queryTask) parseFromSourcesStreaming(fromParam string) ([]FromSourceDefinition, error) {
	reader := strings.NewReader(fromParam)
	decoder := json.NewDecoder(reader)

	// 期望开始是数组
	token, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("读取JSON开始token失败: %w", err)
	}

	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return nil, fmt.Errorf("期望JSON数组，但得到: %v", token)
	}

	var fromSources []FromSourceDefinition
	index := 0

	// 逐个解析数组元素
	for decoder.More() {
		var source FromSourceDefinition
		if err := decoder.Decode(&source); err != nil {
			return nil, fmt.Errorf("解析第%d个数据源失败: %w", index, err)
		}

		// 基本验证（提前发现问题）
		if source.Alias == "" {
			return nil, fmt.Errorf("第%d个数据源缺少别名", index)
		}

		fromSources = append(fromSources, source)
		index++

		// 防止无限解析
		const maxSources = 1000
		if index >= maxSources {
			return nil, fmt.Errorf("数据源数量超过限制: %d (最大允许: %d)", index, maxSources)
		}
	}

	// 期望结束是数组结束
	token, err = decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("读取JSON结束token失败: %w", err)
	}

	if delim, ok := token.(json.Delim); !ok || delim != ']' {
		return nil, fmt.Errorf("期望JSON数组结束，但得到: %v", token)
	}

	log.Ctx(context.Background()).Debug("流式JSON解析完成",
		zap.Int("sourceCount", len(fromSources)),
		zap.Int("jsonSize", len(fromParam)))

	return fromSources, nil
}
