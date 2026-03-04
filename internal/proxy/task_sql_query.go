// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/sqlparser"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// sqlQueryTask executes a SQL query by building a PlanNode directly from the
// parsed SqlComponents IR, bypassing the QueryRequest string-based pipeline.
type sqlQueryTask struct {
	baseTask
	Condition
	*internalpb.RetrieveRequest

	ctx        context.Context
	sqlRequest *milvuspb.SqlQueryRequest
	comp       *sqlparser.SqlComponents
	result     *milvuspb.SqlQueryResults

	collectionName string
	schema         *schemaInfo
	plan           *planpb.PlanNode

	resultBuf      *typeutil.ConcurrentSet[*internalpb.RetrieveResults]
	mixCoord       types.MixCoordClient
	lb             shardclient.LBPolicy
	shardclientMgr shardclient.ShardClientMgr
	channelsMvcc   map[string]Timestamp

	aggregationFieldMap *agg.AggregationFieldMap
}

func (t *sqlQueryTask) TraceCtx() context.Context { return t.ctx }
func (t *sqlQueryTask) ID() UniqueID              { return t.Base.MsgID }
func (t *sqlQueryTask) SetID(uid UniqueID)        { t.Base.MsgID = uid }
func (t *sqlQueryTask) Name() string              { return "SqlQueryTask" }
func (t *sqlQueryTask) Type() commonpb.MsgType    { return t.Base.MsgType }
func (t *sqlQueryTask) BeginTs() Timestamp        { return t.Base.Timestamp }
func (t *sqlQueryTask) EndTs() Timestamp          { return t.Base.Timestamp }
func (t *sqlQueryTask) SetTs(ts Timestamp)        { t.Base.Timestamp = ts }

func (t *sqlQueryTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = &commonpb.MsgBase{}
	}
	t.Base.MsgType = commonpb.MsgType_Retrieve
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *sqlQueryTask) PreExecute(ctx context.Context) error {
	t.Base.MsgType = commonpb.MsgType_Retrieve
	t.Base.SourceID = paramtable.GetNodeID()

	collectionName := t.comp.Collection
	t.collectionName = collectionName

	log := log.Ctx(ctx).With(
		zap.String("collectionName", collectionName),
		zap.String("requestType", "sqlQuery"),
	)

	if err := validateCollectionName(collectionName); err != nil {
		log.Warn("Invalid collectionName.")
		return err
	}

	// Resolve collection
	collID, err := globalMetaCache.GetCollectionID(ctx, t.sqlRequest.GetDbName(), collectionName)
	if err != nil {
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}
	t.CollectionID = collID

	collectionInfo, err := globalMetaCache.GetCollectionInfo(ctx, t.sqlRequest.GetDbName(), collectionName, t.CollectionID)
	if err != nil {
		return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
	}

	schema, err := globalMetaCache.GetCollectionSchema(ctx, t.sqlRequest.GetDbName(), collectionName)
	if err != nil {
		return err
	}
	t.schema = schema

	// Build plan directly from SqlComponents IR
	t.plan, err = sqlparser.BuildQueryPlan(t.comp, schema.schemaHelper)
	if err != nil {
		return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("failed to build SQL query plan: %v", err))
	}

	// Set legacy fields on RetrieveRequest for reducer compatibility.
	// The reducer checks these to decide between agg reducer vs limit reducer.
	query := t.plan.GetQuery()
	if aggNode := query.GetAggregateNode(); aggNode != nil {
		t.RetrieveRequest.Aggregates = aggNode.GetAggregates()
		var groupByFieldIDs []int64
		for _, gb := range aggNode.GetGroupBy() {
			groupByFieldIDs = append(groupByFieldIDs, gb.GetFieldId())
		}
		t.RetrieveRequest.GroupByFieldIds = groupByFieldIDs

		// Also set on QueryPlanNode for C++ segcore
		query.Aggregates = aggNode.GetAggregates()
		query.GroupByFieldIds = groupByFieldIDs

		// Empty output fields for aggregation (same as queryTask)
		t.RetrieveRequest.OutputFieldsId = make([]UniqueID, 0)
		t.plan.OutputFieldIds = make([]int64, 0)

		// Build aggregation field map for reducer
		t.aggregationFieldMap, err = t.buildAggFieldMap()
		if err != nil {
			return merr.WrapErrAsInputError(merr.WrapErrParameterInvalidMsg("failed to build agg field map: %v", err))
		}
	} else {
		// Non-aggregate query: set output field IDs
		outputFieldIDs := t.plan.GetOutputFieldIds()
		outputFieldIDs = append(outputFieldIDs, common.TimeStampField)
		t.RetrieveRequest.OutputFieldsId = outputFieldIDs
		t.plan.OutputFieldIds = outputFieldIDs
	}

	hasAgg := len(t.RetrieveRequest.GetGroupByFieldIds()) > 0 || len(t.RetrieveRequest.GetAggregates()) > 0

	// Set limit on plan and request
	if t.comp.Limit > 0 {
		query.Limit = t.comp.Limit
		t.RetrieveRequest.Limit = t.comp.Limit
	} else if !hasAgg {
		// Non-aggregate query without LIMIT: use default
		t.RetrieveRequest.Limit = typeutil.Unlimited
	}

	// Serialize plan
	log.Debug("SQL query plan built",
		zap.String("sql", t.sqlRequest.GetSql()),
		zap.Int("aggregates", len(query.GetAggregates())),
		zap.Int("groupByFields", len(query.GetGroupByFieldIds())),
		zap.Int("outputFields", len(t.plan.GetOutputFieldIds())))
	if aggNode := query.GetAggregateNode(); aggNode != nil {
		for i, a := range aggNode.GetAggregates() {
			log.Debug("SQL query aggregate",
				zap.Int("index", i),
				zap.String("op", a.GetOp().String()),
				zap.Int64("fieldID", a.GetFieldId()),
				zap.Strings("nestedPath", a.GetNestedPath()))
		}
	}
	t.RetrieveRequest.SerializedExprPlan, err = proto.Marshal(t.plan)
	if err != nil {
		return err
	}
	log.Debug("SQL query serialized plan",
		zap.Int("serializedPlanSize", len(t.RetrieveRequest.SerializedExprPlan)),
		zap.Int("requestAggregates", len(t.RetrieveRequest.GetAggregates())),
		zap.Int64s("requestGroupByFields", t.RetrieveRequest.GetGroupByFieldIds()))

	// Set username
	if username, _ := GetCurUserFromContext(ctx); username != "" {
		t.RetrieveRequest.Username = username
	}

	// Resolve partitions
	partitionIDs, err := getPartitionIDs(ctx, t.sqlRequest.GetDbName(), collectionName, nil)
	if err != nil {
		return err
	}
	t.RetrieveRequest.PartitionIDs = partitionIDs

	// Consistency & timestamps
	consistencyLevel := t.sqlRequest.GetConsistencyLevel()
	if consistencyLevel == 0 {
		consistencyLevel = collectionInfo.consistencyLevel
	}
	t.RetrieveRequest.ConsistencyLevel = consistencyLevel

	guaranteeTs := parseGuaranteeTsFromConsistency(0, t.BeginTs(), consistencyLevel)
	if collectionInfo.updateTimestamp > guaranteeTs {
		guaranteeTs = collectionInfo.updateTimestamp
	}
	t.GuaranteeTimestamp = guaranteeTs

	if collectionInfo.collectionTTL != 0 {
		physicalTime := tsoutil.PhysicalTime(t.GetBase().GetTimestamp())
		expireTime := physicalTime.Add(-time.Duration(collectionInfo.collectionTTL))
		t.CollectionTtlTimestamps = tsoutil.ComposeTSByTime(expireTime, 0)
		if t.CollectionTtlTimestamps > t.GetBase().GetTimestamp() {
			return merr.WrapErrServiceInternal(fmt.Sprintf("ttl timestamp overflow, base timestamp: %d, ttl duration %v",
				t.GetBase().GetTimestamp(), collectionInfo.collectionTTL))
		}
	}

	deadline, ok := t.TraceCtx().Deadline()
	if ok {
		t.TimeoutTimestamp = tsoutil.ComposeTSByTime(deadline, 0)
	}

	t.DbID = 0

	log.Debug("SqlQuery PreExecute done.",
		zap.Uint64("guarantee_ts", guaranteeTs),
		zap.Uint64("mvcc_ts", t.GetMvccTimestamp()),
	)
	return nil
}

func (t *sqlQueryTask) Execute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder(fmt.Sprintf("proxy execute sql query %d", t.ID()))
	defer tr.CtxElapse(ctx, "done")

	log := log.Ctx(ctx).With(
		zap.Int64("collection", t.GetCollectionID()),
		zap.Int64s("partitionIDs", t.GetPartitionIDs()),
		zap.String("requestType", "sqlQuery"),
	)

	t.resultBuf = typeutil.NewConcurrentSet[*internalpb.RetrieveResults]()
	err := t.lb.Execute(ctx, shardclient.CollectionWorkLoad{
		Db:             t.sqlRequest.GetDbName(),
		CollectionID:   t.CollectionID,
		CollectionName: t.collectionName,
		Nq:             1,
		Exec:           t.queryShard,
	})
	if err != nil {
		log.Warn("fail to execute sql query", zap.Error(err))
		return errors.Wrap(err, "failed to execute sql query")
	}

	log.Debug("SqlQuery Execute done.")
	return nil
}

func (t *sqlQueryTask) PostExecute(ctx context.Context) error {
	tr := timerecord.NewTimeRecorder("sqlQueryTask PostExecute")
	defer tr.CtxElapse(ctx, "done")

	log := log.Ctx(ctx).With(
		zap.Int64("collection", t.GetCollectionID()),
		zap.String("requestType", "sqlQuery"),
	)

	toReduceResults := make([]*internalpb.RetrieveResults, 0)
	select {
	case <-t.TraceCtx().Done():
		log.Warn("proxy", zap.Int64("SqlQuery: timeout, msgID:", t.ID()))
		return nil
	default:
		t.resultBuf.Range(func(res *internalpb.RetrieveResults) bool {
			toReduceResults = append(toReduceResults, res)
			log.Debug("SQL query segcore result",
				zap.Int("fields", len(res.GetFieldsData())))
			for i, fd := range res.GetFieldsData() {
				var rows int
				switch fd.GetType() {
				case schemapb.DataType_VarChar:
					rows = len(fd.GetScalars().GetStringData().GetData())
				case schemapb.DataType_Double:
					rows = len(fd.GetScalars().GetDoubleData().GetData())
				case schemapb.DataType_Int64:
					rows = len(fd.GetScalars().GetLongData().GetData())
				}
				log.Debug("SQL query segcore field",
					zap.Int("index", i),
					zap.String("name", fd.GetFieldName()),
					zap.String("type", fd.GetType().String()),
					zap.Int("rows", rows))
			}
			return true
		})
	}

	// Translate ORDER BY items into orderby.OrderByField, resolving against
	// schema fields, group-by columns, and aggregate output columns.
	orderByFields, err := t.buildOrderByFields()
	if err != nil {
		log.Warn("fail to build order by fields", zap.Error(err))
		return err
	}
	log.Debug("SQL query order by fields built",
		zap.Int("orderByFields", len(orderByFields)))
	for i, f := range orderByFields {
		log.Debug("SQL query order by field",
			zap.Int("index", i),
			zap.String("name", f.FieldName),
			zap.Int64("fieldID", f.FieldID),
			zap.Bool("ascending", f.Ascending),
			zap.Bool("isAggregate", f.IsAggregate))
	}

	limit := t.comp.Limit
	if limit <= 0 {
		limit = typeutil.Unlimited
	}

	// If ORDER BY references a computed alias, the pipeline can't sort it
	// (the column doesn't exist until ApplyProjection). Don't let the pipeline
	// LIMIT either — postProjectionSort handles both sort + limit.
	pipelineLimit := limit
	if t.hasComputedOrderBy() {
		pipelineLimit = typeutil.Unlimited
	}

	pipeline, err := NewQueryPipeline(
		t.schema.CollectionSchema,
		pipelineLimit,
		0, // offset
		reduce.IReduceNoOrder,
		orderByFields,
		t.RetrieveRequest.GetGroupByFieldIds(),
		t.RetrieveRequest.GetAggregates(),
		t.aggregationFieldMap,
		filterSystemFields(t.RetrieveRequest.GetOutputFieldsId()),
	)
	if err != nil {
		log.Warn("fail to create sql query pipeline", zap.Error(err))
		return err
	}

	queryResult, err := pipeline.Execute(ctx, toReduceResults)
	if err != nil {
		log.Warn("fail to reduce sql query result", zap.Error(err))
		return err
	}

	// Debug: log pipeline output
	for i, fd := range queryResult.GetFieldsData() {
		log.Info("pipeline output", zap.Int("col", i), zap.String("name", fd.GetFieldName()),
			zap.String("type", fd.GetType().String()))
	}

	// Apply post-aggregation projection for computed expressions (e.g., extract(hour, ...), arithmetic on agg results).
	fieldsData := queryResult.GetFieldsData()
	fieldsData, err = sqlparser.ApplyProjection(fieldsData, t.comp)
	if err != nil {
		log.Warn("fail to apply projection", zap.Error(err))
		return err
	}

	// Post-projection sort: handle ORDER BY on computed aliases that couldn't
	// be sorted by the pipeline (because the column didn't exist until ApplyProjection).
	fieldsData = t.postProjectionSort(fieldsData)

	// Convert QueryResults → SqlQueryResults
	t.result = &milvuspb.SqlQueryResults{
		Status:      merr.Success(),
		FieldsData:  fieldsData,
		ColumnNames: buildColumnNames(t.comp),
	}

	log.Debug("SqlQuery PostExecute done")
	return nil
}

func (t *sqlQueryTask) queryShard(ctx context.Context, nodeID int64, qn types.QueryNodeClient, channel string) error {
	retrieveReq := typeutil.Clone(t.RetrieveRequest)
	retrieveReq.GetBase().TargetID = nodeID
	retrieveReq.ConsistencyLevel = t.ConsistencyLevel

	req := &querypb.QueryRequest{
		Req:         retrieveReq,
		DmlChannels: []string{channel},
		Scope:       querypb.DataScope_All,
	}

	log := log.Ctx(ctx).With(
		zap.Int64("collection", t.GetCollectionID()),
		zap.Int64("nodeID", nodeID),
		zap.String("channel", channel),
	)

	result, err := qn.Query(ctx, req)
	if err != nil {
		log.Warn("QueryNode query return error", zap.Error(err))
		t.shardclientMgr.DeprecateShardCache(t.sqlRequest.GetDbName(), t.collectionName)
		return err
	}
	if result.GetStatus().GetErrorCode() == commonpb.ErrorCode_NotShardLeader {
		log.Warn("QueryNode is not shardLeader")
		t.shardclientMgr.DeprecateShardCache(t.sqlRequest.GetDbName(), t.collectionName)
		return merr.Error(result.GetStatus())
	}
	if result.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
		log.Warn("QueryNode query result error",
			zap.Any("errorCode", result.GetStatus().GetErrorCode()),
			zap.String("reason", result.GetStatus().GetReason()))
		return errors.Wrapf(merr.Error(result.GetStatus()), "fail to Query on QueryNode %d", nodeID)
	}

	t.resultBuf.Insert(result)
	t.lb.UpdateCostMetrics(nodeID, result.CostAggregation)
	return nil
}

// buildAggFieldMap creates the AggregationFieldMap needed by the aggregate reducer.
// It maps output column names to aggregate operations using the schema for field type lookup.
//
// For computed expressions (SelectComputed), this also registers:
//   - Hidden group-by keys: when a GROUP BY references a computed alias (e.g., hour_of_day),
//     the underlying column (e.g., data["time_us"]) must be in the reducer output.
//   - Hidden aggregates: when a computed expression contains nested aggregates (e.g., max/min
//     in Q5's activity_span), they must be registered so the reducer includes them.
func (t *sqlQueryTask) buildAggFieldMap() (*agg.AggregationFieldMap, error) {
	var outputFields []string
	var groupByFields []string
	var aggregates []agg.AggregateBase

	for _, si := range t.comp.SelectItems {
		switch si.Type {
		case sqlparser.SelectColumn:
			if si.Column != nil {
				outputFields = append(outputFields, si.Column.MilvusExpr())
			}
		case sqlparser.SelectAgg:
			if si.AggFunc != nil {
				// Use resolved column (handles nested functions like min(to_timestamp(...)))
				aggStr := formatAggForOutputResolved(si.AggFunc, &si)
				outputFields = append(outputFields, aggStr)

				// Resolve aggregate to AggregateBase via schema
				col := resolveAggColumn(&si)
				if col != nil {
					field, err := t.schema.schemaHelper.GetFieldFromNameDefaultJSON(col.FieldName)
					if err == nil {
						fieldID := field.GetFieldID()
						fieldType := field.GetDataType()
						if fieldType == schemapb.DataType_JSON {
							fieldType = schemapb.DataType_Double
						}
						aggFuncs, err := agg.NewAggregate(si.AggFunc.FuncName, fieldID, aggStr, fieldType)
						if err == nil {
							aggregates = append(aggregates, aggFuncs...)
						}
					}
				} else {
					// count(*) — no column ref
					aggFuncs, err := agg.NewAggregate(si.AggFunc.FuncName, 0, aggStr, schemapb.DataType_None)
					if err == nil {
						aggregates = append(aggregates, aggFuncs...)
					}
				}
			}
		case sqlparser.SelectComputed:
			// Register hidden aggregates from computed expressions (e.g., max/min inside Q5's activity_span).
			if si.RawExpr != nil {
				for _, na := range sqlparser.ExtractNestedAggregates(si.RawExpr) {
					aggStr := fmt.Sprintf("%s(%s)", na.FuncName, na.Column.MilvusExpr())
					outputFields = append(outputFields, aggStr)
					t.addHiddenAggregate(na, aggStr, &aggregates)
				}
			}
		case sqlparser.SelectStar:
			outputFields = append(outputFields, "*")
		}
	}

	// GROUP BY keys — include all keys, resolving computed aliases to their inner columns.
	for _, gb := range t.comp.GroupBy {
		col := gb.Column
		if col == nil && gb.Alias != "" {
			// Resolve alias from SELECT list
			for _, si := range t.comp.SelectItems {
				if si.Alias != gb.Alias {
					continue
				}
				if si.Column != nil {
					col = si.Column
				} else if si.Type == sqlparser.SelectComputed && si.RawExpr != nil {
					col = sqlparser.ExtractDeepColumnRef(si.RawExpr)
					if col != nil {
						// Hidden group-by key: add to outputFields so the reducer includes it
						outputFields = append(outputFields, col.MilvusExpr())
					}
				}
				break
			}
		}
		if col != nil {
			groupByFields = append(groupByFields, col.MilvusExpr())
		}
	}

	log.Info("buildAggFieldMap",
		zap.Strings("outputFields", outputFields),
		zap.Strings("groupByFields", groupByFields),
		zap.Int("numAggregates", len(aggregates)),
	)
	for i, a := range aggregates {
		log.Info("aggregate", zap.Int("idx", i), zap.String("name", a.OriginalName()), zap.Int64("fieldID", a.FieldID()))
	}
	return agg.NewAggregationFieldMap(outputFields, groupByFields, aggregates)
}

// buildOrderByFields translates SQL ORDER BY items into orderby.OrderByField,
// resolving each Ref against (1) the schema, (2) SELECT aliases pointing to a
// column, and (3) SELECT aliases pointing to an aggregate. For aggregate refs
// the FieldID matches the underlying agg's FieldID so that
// ComputeGroupByOrderPositions can locate the right output column.
func (t *sqlQueryTask) buildOrderByFields() ([]*orderby.OrderByField, error) {
	if len(t.comp.OrderBy) == 0 {
		return nil, nil
	}

	result := make([]*orderby.OrderByField, 0, len(t.comp.OrderBy))
	for _, ob := range t.comp.OrderBy {
		if ob.IsDistance {
			// Vector-distance ORDER BY is handled by the search pipeline,
			// not by sqlQueryTask. Skip silently.
			continue
		}

		ascending := !ob.Desc

		// 1. Direct schema field lookup
		if field, err := t.schema.schemaHelper.GetFieldFromName(ob.Ref); err == nil {
			result = append(result, orderby.NewOrderByField(
				field.GetFieldID(), field.GetName(), field.GetDataType(),
				orderby.WithAscending(ascending),
			))
			continue
		}

		// 2. Resolve as alias from SELECT items
		var matchedSelect *sqlparser.SelectItem
		for i := range t.comp.SelectItems {
			if t.comp.SelectItems[i].Alias == ob.Ref {
				matchedSelect = &t.comp.SelectItems[i]
				break
			}
		}

		if matchedSelect != nil {
			// 2a. Alias of a plain column
			if matchedSelect.Column != nil {
				field, err := t.schema.schemaHelper.GetFieldFromNameDefaultJSON(matchedSelect.Column.FieldName)
				if err == nil {
					result = append(result, orderby.NewOrderByField(
						field.GetFieldID(), field.GetName(), field.GetDataType(),
						orderby.WithAscending(ascending),
					))
					continue
				}
			}

			// 2b. Alias of an aggregate — match against plan aggregates by FieldID
			if matchedSelect.AggFunc != nil {
				if of, ok := t.aggregateOrderByField(matchedSelect, ascending); ok {
					result = append(result, of)
					continue
				}
			}

			// 2c. Alias of a computed expression — ORDER BY must happen after
			// ApplyProjection, which runs after the pipeline. For now, skip
			// and let the pipeline return unsorted; ApplyProjection will produce
			// the column but it won't be sorted.
			// TODO: support ORDER BY on computed expressions
			if matchedSelect.Type == sqlparser.SelectComputed {
				continue
			}
		}

		// 3. Bare aggregate expression in ORDER BY (e.g. ORDER BY count(*))
		if of, ok := t.matchAggregateByExpr(ob.Ref, ascending); ok {
			result = append(result, of)
			continue
		}

		return nil, fmt.Errorf("ORDER BY field %q not found in schema or SELECT list", ob.Ref)
	}

	return result, nil
}

// aggregateOrderByField builds an OrderByField for an aggregate reference,
// using the agg's resolved FieldID so the pipeline can position it.
func (t *sqlQueryTask) aggregateOrderByField(si *sqlparser.SelectItem, ascending bool) (*orderby.OrderByField, bool) {
	afc := si.AggFunc
	col := resolveAggColumn(si)

	var fieldID int64
	dataType := schemapb.DataType_Int64

	if col == nil {
		// count(*) — fieldID 0
		fieldID = 0
	} else {
		field, err := t.schema.schemaHelper.GetFieldFromNameDefaultJSON(col.FieldName)
		if err != nil {
			return nil, false
		}
		fieldID = field.GetFieldID()
		dataType = field.GetDataType()
		// JSON fields with nested_path are extracted as DOUBLE by segcore
		if dataType == schemapb.DataType_JSON {
			dataType = schemapb.DataType_Double
		}
	}

	of := orderby.NewOrderByField(
		fieldID, formatAggForOutputResolved(afc, si), dataType,
		orderby.WithAscending(ascending),
	)
	of.IsAggregate = true
	return of, true
}

// matchAggregateByExpr scans SELECT items for an aggregate whose printed form
// matches the given ref string (e.g. "count(*)").
func (t *sqlQueryTask) matchAggregateByExpr(ref string, ascending bool) (*orderby.OrderByField, bool) {
	for i := range t.comp.SelectItems {
		si := &t.comp.SelectItems[i]
		if si.AggFunc == nil {
			continue
		}
		if formatAggForOutput(si.AggFunc) == ref || formatAggForOutputResolved(si.AggFunc, si) == ref {
			return t.aggregateOrderByField(si, ascending)
		}
	}
	return nil, false
}

// hasComputedOrderBy returns true if any ORDER BY item references a computed alias.
func (t *sqlQueryTask) hasComputedOrderBy() bool {
	for _, ob := range t.comp.OrderBy {
		for _, si := range t.comp.SelectItems {
			if si.Type == sqlparser.SelectComputed && si.Alias == ob.Ref {
				return true
			}
		}
	}
	return false
}

// postProjectionSort sorts fieldsData by ORDER BY items that reference
// computed expression aliases. These can't be sorted by the pipeline because
// the column only exists after ApplyProjection.
func (t *sqlQueryTask) postProjectionSort(fieldsData []*schemapb.FieldData) []*schemapb.FieldData {
	if len(t.comp.OrderBy) == 0 || len(fieldsData) == 0 {
		return fieldsData
	}

	// Find computed ORDER BY: match OrderItem.Ref to SelectItem.Alias of SelectComputed
	var sortColIdx int = -1
	var sortDesc bool
	for _, ob := range t.comp.OrderBy {
		for i, si := range t.comp.SelectItems {
			if si.Type == sqlparser.SelectComputed && si.Alias == ob.Ref {
				if i < len(fieldsData) {
					sortColIdx = i
					sortDesc = ob.Desc
				}
				break
			}
		}
		if sortColIdx >= 0 {
			break // only support single computed ORDER BY for now
		}
	}

	if sortColIdx < 0 {
		return fieldsData
	}

	// Get sort column values as float64
	sortCol := fieldsData[sortColIdx]
	var vals []float64
	switch sortCol.GetType() {
	case schemapb.DataType_Double:
		vals = sortCol.GetScalars().GetDoubleData().GetData()
	case schemapb.DataType_Int64:
		longData := sortCol.GetScalars().GetLongData().GetData()
		vals = make([]float64, len(longData))
		for i, v := range longData {
			vals[i] = float64(v)
		}
	case schemapb.DataType_Float:
		floatData := sortCol.GetScalars().GetFloatData().GetData()
		vals = make([]float64, len(floatData))
		for i, v := range floatData {
			vals[i] = float64(v)
		}
	default:
		return fieldsData // can't sort non-numeric computed columns
	}

	if len(vals) <= 1 {
		return fieldsData
	}

	// Build sort indices
	indices := make([]int, len(vals))
	for i := range indices {
		indices[i] = i
	}
	sort.SliceStable(indices, func(i, j int) bool {
		if sortDesc {
			return vals[indices[i]] > vals[indices[j]]
		}
		return vals[indices[i]] < vals[indices[j]]
	})

	// Apply LIMIT
	if t.comp.Limit > 0 && int(t.comp.Limit) < len(indices) {
		indices = indices[:int(t.comp.Limit)]
	}

	// Reorder all columns by copying rows in sorted order
	result := make([]*schemapb.FieldData, len(fieldsData))
	for c, fd := range fieldsData {
		result[c] = copyFieldDataByIndices(fd, indices)
	}
	return result
}

// copyFieldDataByIndices creates a new FieldData with rows reordered by indices.
func copyFieldDataByIndices(fd *schemapb.FieldData, indices []int) *schemapb.FieldData {
	switch fd.GetType() {
	case schemapb.DataType_Double:
		src := fd.GetScalars().GetDoubleData().GetData()
		dst := make([]float64, len(indices))
		for i, idx := range indices {
			dst[i] = src[idx]
		}
		return &schemapb.FieldData{
			Type: fd.GetType(), FieldName: fd.GetFieldName(),
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: dst}}}}}
	case schemapb.DataType_Int64:
		src := fd.GetScalars().GetLongData().GetData()
		dst := make([]int64, len(indices))
		for i, idx := range indices {
			dst[i] = src[idx]
		}
		return &schemapb.FieldData{
			Type: fd.GetType(), FieldName: fd.GetFieldName(),
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: dst}}}}}
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		src := fd.GetScalars().GetStringData().GetData()
		dst := make([]string, len(indices))
		for i, idx := range indices {
			dst[i] = src[idx]
		}
		return &schemapb.FieldData{
			Type: fd.GetType(), FieldName: fd.GetFieldName(),
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: dst}}}}}
	case schemapb.DataType_Float:
		src := fd.GetScalars().GetFloatData().GetData()
		dst := make([]float32, len(indices))
		for i, idx := range indices {
			dst[i] = src[idx]
		}
		return &schemapb.FieldData{
			Type: fd.GetType(), FieldName: fd.GetFieldName(),
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: dst}}}}}
	default:
		return fd
	}
}

// addHiddenAggregate creates an AggregateBase for a nested aggregate found in a computed expression.
func (t *sqlQueryTask) addHiddenAggregate(na sqlparser.NestedAggregate, aggStr string, aggregates *[]agg.AggregateBase) {
	field, err := t.schema.schemaHelper.GetFieldFromNameDefaultJSON(na.Column.FieldName)
	if err != nil {
		return
	}
	fieldID := field.GetFieldID()
	fieldType := field.GetDataType()
	// JSON fields with nested_path are extracted as DOUBLE by segcore's ProjectNode.
	if fieldType == schemapb.DataType_JSON {
		fieldType = schemapb.DataType_Double
	}
	aggFuncs, err := agg.NewAggregate(na.FuncName, fieldID, aggStr, fieldType)
	if err == nil {
		*aggregates = append(*aggregates, aggFuncs...)
	}
}

// buildColumnNames extracts column names from SELECT items, preferring aliases.
func buildColumnNames(comp *sqlparser.SqlComponents) []string {
	var names []string
	for _, si := range comp.SelectItems {
		if si.Alias != "" {
			names = append(names, si.Alias)
		} else if si.Column != nil {
			names = append(names, si.Column.MilvusExpr())
		} else if si.AggFunc != nil {
			names = append(names, formatAggForOutput(si.AggFunc))
		}
	}
	return names
}

// formatAggForOutput formats an AggFuncCall as a string that
// the aggregate reducer can parse.
// e.g. count(*), sum(field), min(data["time_us"]), count_distinct(data["did"])
func formatAggForOutput(afc *sqlparser.AggFuncCall) string {
	name := afc.FuncName
	if afc.Distinct && strings.ToLower(name) == "count" {
		name = "count_distinct"
	}
	if afc.Arg == nil {
		return fmt.Sprintf("%s(*)", name)
	}
	return fmt.Sprintf("%s(%s)", name, afc.Arg.MilvusExpr())
}

// resolveAggColumn returns the effective column for an aggregate, using
// extractDeepColumnRef as fallback when Arg is nil (nested functions like
// min(to_timestamp(...))). Mirrors plan_builder.go's buildAggregate logic.
func resolveAggColumn(si *sqlparser.SelectItem) *sqlparser.ColumnRef {
	if si.AggFunc != nil && si.AggFunc.Arg != nil {
		return si.AggFunc.Arg
	}
	if si.RawExpr != nil {
		return sqlparser.ExtractDeepColumnRef(si.RawExpr)
	}
	return nil
}

// formatAggForOutputResolved formats an aggregate using the resolved column
// (including nested function fallback). Use this in buildAggFieldMap where
// the output name must match what segcore's reducer produces.
func formatAggForOutputResolved(afc *sqlparser.AggFuncCall, si *sqlparser.SelectItem) string {
	col := resolveAggColumn(si)
	if col == nil {
		return fmt.Sprintf("%s(*)", afc.FuncName)
	}
	return fmt.Sprintf("%s(%s)", afc.FuncName, col.MilvusExpr())
}

// sqlComponentsToSearchRequest converts SQL vector search components into a SearchRequest.
// This allows us to reuse the existing search pipeline which handles plan building,
// placeholder group conversion, shard routing, and result merging.
func sqlComponentsToSearchRequest(comp *sqlparser.SqlComponents, sqlReq *milvuspb.SqlQueryRequest) (*milvuspb.SearchRequest, error) {
	vs := comp.VectorSearch
	if vs == nil {
		return nil, fmt.Errorf("no vector search definition")
	}

	// Build output fields from SELECT items (exclude vector distance — search adds it automatically)
	var outputFields []string
	for _, si := range comp.SelectItems {
		switch si.Type {
		case sqlparser.SelectColumn:
			if si.Column != nil {
				outputFields = append(outputFields, si.Column.MilvusExpr())
			}
		case sqlparser.SelectStar:
			outputFields = []string{"*"}
		}
	}

	// Build search params: start with SQL-derived values, then merge user-provided searchParams.
	searchParams := []*commonpb.KeyValuePair{
		{Key: AnnsFieldKey, Value: vs.FieldName},
		{Key: TopKKey, Value: fmt.Sprintf("%d", vs.TopK)},
		{Key: MetricTypeKey, Value: vs.MetricType},
		{Key: RoundDecimalKey, Value: "-1"},
	}

	// Collect index-tuning and search-control params from the HTTP request's searchParams.
	// Keys like nprobe, ef, radius, range_filter, offset, etc. are forwarded as-is.
	// Vector parameter refs ($1, $2, ...) are excluded — they carry query vectors, not search params.
	paramsMap := make(map[string]interface{})
	for key, value := range sqlReq.GetSearchParams() {
		if strings.HasPrefix(key, "$") {
			continue // skip vector parameter references
		}
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: key, Value: value})
		paramsMap[key] = value
	}
	paramsJSON, _ := json.Marshal(paramsMap)
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: ParamsKey, Value: string(paramsJSON)})

	// Build WHERE filter expression
	filterExpr := ""
	if comp.Where != nil {
		filterExpr = comp.Where.MilvusExpr
	}

	// Build placeholder group from query vector or resolve from searchParams
	var placeholderGroupBytes []byte
	if len(vs.Vector) > 0 {
		pg, err := sqlparser.BuildPlaceholderGroup([][]float32{vs.Vector})
		if err != nil {
			return nil, fmt.Errorf("failed to build placeholder group: %w", err)
		}
		placeholderGroupBytes = pg
	} else if vs.ParamRef != "" {
		// Resolve parameter reference from SearchParams
		vecJSON, ok := sqlReq.GetSearchParams()[vs.ParamRef]
		if !ok {
			return nil, fmt.Errorf("vector parameter %s not found in searchParams", vs.ParamRef)
		}
		var vec []float32
		if err := json.Unmarshal([]byte(vecJSON), &vec); err != nil {
			return nil, fmt.Errorf("failed to parse vector parameter %s: %w", vs.ParamRef, err)
		}
		pg, err := sqlparser.BuildPlaceholderGroup([][]float32{vec})
		if err != nil {
			return nil, fmt.Errorf("failed to build placeholder group: %w", err)
		}
		placeholderGroupBytes = pg
	} else {
		return nil, fmt.Errorf("no query vector provided: specify vector literal or parameter reference")
	}

	return &milvuspb.SearchRequest{
		DbName:         sqlReq.GetDbName(),
		CollectionName: comp.Collection,
		Dsl:            filterExpr,
		DslType:        commonpb.DslType_BoolExprV1,
		SearchInput: &milvuspb.SearchRequest_PlaceholderGroup{
			PlaceholderGroup: placeholderGroupBytes,
		},
		OutputFields:          outputFields,
		SearchParams:          searchParams,
		Nq:                    1,
		ConsistencyLevel:      sqlReq.GetConsistencyLevel(),
		UseDefaultConsistency: true,
	}, nil
}

// searchResultsToSqlResults converts SearchResults into SqlQueryResults format.
func searchResultsToSqlResults(res *milvuspb.SearchResults, comp *sqlparser.SqlComponents) *milvuspb.SqlQueryResults {
	results := res.GetResults()
	if results == nil {
		return &milvuspb.SqlQueryResults{
			Status: merr.Success(),
		}
	}

	// Build column names from SQL SELECT items
	columnNames := buildColumnNames(comp)

	// Fields data from search results
	fieldsData := results.GetFieldsData()

	// If the SQL has a distance alias, add scores as a field
	if comp.VectorSearch != nil && comp.VectorSearch.DistAlias != "" {
		scores := results.GetScores()
		if len(scores) > 0 {
			distField := &schemapb.FieldData{
				Type:      schemapb.DataType_Float,
				FieldName: comp.VectorSearch.DistAlias,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{Data: scores},
						},
					},
				},
			}
			fieldsData = append(fieldsData, distField)
		}
	}

	return &milvuspb.SqlQueryResults{
		Status:      merr.Success(),
		ColumnNames: columnNames,
		FieldsData:  fieldsData,
	}
}
