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

package segments

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
)

// Distance query related constants
const (
	// Distance field dedicated ID, using negative number to avoid conflicts with actual field IDs
	DistanceFieldID = -100

	// Default distance metric type
	DefaultMetricType = "L2"

	// Supported distance metric types
	MetricTypeL2      = "L2"
	MetricTypeIP      = "IP"
	MetricTypeCOSINE  = "COSINE"
	MetricTypeHAMMING = "HAMMING"
	MetricTypeJACCARD = "JACCARD"

	// Index related constants
	PrimaryKeyIndex = 0
)

// DistanceQueryPlanner distance query plan parser
type DistanceQueryPlanner struct {
	schema      *schemapb.CollectionSchema
	fromSources []*planpb.QueryFromSource
	monitor     *distanceQueryPerformanceMonitor
}

// DistanceResultFormatter result formatter
type DistanceResultFormatter struct {
	schema           *schemapb.CollectionSchema
	outputFieldIDs   []int64
	distanceFieldIDs []int64 // explicitly mark which field IDs are distance fields
}

// DistanceQueryCoordinator query coordinator
type DistanceQueryCoordinator struct {
	planner   *DistanceQueryPlanner
	formatter *DistanceResultFormatter
	monitor   *distanceQueryPerformanceMonitor
}

// DistanceExecutor distance query executor
type DistanceExecutor struct {
	ctx            context.Context
	manager        *Manager
	segments       []Segment
	plan           *planpb.PlanNode
	req            *querypb.QueryRequest
	metricType     string
	limit          int64
	offset         int64
	outputFieldIDs []int64
	coordinator    *DistanceQueryCoordinator
}

// DistanceQueryResult single result of distance query
type DistanceQueryResult struct {
	LeftVectorID  interface{}           // ID of left vector
	RightVectorID interface{}           // ID of right vector
	Distance      float32               // calculated distance
	LeftFields    map[int64]interface{} // fields of left record
	RightFields   map[int64]interface{} // fields of right record
}

// NewDistanceQueryPlanner creates query plan parser with from sources
func NewDistanceQueryPlanner(schema *schemapb.CollectionSchema, fromSources []*planpb.QueryFromSource) *DistanceQueryPlanner {
	return &DistanceQueryPlanner{
		schema:      schema,
		fromSources: fromSources,
		monitor:     newDistanceQueryPerformanceMonitor(),
	}
}

// NewDistanceResultFormatter create result formatter
func NewDistanceResultFormatter(schema *schemapb.CollectionSchema, outputFieldIDs []int64) *DistanceResultFormatter {
	distanceFieldIDs := make([]int64, 0)

	for i, fieldID := range outputFieldIDs {
		if i == PrimaryKeyIndex {
			continue
		}

		if fieldID == common.TimeStampField {
			continue
		}

		existsInSchema := false
		for _, field := range schema.GetFields() {
			if field.GetFieldID() == fieldID {
				existsInSchema = true
				break
			}
		}

		if !existsInSchema {
			distanceFieldIDs = append(distanceFieldIDs, fieldID)
		}
	}

	return &DistanceResultFormatter{
		schema:           schema,
		outputFieldIDs:   outputFieldIDs,
		distanceFieldIDs: distanceFieldIDs,
	}
}

// NewDistanceQueryCoordinator creates query coordinator with from sources
func NewDistanceQueryCoordinator(schema *schemapb.CollectionSchema, outputFieldIDs []int64, fromSources []*planpb.QueryFromSource) *DistanceQueryCoordinator {
	return &DistanceQueryCoordinator{
		planner:   NewDistanceQueryPlanner(schema, fromSources),
		formatter: NewDistanceResultFormatter(schema, outputFieldIDs),
		monitor:   newDistanceQueryPerformanceMonitor(),
	}
}

// NewDistanceExecutor create distance query executor
func NewDistanceExecutor(
	ctx context.Context,
	manager *Manager,
	schema *schemapb.CollectionSchema,
	segments []Segment,
	plan *planpb.PlanNode,
	req *querypb.QueryRequest,
) *DistanceExecutor {
	// Filter out nil segments
	validSegments := make([]Segment, 0, len(segments))
	for _, segment := range segments {
		if segment != nil {
			validSegments = append(validSegments, segment)
		}
	}

	outputFieldIDs := req.GetReq().GetOutputFieldsId()

	// Get from sources before creating coordinator
	tempExecutor := &DistanceExecutor{
		ctx:  ctx,
		plan: plan,
		req:  req,
	}
	fromSources := tempExecutor.getFromSources()

	executor := &DistanceExecutor{
		ctx:         ctx,
		manager:     manager,
		segments:    validSegments,
		plan:        plan,
		req:         req,
		metricType:  DefaultMetricType, // default distance metric
		limit:       req.GetReq().GetLimit(),
		offset:      getOffsetFromRequest(req),
		coordinator: NewDistanceQueryCoordinator(schema, outputFieldIDs, fromSources),
	}

	return executor
}

// Execute distance query execution
func (e *DistanceExecutor) Execute() (*internalpb.RetrieveResults, error) {

	// Parse query plan
	distanceExpr, err := e.coordinator.planner.ParseDistanceQuery(e.plan)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query plan: %w", err)
	}
	// Update distance metric type
	e.updateMetricType(distanceExpr)
	// Execute distance calculation
	results, err := e.executeDistanceCalculation(distanceExpr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute distance calculation: %w", err)
	}
	// Format results
	finalResult, err := e.coordinator.formatter.FormatResults(results)
	if err != nil {
		return nil, fmt.Errorf("failed to format results: %w", err)
	}
	return finalResult, nil
}

// updateMetricType update distance metric type
func (e *DistanceExecutor) updateMetricType(distanceExpr *planpb.DistanceExpr) {
	// Validate integrity of distance expression
	if err := e.validateDistanceExpr(distanceExpr); err != nil {
		log.Ctx(e.ctx).Warn("Distance expression validation failed, using default metric",
			zap.Error(err))
		e.metricType = DefaultMetricType
		return
	}

	if distanceExpr.GetMetricEnum() != planpb.DistanceMetric_DISTANCE_METRIC_UNSPECIFIED {
		// Convert proto enumeration to string format expected by distance calculator
		e.metricType = e.convertProtoMetricToString(distanceExpr.GetMetricEnum())
	} else if distanceExpr.GetMetricString() != "" {
		e.metricType = distanceExpr.GetMetricString()
	} else {
		e.metricType = DefaultMetricType // default to L2 distance
	}
}

// validateDistanceExpr validate validity of distance expression
func (e *DistanceExecutor) validateDistanceExpr(distanceExpr *planpb.DistanceExpr) error {
	if distanceExpr == nil {
		return fmt.Errorf("distance expression is nil")
	}

	// Check left vector expression
	if distanceExpr.GetLeftVector() == nil {
		return fmt.Errorf("left vector expression is missing")
	}

	// Check right vector expression
	if distanceExpr.GetRightVector() == nil {
		return fmt.Errorf("right vector expression is missing")
	}

	// Check distance metric type
	hasValidMetric := false
	if distanceExpr.GetMetricEnum() != planpb.DistanceMetric_DISTANCE_METRIC_UNSPECIFIED {
		hasValidMetric = true
	}
	if distanceExpr.GetMetricString() != "" {
		hasValidMetric = true
	}

	if !hasValidMetric {
		log.Ctx(e.ctx).Warn("No valid metric specified, will use default L2")
	}

	return nil
}

// convertProtoMetricToString convert proto distance metric enumeration to string
func (e *DistanceExecutor) convertProtoMetricToString(metric planpb.DistanceMetric) string {
	switch metric {
	case planpb.DistanceMetric_DISTANCE_METRIC_L2:
		return MetricTypeL2
	case planpb.DistanceMetric_DISTANCE_METRIC_IP:
		return MetricTypeIP
	case planpb.DistanceMetric_DISTANCE_METRIC_COSINE:
		return MetricTypeCOSINE
	case planpb.DistanceMetric_DISTANCE_METRIC_HAMMING:
		return MetricTypeHAMMING
	case planpb.DistanceMetric_DISTANCE_METRIC_JACCARD:
		return MetricTypeJACCARD
	default:
		return MetricTypeL2
	}
}

// ParseDistanceQuery parse distance query plan
func (p *DistanceQueryPlanner) ParseDistanceQuery(plan *planpb.PlanNode) (*planpb.DistanceExpr, error) {

	queryPlan := plan.GetQuery()
	if queryPlan == nil {
		return nil, fmt.Errorf("invalid query plan")
	}

	distanceExpr, err := p.extractDistanceExpression(queryPlan.GetPredicates())
	if err != nil {
		return nil, fmt.Errorf("failed to extract distance expression: %w", err)
	}

	if distanceExpr == nil {
		// Add detailed information to error message
		predicatesType := "nil"
		if queryPlan.GetPredicates() != nil {
			predicatesType = fmt.Sprintf("%T", queryPlan.GetPredicates().GetExpr())
		}

		// If no distance expression found in predicates but isDistanceQuery=true,
		// try to reconstruct distance expression from context
		if queryPlan.GetIsDistanceQuery() {
			distanceExpr = p.buildDistanceExprFromAliases("a", "b")
		}

		if distanceExpr == nil {
			return nil, fmt.Errorf("distance expression not found: predicatesType=%s, isDistanceQuery=%v",
				predicatesType, queryPlan.GetIsDistanceQuery())
		}
	}

	return distanceExpr, nil
}

// extractDistanceExpression extract distance calculation expression from expression
func (p *DistanceQueryPlanner) extractDistanceExpression(expr *planpb.Expr) (*planpb.DistanceExpr, error) {
	if expr == nil {
		return nil, nil
	}

	switch exprType := expr.GetExpr().(type) {
	case *planpb.Expr_DistanceExpr:
		// C++ layer successfully parsed distance expression, return directly
		return exprType.DistanceExpr, nil
	case *planpb.Expr_AliasedExpr:
		// Check distance calculation in aliased expression
		return p.extractDistanceExpression(exprType.AliasedExpr.GetExpr())
	case *planpb.Expr_AlwaysTrueExpr:
		// Fallback: construct basic distance expression
		distanceExpr := p.buildDistanceExprFromAliases("a", "b")
		if distanceExpr == nil {
			return nil, fmt.Errorf("failed to reconstruct distance expression from AlwaysTrueExpr context")
		}
		return distanceExpr, nil
	case *planpb.Expr_BinaryExpr:
		// Search for distance calculation in binary expression
		if leftDist, _ := p.extractDistanceExpression(exprType.BinaryExpr.GetLeft()); leftDist != nil {
			return leftDist, nil
		}
		if rightDist, _ := p.extractDistanceExpression(exprType.BinaryExpr.GetRight()); rightDist != nil {
			return rightDist, nil
		}
	case *planpb.Expr_UnaryExpr:
		// Search for distance calculation in unary expression
		return p.extractDistanceExpression(exprType.UnaryExpr.GetChild())
	case *planpb.Expr_UnaryRangeExpr:
		// Reconstruct distance expression in distance query context
		distanceExpr := p.buildDistanceExprFromAliases("a", "b")
		if distanceExpr == nil {
			return nil, fmt.Errorf("failed to reconstruct distance expression in UnaryRangeExpr context")
		}
		return distanceExpr, nil
	case *planpb.Expr_TermExpr:
		// Reconstruct distance expression in distance query context
		distanceExpr := p.buildDistanceExprFromAliases("a", "b")
		if distanceExpr == nil {
			return nil, fmt.Errorf("failed to reconstruct distance expression in TermExpr context")
		}
		return distanceExpr, nil
	case *planpb.Expr_CompareExpr:
		// Reconstruct distance expression in distance query context
		distanceExpr := p.buildDistanceExprFromAliases("a", "b")
		if distanceExpr == nil {
			return nil, fmt.Errorf("failed to reconstruct distance expression in CompareExpr context")
		}
		return distanceExpr, nil
	default:
		// Try to reconstruct for unknown types in distance query context
		distanceExpr := p.buildDistanceExprFromAliases("a", "b")
		if distanceExpr != nil {
			return distanceExpr, nil
		}
		// Return error if reconstruction fails
		return nil, fmt.Errorf("unknown expression type: %T", exprType)
	}

	return nil, nil
}

// getVectorFieldID dynamically get vector field ID
func (p *DistanceQueryPlanner) getVectorFieldID() (int64, error) {
	if p.schema == nil {
		return 0, fmt.Errorf("schema is empty, cannot get vector field ID")
	}

	// Find first vector field
	for _, field := range p.schema.GetFields() {
		if isVectorFieldType(field.GetDataType()) {
			return field.GetFieldID(), nil
		}
	}

	return 0, fmt.Errorf("no vector field found in schema")
}

// buildDistanceExprFromAliases builds proper distance expression from alias context
func (p *DistanceQueryPlanner) buildDistanceExprFromAliases(leftAlias, rightAlias string) *planpb.DistanceExpr {
	// If we have from sources, use actual aliases from them
	if len(p.fromSources) >= 2 {
		leftAlias = p.fromSources[0].GetAlias()
		rightAlias = p.fromSources[1].GetAlias()
	}

	// Get vector field ID for column expressions
	vectorFieldID, err := p.getVectorFieldID()
	if err != nil {
		return nil
	}

	// Create aliased expressions for left and right vectors
	leftVectorExpr := &planpb.Expr{
		Expr: &planpb.Expr_AliasedExpr{
			AliasedExpr: &planpb.AliasedExpr{
				Alias: leftAlias,
				Expr: &planpb.Expr{
					Expr: &planpb.Expr_ColumnExpr{
						ColumnExpr: &planpb.ColumnExpr{
							Info: &planpb.ColumnInfo{
								FieldId:  vectorFieldID,
								DataType: schemapb.DataType_FloatVector,
							},
						},
					},
				},
			},
		},
	}

	rightVectorExpr := &planpb.Expr{
		Expr: &planpb.Expr_AliasedExpr{
			AliasedExpr: &planpb.AliasedExpr{
				Alias: rightAlias,
				Expr: &planpb.Expr{
					Expr: &planpb.Expr_ColumnExpr{
						ColumnExpr: &planpb.ColumnExpr{
							Info: &planpb.ColumnInfo{
								FieldId:  vectorFieldID,
								DataType: schemapb.DataType_FloatVector,
							},
						},
					},
				},
			},
		},
	}

	distanceExpr := &planpb.DistanceExpr{
		LeftVector:  leftVectorExpr,
		RightVector: rightVectorExpr,
		Metric: &planpb.DistanceExpr_MetricEnum{
			MetricEnum: planpb.DistanceMetric_DISTANCE_METRIC_L2,
		},
	}

	return distanceExpr
}

// executeDistanceCalculation execute distance calculation
func (e *DistanceExecutor) executeDistanceCalculation(distanceExpr *planpb.DistanceExpr) ([]DistanceQueryResult, error) {
	// Get left vector data
	leftVectors, leftResults, err := e.getVectorData(distanceExpr.GetLeftVector())
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve left vector data: %w", err)
	}

	// Get right vector data
	rightVectors, rightResults, err := e.getVectorData(distanceExpr.GetRightVector())
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve right vector data: %w", err)
	}

	// Validate vector data
	if len(leftVectors) == 0 {
		return []DistanceQueryResult{}, nil
	}
	if len(rightVectors) == 0 {
		return []DistanceQueryResult{}, nil
	}

	// Validate vector dimension consistency
	if len(leftVectors[0]) != len(rightVectors[0]) {
		return nil, fmt.Errorf("vector dimension mismatch: left=%d, right=%d",
			len(leftVectors[0]), len(rightVectors[0]))
	}

	// Batch calculate distances
	distances, err := BatchCalculateDistances(leftVectors, rightVectors, e.metricType)
	if err != nil {
		return nil, fmt.Errorf("batch distance calculation failed: %w", err)
	}

	// Combine results with enhanced error handling
	expectedResultCount := len(leftResults) * len(rightResults)
	if len(distances) != expectedResultCount {
		return nil, fmt.Errorf("distance count mismatch: expected %d, got %d",
			expectedResultCount, len(distances))
	}

	results := make([]DistanceQueryResult, 0, len(distances))
	distanceIndex := 0

	for _, leftResult := range leftResults {
		for _, rightResult := range rightResults {
			if distanceIndex < len(distances) {
				result := DistanceQueryResult{
					LeftVectorID:  leftResult.ID,
					RightVectorID: rightResult.ID,
					Distance:      distances[distanceIndex],
					LeftFields:    leftResult.Fields,
					RightFields:   rightResult.Fields,
				}
				results = append(results, result)
				distanceIndex++
			}
		}
	}

	// Apply limit and offset
	if e.offset > 0 {
		if int64(len(results)) <= e.offset {
			results = []DistanceQueryResult{}
		} else {
			results = results[e.offset:]
		}
	}

	if e.limit > 0 && int64(len(results)) > e.limit {
		results = results[:e.limit]
	}

	return results, nil
}

// VectorDataResult vector data query result
type VectorDataResult struct {
	ID     interface{}           // record ID
	Vector []float32             // vector data
	Fields map[int64]interface{} // other field data
}

// getVectorData get vector data
func (e *DistanceExecutor) getVectorData(vectorExpr *planpb.Expr) ([][]float32, []VectorDataResult, error) {
	if vectorExpr == nil {
		return nil, nil, fmt.Errorf("vector expression is null")
	}

	switch expr := vectorExpr.GetExpr().(type) {
	case *planpb.Expr_ColumnExpr:
		// Column expression: get vector data from collection
		return e.getVectorDataFromColumn(expr.ColumnExpr)

	case *planpb.Expr_AliasedExpr:
		// Aliased expression: get vector data from aliased data source
		return e.getVectorDataFromAlias(expr.AliasedExpr)

	case *planpb.Expr_ValueExpr:
		// Value expression: might be external vector data
		return e.getVectorDataFromValue(expr.ValueExpr)

	default:
		return nil, nil, fmt.Errorf("unsupported vector expression type: %T", expr)
	}
}

// getVectorDataFromColumn get vector data from column expression
func (e *DistanceExecutor) getVectorDataFromColumn(columnExpr *planpb.ColumnExpr) ([][]float32, []VectorDataResult, error) {
	// Get field information
	fieldInfo := columnExpr.GetInfo()
	if fieldInfo == nil {
		return nil, nil, fmt.Errorf("column expression missing field info")
	}

	// Verify if it's a vector field
	field, err := e.getFieldByID(fieldInfo.GetFieldId())
	if err != nil {
		return nil, nil, fmt.Errorf("field ID %d not found: %w", fieldInfo.GetFieldId(), err)
	}

	if !isVectorField(field) {
		return nil, nil, fmt.Errorf("field %s is not a vector field", field.GetName())
	}

	// Query vector data from segments
	return e.queryVectorDataFromSegments(fieldInfo.GetFieldId())
}

// getVectorDataFromAlias get vector data from alias expression
func (e *DistanceExecutor) getVectorDataFromAlias(aliasedExpr *planpb.AliasedExpr) ([][]float32, []VectorDataResult, error) {
	alias := aliasedExpr.GetAlias()
	if alias == "" {
		return nil, nil, fmt.Errorf("aliased expression missing alias")
	}

	// Check if it's external vector data source
	if externalData, exists := e.getExternalVectorData(alias); exists {
		return e.convertExternalVectorData(externalData, alias)
	}

	// Find corresponding data source and filter condition from alias in from parameters
	fromSources := e.getFromSources()

	if fromSources != nil && len(fromSources) > 0 {
		for _, source := range fromSources {
			if source.GetAlias() == alias {
				// Query data from segments based on filter condition
				return e.queryVectorDataWithFilter(source.GetFilter())
			}
		}
	}

	// If no corresponding from source is found, get from internal expression of aliased expression
	return e.getVectorData(aliasedExpr.GetExpr())
}

// getVectorDataFromValue get vector data from value expression
func (e *DistanceExecutor) getVectorDataFromValue(valueExpr *planpb.ValueExpr) ([][]float32, []VectorDataResult, error) {
	// Value expressions may contain direct vector data
	// This case is rare, vector data is usually passed through alias or column expressions
	return nil, nil, fmt.Errorf("value expression vector data retrieval not yet implemented")
}

// queryVectorDataFromSegments query vector data from segments
func (e *DistanceExecutor) queryVectorDataFromSegments(fieldID int64) ([][]float32, []VectorDataResult, error) {
	if len(e.segments) == 0 {
		return [][]float32{}, []VectorDataResult{}, nil
	}

	// Collect query results from all segments
	allVectors := [][]float32{}
	allResults := []VectorDataResult{}

	for _, segment := range e.segments {
		if segment == nil {
			continue
		}

		segVectors, segResults, err := e.queryVectorDataFromSingleSegment(segment, fieldID)
		if err != nil {
			log.Ctx(e.ctx).Warn("failed to query segment",
				zap.Int64("segmentID", segment.ID()),
				zap.Error(err))
			continue // Continue processing other segments
		}

		allVectors = append(allVectors, segVectors...)
		allResults = append(allResults, segResults...)
	}

	return allVectors, allResults, nil
}

// queryVectorDataWithFilter query vector data based on filter condition
func (e *DistanceExecutor) queryVectorDataWithFilter(filter string) ([][]float32, []VectorDataResult, error) {
	if filter == "" {
		// If no filter condition, query all data
		return e.queryAllVectorData()
	}

	// Parse filter condition, currently supports simple "id IN [1,2,3]" format
	ids, err := e.parseIDInFilter(filter)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse filter condition: %w", err)
	}

	// Query vector data based on ID list
	vectors, results, err := e.queryVectorDataByIDs(ids)

	return vectors, results, err
}

// parseIDInFilter parse "id IN [1,2,3]" format filter condition
func (e *DistanceExecutor) parseIDInFilter(filter string) ([]int64, error) {
	// Simple regex parsing: id IN [1,2,3]
	// More complete implementation should use formal expression parser

	filter = strings.TrimSpace(filter)

	// Check if it's "id IN [...]" format
	if !strings.HasPrefix(strings.ToLower(filter), "id in [") {
		return nil, fmt.Errorf("currently only supports 'id IN [...]' filter format, got: %s", filter)
	}

	// Extract content within square brackets
	start := strings.Index(filter, "[")
	end := strings.LastIndex(filter, "]")
	if start == -1 || end == -1 || start >= end {
		return nil, fmt.Errorf("invalid filter format, cannot find valid ID list: %s", filter)
	}

	idListStr := filter[start+1 : end]
	idListStr = strings.TrimSpace(idListStr)

	if idListStr == "" {
		return []int64{}, nil
	}

	// Split ID list
	idStrings := strings.Split(idListStr, ",")
	ids := make([]int64, 0, len(idStrings))

	for _, idStr := range idStrings {
		idStr = strings.TrimSpace(idStr)
		if idStr == "" {
			continue
		}

		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ID: %s, error: %w", idStr, err)
		}

		ids = append(ids, id)
	}

	return ids, nil
}

// queryAllVectorData query all vector data (without filter condition)
func (e *DistanceExecutor) queryAllVectorData() ([][]float32, []VectorDataResult, error) {
	// Get vector field ID
	vectorFieldID, err := e.getVectorFieldID()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get vector field ID: %w", err)
	}

	// Query vector data from all segments (no filter)
	allVectors := [][]float32{}
	allResults := []VectorDataResult{}

	for _, segment := range e.segments {
		if segment == nil {
			continue
		}

		// Query all vector data for each segment
		vectors, results, err := e.queryAllVectorDataFromSegment(segment, vectorFieldID)
		if err != nil {
			log.Ctx(e.ctx).Warn("failed to query all vector data from segment",
				zap.Int64("segmentID", segment.ID()),
				zap.Error(err))
			continue
		}

		allVectors = append(allVectors, vectors...)
		allResults = append(allResults, results...)
	}

	return allVectors, allResults, nil
}

// queryVectorDataByIDs query vector data based on ID list
func (e *DistanceExecutor) queryVectorDataByIDs(ids []int64) ([][]float32, []VectorDataResult, error) {
	if len(ids) == 0 {
		return [][]float32{}, []VectorDataResult{}, nil
	}

	// 1. Create ID IN filter expression
	filterExpr, err := e.createIDInFilterExpression(ids)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create ID filter: %w", err)
	}

	// 2. Get vector field ID
	vectorFieldID, err := e.getVectorFieldID()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get vector field ID: %w", err)
	}

	// 3. Execute query from segments
	allVectors := [][]float32{}
	allResults := []VectorDataResult{}

	for i, segment := range e.segments {
		if segment == nil {
			continue
		}

		// Create RetrievePlan for each segment and execute query
		vectors, results, err := e.queryVectorDataFromSegmentWithFilter(segment, filterExpr, vectorFieldID)
		if err != nil {
			log.Ctx(e.ctx).Error("failed to query vector data from segment",
				zap.Int("segmentIndex", i),
				zap.Int64("segmentID", segment.ID()),
				zap.Error(err))
			// Continue processing other segments instead of returning error
			continue
		}

		allVectors = append(allVectors, vectors...)
		allResults = append(allResults, results...)
	}

	return allVectors, allResults, nil
}

// createIDInFilterExpression create ID IN filter expression
func (e *DistanceExecutor) createIDInFilterExpression(ids []int64) (*planpb.Expr, error) {
	if len(ids) == 0 {
		return nil, fmt.Errorf("ID list cannot be empty")
	}

	// Get primary key field information
	pkField, err := e.getPrimaryKeyField()
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key field: %w", err)
	}

	// Create GenericValue list
	values := make([]*planpb.GenericValue, 0, len(ids))
	for _, id := range ids {
		values = append(values, &planpb.GenericValue{
			Val: &planpb.GenericValue_Int64Val{
				Int64Val: id,
			},
		})
	}

	// Create TermExpr (IN expression)
	termExpr := &planpb.TermExpr{
		ColumnInfo: &planpb.ColumnInfo{
			FieldId:  pkField.GetFieldID(),
			DataType: pkField.GetDataType(),
		},
		Values: values,
	}

	// Wrap as Expr
	return &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: termExpr,
		},
	}, nil
}

// getPrimaryKeyField get primary key field information
func (e *DistanceExecutor) getPrimaryKeyField() (*schemapb.FieldSchema, error) {
	if e.coordinator == nil || e.coordinator.planner == nil || e.coordinator.planner.schema == nil {
		return nil, fmt.Errorf("schema information not available")
	}

	for _, field := range e.coordinator.planner.schema.GetFields() {
		if field.GetIsPrimaryKey() {
			return field, nil
		}
	}

	return nil, fmt.Errorf("primary key field not found")
}

// queryVectorDataFromSegmentWithFilter query vector data from single segment using filter
func (e *DistanceExecutor) queryVectorDataFromSegmentWithFilter(segment Segment, filterExpr *planpb.Expr, vectorFieldID int64) ([][]float32, []VectorDataResult, error) {
	// 获取主键字段ID
	pkField, err := e.getPrimaryKeyField()
	if err != nil {
		return nil, nil, fmt.Errorf("获取主键字段失败: %w", err)
	}

	// Create query plan containing filter and vector field
	outputFieldIds := []int64{vectorFieldID}
	// Ensure primary key field is in output
	if pkField.GetFieldID() != vectorFieldID {
		outputFieldIds = append(outputFieldIds, pkField.GetFieldID())
	}

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Predicates{
			Predicates: filterExpr,
		},
		OutputFieldIds: outputFieldIds,
	}

	// Serialize PlanNode to bytes
	planBytes, err := proto.Marshal(planNode)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to serialize query plan: %w", err)
	}

	// Get collection information
	collection := e.manager.Collection.Get(e.req.Req.GetCollectionID())
	if collection == nil {
		return nil, nil, fmt.Errorf("unable to get collection information")
	}

	// Create RetrievePlan - use correct request parameters
	retrievePlan, err := segcore.NewRetrievePlan(
		collection.GetCCollection(),
		planBytes,
		e.req.Req.GetMvccTimestamp(),           // Use correct timestamp from request
		e.req.Req.Base.GetMsgID(),              // Use message ID from request
		e.req.Req.GetConsistencyLevel(),        // Use consistency level from request
		e.req.Req.GetCollectionTtlTimestamps()) // Use TTL timestamps from request
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create RetrievePlan: %w", err)
	}
	defer retrievePlan.Delete()

	// Execute segment query
	result, err := segment.Retrieve(e.ctx, retrievePlan)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to execute segment query: %w", err)
	}

	// Parse query results
	vectors, results, err := e.extractVectorDataFromRetrieveResult(result, vectorFieldID)
	if err != nil {
		return nil, nil, err
	}

	return vectors, results, nil
}

// minInt helper function
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// extractVectorDataFromRetrieveResult extract vector data from query results
func (e *DistanceExecutor) extractVectorDataFromRetrieveResult(result *segcorepb.RetrieveResults, vectorFieldID int64) ([][]float32, []VectorDataResult, error) {
	if result == nil {
		return [][]float32{}, []VectorDataResult{}, nil
	}

	// Get ID data
	ids := result.GetIds()
	if ids == nil {
		return [][]float32{}, []VectorDataResult{}, nil
	}

	if ids.GetIdField() == nil {
		return [][]float32{}, []VectorDataResult{}, nil
	}

	var entityIDs []int64
	switch idData := ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		entityIDs = idData.IntId.GetData()
		maxShow := len(entityIDs)
		if maxShow > 5 {
			maxShow = 5
		}

	default:
		return nil, nil, fmt.Errorf("ID types not currently supported: %T", idData)
	}

	// Check if matching entities were found
	if len(entityIDs) == 0 {
		return [][]float32{}, []VectorDataResult{}, nil
	}

	// Find vector field data
	var vectorData [][]float32

	vectorFieldFound := false

	for _, fieldData := range result.GetFieldsData() {

		if fieldData.GetFieldId() == vectorFieldID {
			vectorFieldFound = true

			switch vectorField := fieldData.GetField().(type) {
			case *schemapb.FieldData_Vectors:
				switch vectorType := vectorField.Vectors.GetData().(type) {
				case *schemapb.VectorField_FloatVector:
					floats := vectorType.FloatVector.GetData()
					dim := vectorField.Vectors.GetDim()

					if len(floats) == 0 {
						break
					}

					if dim <= 0 {
						return nil, nil, fmt.Errorf("invalid vector dimension: %d", dim)
					}

					// 将一维float数组转换为二维向量数组
					vectorCount := len(floats) / int(dim)

					if len(floats)%int(dim) != 0 {
						return nil, nil, fmt.Errorf("the length of vector data does not match the dimension : %d %% %d != 0", len(floats), dim)
					}

					vectorData = make([][]float32, vectorCount)
					for i := 0; i < vectorCount; i++ {
						start := i * int(dim)
						end := start + int(dim)
						vectorData[i] = floats[start:end]
					}
				default:
					return nil, nil, fmt.Errorf("vector types not currently supported: %T", vectorType)
				}
			default:
				continue // Skip non vector fields
			}
			break
		}
	}

	if !vectorFieldFound {

		// 列出所有可用字段
		var availableFields []string
		for _, field := range result.GetFieldsData() {
			availableFields = append(availableFields, fmt.Sprintf("ID:%d,Type:%s", field.GetFieldId(), field.GetType().String()))
		}
		return nil, nil, fmt.Errorf("vector field ID %d not found", vectorFieldID)
	}

	if len(vectorData) != len(entityIDs) {
		// Not returning an error, taking the minimum value
		minCount := minInt(len(vectorData), len(entityIDs))
		if minCount == 0 {
			return [][]float32{}, []VectorDataResult{}, nil
		}
		vectorData = vectorData[:minCount]
		entityIDs = entityIDs[:minCount]
	}

	results := make([]VectorDataResult, len(entityIDs))
	for i, id := range entityIDs {
		results[i] = VectorDataResult{
			ID:     id,
			Fields: make(map[int64]interface{}),
		}
	}
	return vectorData, results, nil
}

// queryAllVectorDataFromSegment 从单个segment查询所有向量数据
func (e *DistanceExecutor) queryAllVectorDataFromSegment(segment Segment, vectorFieldID int64) ([][]float32, []VectorDataResult, error) {
	pkField, err := e.getPrimaryKeyField()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve primary key field: %w", err)
	}

	outputFieldIds := []int64{vectorFieldID}
	// Ensure primary key field is in output
	if pkField.GetFieldID() != vectorFieldID {
		outputFieldIds = append(outputFieldIds, pkField.GetFieldID())
	}

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				Predicates: nil,
			},
		},
		OutputFieldIds: outputFieldIds,
	}

	// Serialize PlanNode to bytes
	planBytes, err := proto.Marshal(planNode)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to serialize query plan: %w", err)
	}

	// Get collection information
	collection := e.manager.Collection.Get(e.req.Req.GetCollectionID())
	if collection == nil {
		return nil, nil, fmt.Errorf("unable to get collection information")
	}

	// Create RetrievePlan - use correct request parameters
	retrievePlan, err := segcore.NewRetrievePlan(
		collection.GetCCollection(),
		planBytes,
		e.req.Req.GetMvccTimestamp(),           // Use correct timestamp from request
		e.req.Req.Base.GetMsgID(),              // Use message ID from request
		e.req.Req.GetConsistencyLevel(),        // Use consistency level from request
		e.req.Req.GetCollectionTtlTimestamps()) // Use TTL timestamps from request
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create RetrievePlan: %w", err)
	}
	defer retrievePlan.Delete()

	// Execute segment query
	result, err := segment.Retrieve(e.ctx, retrievePlan)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to execute segment query: %w", err)
	}

	// Parse query results
	return e.extractVectorDataFromRetrieveResult(result, vectorFieldID)
}

// queryVectorDataFromSingleSegment Querying Vector Data from a Single Segment - Using Real Segment Query Logic
func (e *DistanceExecutor) queryVectorDataFromSingleSegment(segment Segment, fieldID int64) ([][]float32, []VectorDataResult, error) {
	pkField, err := e.getPrimaryKeyField()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve primary key field: %w", err)
	}

	outputFieldIds := []int64{fieldID}
	// Ensure primary key field is in output
	if pkField.GetFieldID() != fieldID {
		outputFieldIds = append(outputFieldIds, pkField.GetFieldID())
	}

	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				Predicates: nil,
			},
		},
		OutputFieldIds: outputFieldIds,
	}

	// Serialize PlanNode to bytes
	planBytes, err := proto.Marshal(planNode)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to serialize query plan: %w", err)
	}

	// Get collection information
	collection := e.manager.Collection.Get(e.req.Req.GetCollectionID())
	if collection == nil {
		return nil, nil, fmt.Errorf("unable to get collection information")
	}

	// Create RetrievePlan - use correct request parameters
	retrievePlan, err := segcore.NewRetrievePlan(
		collection.GetCCollection(),
		planBytes,
		e.req.Req.GetMvccTimestamp(),           // Use correct timestamp from request
		e.req.Req.Base.GetMsgID(),              // Use message ID from request
		e.req.Req.GetConsistencyLevel(),        // Use consistency level from request
		e.req.Req.GetCollectionTtlTimestamps()) // Use TTL timestamps from request
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create RetrievePlan: %w", err)
	}
	defer retrievePlan.Delete()

	// Execute segment query
	result, err := segment.Retrieve(e.ctx, retrievePlan)
	if err != nil {
		log.Ctx(e.ctx).Warn("向量数据获取失败",
			zap.Int64("segmentID", segment.ID()),
			zap.Int64("fieldID", fieldID),
			zap.Error(err))
		return nil, nil, fmt.Errorf("failed to retrieve vector data from segment: %w", err)
	}

	// Use extractVectorDataMromRetrieve Result to parse the results
	vectors, results, err := e.extractVectorDataFromRetrieveResult(result, fieldID)
	return vectors, results, err
}

// extractVectorDataFromSegcoreResult
func (e *DistanceExecutor) extractVectorDataFromSegcoreResult(result *segcorepb.RetrieveResults, fieldID int64) ([][]float32, []VectorDataResult, error) {
	if result == nil {
		return [][]float32{}, []VectorDataResult{}, nil
	}

	// 查找指定的向量字段
	var vectorField *schemapb.FieldData
	for _, field := range result.GetFieldsData() {
		if field.GetFieldId() == fieldID {
			vectorField = field
			break
		}
	}

	if vectorField == nil {
		return nil, nil, fmt.Errorf("vector data for field ID %d not found", fieldID)
	}

	vectors, err := e.extractVectorsFromFieldData(vectorField)
	if err != nil {
		return nil, nil, fmt.Errorf("extracting vector data failed: %w", err)
	}

	// Get ID data
	ids := e.extractIDsFromResult(result)

	results := make([]VectorDataResult, len(vectors))
	for i, vector := range vectors {
		var id interface{} = int64(i) // 默认ID
		if i < len(ids) {
			id = ids[i]
		}

		results[i] = VectorDataResult{
			ID:     id,
			Vector: vector,
			Fields: e.extractFieldsFromResult(result, i),
		}
	}

	return vectors, results, nil
}

// extractVectorsFromFieldData 从FieldData中提取向量
func (e *DistanceExecutor) extractVectorsFromFieldData(fieldData *schemapb.FieldData) ([][]float32, error) {
	switch fieldData.GetType() {
	case schemapb.DataType_FloatVector:
		// 处理浮点向量
		vectorData := fieldData.GetVectors().GetFloatVector()
		if vectorData == nil {
			return nil, fmt.Errorf("floating point vector data is empty")
		}

		// 从字段schema中获取向量维度
		field, err := e.getFieldByID(fieldData.GetFieldId())
		if err != nil {
			return nil, fmt.Errorf("field not found schema %d: %w", fieldData.GetFieldId(), err)
		}

		dim := getDimensionFromField(field)
		if dim <= 0 {
			return nil, fmt.Errorf("invalid vector dimension: %d", dim)
		}

		data := vectorData.GetData()

		if len(data)%dim != 0 {
			return nil, fmt.Errorf("the length of vector data does not match the dimension: %d %% %d != 0", len(data), dim)
		}

		vectorCount := len(data) / dim
		vectors := make([][]float32, vectorCount)

		for i := 0; i < vectorCount; i++ {
			start := i * dim
			end := start + dim
			vectors[i] = data[start:end]
		}

		return vectors, nil

	case schemapb.DataType_BinaryVector:
		// 当前不支持二进制向量，可以在未来扩展
		return nil, fmt.Errorf("binary vectors are not currently supported")

	case schemapb.DataType_SparseFloatVector:
		// 当前不支持稀疏向量，可以在未来扩展
		return nil, fmt.Errorf("sparse vectors are not currently supported")

	default:
		return nil, fmt.Errorf("unsupported vector types: %v", fieldData.GetType())
	}
}

// extractIDsFromResult
func (e *DistanceExecutor) extractIDsFromResult(result *segcorepb.RetrieveResults) []interface{} {
	ids := result.GetIds()
	if ids == nil {
		return []interface{}{}
	}

	switch idData := ids.GetIdField().(type) {
	case *schemapb.IDs_IntId:
		intIds := idData.IntId.GetData()
		idList := make([]interface{}, len(intIds))
		for i, id := range intIds {
			idList[i] = id
		}
		return idList

	case *schemapb.IDs_StrId:
		strIds := idData.StrId.GetData()
		idList := make([]interface{}, len(strIds))
		for i, id := range strIds {
			idList[i] = id
		}
		return idList

	default:
		return []interface{}{}
	}
}

// extractFieldsFromResult 从查询结果中提取字段数据
func (e *DistanceExecutor) extractFieldsFromResult(result *segcorepb.RetrieveResults, rowIndex int) map[int64]interface{} {
	fields := make(map[int64]interface{})

	for _, fieldData := range result.GetFieldsData() {
		fieldID := fieldData.GetFieldId()

		// Skip vector fields and only extract scalar fields
		if isVectorFieldType(fieldData.GetType()) {
			continue
		}

		value := e.extractScalarValueFromField(fieldData, rowIndex)
		if value != nil {
			fields[fieldID] = value
		}
	}

	return fields
}

// extractScalarValueFromField
func (e *DistanceExecutor) extractScalarValueFromField(fieldData *schemapb.FieldData, rowIndex int) interface{} {
	scalars := fieldData.GetScalars()
	if scalars == nil {
		return nil
	}

	switch data := scalars.GetData().(type) {
	case *schemapb.ScalarField_LongData:
		if rowIndex < len(data.LongData.GetData()) {
			return data.LongData.GetData()[rowIndex]
		}
	case *schemapb.ScalarField_IntData:
		if rowIndex < len(data.IntData.GetData()) {
			return data.IntData.GetData()[rowIndex]
		}
	case *schemapb.ScalarField_FloatData:
		if rowIndex < len(data.FloatData.GetData()) {
			return data.FloatData.GetData()[rowIndex]
		}
	case *schemapb.ScalarField_DoubleData:
		if rowIndex < len(data.DoubleData.GetData()) {
			return data.DoubleData.GetData()[rowIndex]
		}
	case *schemapb.ScalarField_StringData:
		if rowIndex < len(data.StringData.GetData()) {
			return data.StringData.GetData()[rowIndex]
		}
	case *schemapb.ScalarField_BoolData:
		if rowIndex < len(data.BoolData.GetData()) {
			return data.BoolData.GetData()[rowIndex]
		}
	}

	return nil
}

// isVectorFieldType 检查是否为向量字段类型
func isVectorFieldType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_SparseFloatVector:
		return true
	default:
		return false
	}
}

// getDimensionFromField 从字段schema中获取向量维度
func getDimensionFromField(field *schemapb.FieldSchema) int {
	for _, param := range field.GetTypeParams() {
		if param.GetKey() == "dim" {
			if dim, err := strconv.Atoi(param.GetValue()); err == nil {
				return dim
			}
		}
	}
	return 0
}

// convertExternalVectorData 转换外部向量数据
func (e *DistanceExecutor) convertExternalVectorData(externalData *planpb.ExternalVectorData, alias string) ([][]float32, []VectorDataResult, error) {
	vectors := make([][]float32, 0, len(externalData.GetVectors()))
	results := make([]VectorDataResult, 0, len(externalData.GetVectors()))

	for i, genericValue := range externalData.GetVectors() {
		// 从 GenericValue 中提取向量数据
		vector, err := e.extractVectorFromGenericValue(genericValue)
		if err != nil {
			return nil, nil, fmt.Errorf("提取向量 %d 失败: %w", i, err)
		}

		vectors = append(vectors, vector)
		results = append(results, VectorDataResult{
			ID:     int64(i), // 使用索引作为ID
			Vector: vector,
			Fields: map[int64]interface{}{
				-1: int64(i), // 外部向量使用负数字段ID
			},
		})
	}

	return vectors, results, nil
}

// extractVectorFromGenericValue 从 GenericValue 提取向量
func (e *DistanceExecutor) extractVectorFromGenericValue(genericValue *planpb.GenericValue) ([]float32, error) {
	if genericValue == nil {
		return nil, fmt.Errorf("GenericValue is empty")
	}

	switch val := genericValue.GetVal().(type) {
	case *planpb.GenericValue_ArrayVal:
		// 从数组值中提取向量
		array := val.ArrayVal
		if array == nil {
			return nil, fmt.Errorf("the array value is empty")
		}

		vector := make([]float32, len(array.GetArray()))
		for i, element := range array.GetArray() {
			floatVal, err := e.extractFloatFromGenericValue(element)
			if err != nil {
				return nil, fmt.Errorf("extract array elements %d faile: %w", i, err)
			}
			vector[i] = floatVal
		}
		return vector, nil

	default:
		return nil, fmt.Errorf("unsupported GenericValue type: %T", val)
	}
}

// extractFloatFromGenericValue 从 GenericValue 提取 float 值
func (e *DistanceExecutor) extractFloatFromGenericValue(genericValue *planpb.GenericValue) (float32, error) {
	if genericValue == nil {
		return 0, fmt.Errorf("GenericValue is empty")
	}

	switch val := genericValue.GetVal().(type) {
	case *planpb.GenericValue_FloatVal:
		return float32(val.FloatVal), nil
	case *planpb.GenericValue_Int64Val:
		return float32(val.Int64Val), nil
	default:
		return 0, fmt.Errorf("unsupported type: %T", val)
	}
}

// 辅助方法
func (f *DistanceResultFormatter) getFieldByID(fieldID int64) *schemapb.FieldSchema {
	if f.schema == nil {
		return nil
	}
	for _, field := range f.schema.GetFields() {
		if field.GetFieldID() == fieldID {
			return field
		}
	}
	return nil
}

func isVectorField(field *schemapb.FieldSchema) bool {
	switch field.GetDataType() {
	case schemapb.DataType_FloatVector, schemapb.DataType_BinaryVector, schemapb.DataType_SparseFloatVector:
		return true
	default:
		return false
	}
}

func (e *DistanceExecutor) getExternalVectorData(alias string) (*planpb.ExternalVectorData, bool) {
	// 从查询计划或请求中获取外部向量数据
	// 目前从计划中查找
	for _, source := range e.getFromSources() {
		if source.GetAlias() == alias {
			if externalVectors := source.GetExternalVectors(); externalVectors != nil {
				return externalVectors, true
			}
		}
	}
	return nil, false
}

// getOffsetFromRequest 从请求中获取 offset 值
func getOffsetFromRequest(req *querypb.QueryRequest) int64 {
	// 当前版本: 返回默认值 0
	// 未来扩展: 可以从查询计划或请求参数中解析 offset
	// 例如: 支持 SQL 语法中的 OFFSET 子句
	return 0
}

func (e *DistanceExecutor) getFromSources() []*planpb.QueryFromSource {
	// 首先尝试从查询计划中获取数据源信息
	if queryPlan := e.plan.GetQuery(); queryPlan != nil {
		fromSources := queryPlan.GetFromSources()
		if len(fromSources) > 0 {
			return fromSources
		}
	}

	// 如果PlanNode中没有from sources，尝试从RetrieveRequest中获取
	// 这是一个fallback机制，因为from参数可能没有被正确传递到PlanNode
	if e.req != nil && e.req.GetReq() != nil {
		retrieveReq := e.req.GetReq()
		if retrieveReq.GetFromSources() != nil && len(retrieveReq.GetFromSources()) > 0 {
			return retrieveReq.GetFromSources()
		}
	}

	return nil
}

// FormatResults 格式化查询结果 - 修复距离数据被删除问题
func (f *DistanceResultFormatter) FormatResults(results []DistanceQueryResult) (*internalpb.RetrieveResults, error) {
	if len(results) == 0 {
		return &internalpb.RetrieveResults{
			Ids:        &schemapb.IDs{},
			FieldsData: make([]*schemapb.FieldData, 0),
		}, nil
	}

	// 创建ID字段
	idField, err := f.createIdField(results)
	if err != nil {
		return nil, fmt.Errorf("failed to create ID field: %w", err)
	}

	// 创建结果容器
	retrieveResults := &internalpb.RetrieveResults{
		Ids:        idField,
		FieldsData: make([]*schemapb.FieldData, 0, len(f.outputFieldIDs)+1), // +1为距离字段
	}

	// 按照 outputFieldIDs 的顺序创建每个字段
	for _, fieldID := range f.outputFieldIDs {
		var fieldData *schemapb.FieldData
		var err error

		if fieldID == common.TimeStampField {
			// Create system timestamp field
			fieldData = f.createTimestampField(results)
		} else if f.isDistanceField(fieldID) {
			// Create distance field
			fieldData = f.createDistanceFieldWithID(results, fieldID)
		} else {
			// Create regular field
			fieldData, err = f.extractOutputField(fieldID, results)
			if err != nil {
				continue
			}
		}

		if fieldData != nil {
			retrieveResults.FieldsData = append(retrieveResults.FieldsData, fieldData)
		}
	}

	// 添加独立的距离字段（使用不会被删除的字段ID）
	distanceField := &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: "_distance",
		FieldId:   DistanceFieldID, // 使用常量避免冲突，类似系统字段
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: func() []float32 {
							distances := make([]float32, len(results))
							for i, result := range results {
								distances[i] = result.Distance
							}
							return distances
						}(),
					},
				},
			},
		},
	}
	retrieveResults.FieldsData = append(retrieveResults.FieldsData, distanceField)

	return retrieveResults, nil
}

// isDistanceField 判断字段ID是否是距离字段
func (f *DistanceResultFormatter) isDistanceField(fieldID int64) bool {
	// 直接检查字段ID是否在距离字段列表中
	for _, distanceFieldID := range f.distanceFieldIDs {
		if distanceFieldID == fieldID {
			return true
		}
	}

	return false
}

// createDistanceFieldWithID 创建带指定字段ID的距离字段
func (f *DistanceResultFormatter) createDistanceFieldWithID(results []DistanceQueryResult, fieldID int64) *schemapb.FieldData {
	distances := make([]float32, len(results))
	for i, result := range results {
		distances[i] = result.Distance
	}

	// 对于距离字段，总是使用距离相关的字段名，不要重用系统字段名
	fieldName := "_distance"

	// 如果fieldID是系统字段ID，不要使用系统字段名，避免冲突
	if fieldID == common.TimeStampField {
		fieldName = "_distance"
	} else if f.schema != nil {
		// 只有在不是系统字段时，才尝试从schema获取字段名
		for _, field := range f.schema.GetFields() {
			if field.GetFieldID() == fieldID {
				originalName := field.GetName()
				// 如果原始字段名不是系统字段名，可以参考使用
				if !strings.Contains(originalName, "Timestamp") && !strings.Contains(originalName, "RowID") {
					fieldName = originalName
				}
				break
			}
		}
	}

	return &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: fieldName,
		FieldId:   fieldID, // 使用正确的字段ID
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: distances,
					},
				},
			},
		},
	}
}

// createIdField 动态创建ID字段，支持多种类型
func (f *DistanceResultFormatter) createIdField(results []DistanceQueryResult) (*schemapb.IDs, error) {
	if len(results) == 0 {
		return &schemapb.IDs{}, nil
	}

	// 检查第一个结果的ID类型来确定整体类型
	switch results[0].LeftVectorID.(type) {
	case int64:
		return f.createIntIdField(results)
	case string:
		return f.createStringIdField(results)
	default:
		// 尝试转换为int64
		return f.createIntIdFieldWithConversion(results)
	}
}

// extractOutputField 提取指定的输出字段
func (f *DistanceResultFormatter) extractOutputField(fieldID int64, results []DistanceQueryResult) (*schemapb.FieldData, error) {
	if len(results) == 0 {
		return nil, nil
	}

	// 查找字段schema
	field := f.getFieldByID(fieldID)
	if field == nil {
		return nil, fmt.Errorf("field schema not found: %d", fieldID)
	}

	// 收集字段数据
	var fieldValues []interface{}
	for _, result := range results {
		var value interface{}

		// 对于主键字段，使用LeftVectorID作为值
		if field.GetIsPrimaryKey() {
			value = result.LeftVectorID
		} else {
			// 优先从左侧字段获取，如果没有则从右侧获取
			if val, exists := result.LeftFields[fieldID]; exists {
				value = val
			} else if val, exists := result.RightFields[fieldID]; exists {
				value = val
			} else {
				value = nil // 填充默认值
			}
		}

		fieldValues = append(fieldValues, value)
	}

	// 根据字段类型创建FieldData
	return f.createFieldDataFromValues(field, fieldValues)
}

// createFieldDataFromValues 根据字段类型和值创建FieldData
func (f *DistanceResultFormatter) createFieldDataFromValues(field *schemapb.FieldSchema, values []interface{}) (*schemapb.FieldData, error) {
	fieldData := &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		FieldId:   field.GetFieldID(),
	}

	switch field.GetDataType() {
	case schemapb.DataType_Int64:
		int64Data := make([]int64, len(values))
		for i, val := range values {
			if val != nil {
				if v, ok := val.(int64); ok {
					int64Data[i] = v
				}
			}
		}
		fieldData.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: int64Data},
				},
			},
		}

	case schemapb.DataType_VarChar:
		stringData := make([]string, len(values))
		for i, val := range values {
			if val != nil {
				if v, ok := val.(string); ok {
					stringData[i] = v
				}
			}
		}
		fieldData.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: stringData},
				},
			},
		}

	// 可以继续添加其他数据类型的支持
	default:
		return nil, fmt.Errorf("暂不支持的字段类型: %v", field.GetDataType())
	}

	return fieldData, nil
}

// ExecuteDistanceQuery 执行距离查询 - 核心实现
func ExecuteDistanceQuery(
	ctx context.Context,
	manager *Manager,
	schema *schemapb.CollectionSchema,
	segments []Segment,
	plan *planpb.PlanNode,
	req *querypb.QueryRequest,
) (*internalpb.RetrieveResults, error) {
	// 使用之前优化后的DistanceExecutor
	executor := NewDistanceExecutor(ctx, manager, schema, segments, plan, req)

	// 执行距离计算
	result, err := executor.Execute()
	if err != nil {
		return nil, fmt.Errorf("distance query execution failed: %w", err)
	}

	return result, nil
}

// getVectorFieldID 获取向量字段ID
func (e *DistanceExecutor) getVectorFieldID() (int64, error) {
	// 从请求中获取集合ID
	collectionID := e.req.GetReq().GetCollectionID()
	if collectionID == 0 {
		return 0, fmt.Errorf("无效的集合ID")
	}

	// 从 manager获取collection
	collection := e.manager.Collection.Get(collectionID)
	if collection == nil {
		return 0, fmt.Errorf("未找到集合: %d", collectionID)
	}

	// 获取schema
	schema := collection.Schema()
	if schema == nil {
		return 0, fmt.Errorf("schema为空")
	}

	// Find first vector field
	for _, field := range schema.GetFields() {
		if isVectorFieldType(field.GetDataType()) {
			return field.GetFieldID(), nil
		}
	}

	return 0, fmt.Errorf("no vector field found in schema")
}

type distanceQueryPerformanceMonitor struct {
	startTime    time.Time
	phases       map[string]phaseInfo
	metrics      map[string]interface{}
	errors       []errorInfo
	currentPhase string
}

// phaseInfo 阶段执行信息
type phaseInfo struct {
	startTime time.Time
	duration  time.Duration
	completed bool
}

// errorInfo 错误信息
type errorInfo struct {
	phase   string
	message string
	time    time.Time
}

// newDistanceQueryPerformanceMonitor 创建性能监控器
func newDistanceQueryPerformanceMonitor() *distanceQueryPerformanceMonitor {
	return &distanceQueryPerformanceMonitor{
		startTime: time.Now(),
		phases:    make(map[string]phaseInfo),
		metrics:   make(map[string]interface{}),
		errors:    make([]errorInfo, 0),
	}
}

// markPhaseStart 标记阶段开始
func (pm *distanceQueryPerformanceMonitor) markPhaseStart(phaseName string) {
	pm.currentPhase = phaseName
	pm.phases[phaseName] = phaseInfo{
		startTime: time.Now(),
		completed: false,
	}
}

// markPhaseEnd 标记阶段结束
func (pm *distanceQueryPerformanceMonitor) markPhaseEnd(phaseName string) {
	if phase, exists := pm.phases[phaseName]; exists {
		phase.duration = time.Since(phase.startTime)
		phase.completed = true
		pm.phases[phaseName] = phase
	}
}

// recordMetric 记录性能指标
func (pm *distanceQueryPerformanceMonitor) recordMetric(name string, value interface{}) {
	pm.metrics[name] = value
}

// recordError 记录错误
func (pm *distanceQueryPerformanceMonitor) recordError(phase, message string) {
	pm.errors = append(pm.errors, errorInfo{
		phase:   phase,
		message: message,
		time:    time.Now(),
	})
}

// getFieldIDByName 根据字段名获取字段ID
func (e *DistanceExecutor) getFieldIDByName(fieldName string) int64 {
	// 从请求中获取集合ID
	collectionID := e.req.GetReq().GetCollectionID()
	if collectionID == 0 {
		return -1
	}

	// 从 manager获取collection
	collection := e.manager.Collection.Get(collectionID)
	if collection == nil {
		return -1
	}

	// 获取schema
	schema := collection.Schema()
	if schema == nil {
		return -1
	}

	for _, field := range schema.GetFields() {
		if field.GetName() == fieldName {
			return field.GetFieldID()
		}
	}

	return -1
}

// validateSchema 验证schema是否包含必要字段
func (e *DistanceExecutor) validateSchema() error {
	// 从请求中获取集合ID
	collectionID := e.req.GetReq().GetCollectionID()
	if collectionID == 0 {
		return fmt.Errorf("invalid collection ID")
	}

	// 从 manager获取collection
	collection := e.manager.Collection.Get(collectionID)
	if collection == nil {
		return fmt.Errorf("collection not found: %d", collectionID)
	}

	// 获取schema
	schema := collection.Schema()
	if schema == nil {
		return fmt.Errorf("schema is empty")
	}

	// 检查是否有向量字段
	hasVectorField := false
	for _, field := range schema.GetFields() {
		if field.GetDataType() == schemapb.DataType_FloatVector ||
			field.GetDataType() == schemapb.DataType_BinaryVector {
			hasVectorField = true
			break
		}
	}

	if !hasVectorField {
		return fmt.Errorf("vector field not found in schema")
	}

	return nil
}

// getFieldByID 获取指定ID的字段schema
func (e *DistanceExecutor) getFieldByID(fieldID int64) (*schemapb.FieldSchema, error) {
	// 从请求中获取集合ID
	collectionID := e.req.GetReq().GetCollectionID()
	if collectionID == 0 {
		return nil, fmt.Errorf("invalid collection ID")
	}

	// 从manager获取collection
	collection := e.manager.Collection.Get(collectionID)
	if collection == nil {
		return nil, fmt.Errorf("collection not found: %d", collectionID)
	}

	// 获取schema
	schema := collection.Schema()
	if schema == nil {
		return nil, fmt.Errorf("schema is empty")
	}

	// 查找指定字段
	for _, field := range schema.GetFields() {
		if field.GetFieldID() == fieldID {
			return field, nil
		}
	}

	return nil, fmt.Errorf("未找到字段ID: %d", fieldID)
}

// DistanceResultFormatter的缺失方法实现

// createTimestampField 创建时间戳字段 - 正常的timestamp数据
func (f *DistanceResultFormatter) createTimestampField(results []DistanceQueryResult) *schemapb.FieldData {
	// 创建当前时间戳数组，所有结果使用相同的时间戳
	currentTime := time.Now().UnixNano() / int64(time.Millisecond)
	timestamps := make([]int64, len(results))
	for i := range timestamps {
		timestamps[i] = currentTime
	}

	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: common.TimeStampFieldName,
		FieldId:   common.TimeStampField,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: timestamps,
					},
				},
			},
		},
	}
}

// createIntIdField 创建整数ID字段
func (f *DistanceResultFormatter) createIntIdField(results []DistanceQueryResult) (*schemapb.IDs, error) {
	ids := make([]int64, len(results))
	for i, result := range results {
		if id, ok := result.LeftVectorID.(int64); ok {
			ids[i] = id
		}
	}

	return &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: ids,
			},
		},
	}, nil
}

// createStringIdField 创建字符串ID字段
func (f *DistanceResultFormatter) createStringIdField(results []DistanceQueryResult) (*schemapb.IDs, error) {
	ids := make([]string, len(results))
	for i, result := range results {
		if id, ok := result.LeftVectorID.(string); ok {
			ids[i] = id
		}
	}

	return &schemapb.IDs{
		IdField: &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: ids,
			},
		},
	}, nil
}

// createIntIdFieldWithConversion 创建带转换的整数ID字段
func (f *DistanceResultFormatter) createIntIdFieldWithConversion(results []DistanceQueryResult) (*schemapb.IDs, error) {
	ids := make([]int64, len(results))
	for i, result := range results {
		// 尝试转换为int64
		switch v := result.LeftVectorID.(type) {
		case int:
			ids[i] = int64(v)
		case int32:
			ids[i] = int64(v)
		case float64:
			ids[i] = int64(v)
		default:
			ids[i] = 0 // 默认值
		}
	}

	return &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: ids,
			},
		},
	}, nil
}
