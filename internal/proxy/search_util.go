package proxy

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type rankParams struct {
	limit             int64
	offset            int64
	roundDecimal      int64
	groupByFieldIds   []int64
	groupByFieldNames []string
	groupSize         int64
	strictGroupSize   bool
}

func (r *rankParams) GetLimit() int64 {
	if r != nil {
		return r.limit
	}
	return 0
}

func (r *rankParams) GetOffset() int64 {
	if r != nil {
		return r.offset
	}
	return 0
}

func (r *rankParams) GetRoundDecimal() int64 {
	if r != nil {
		return r.roundDecimal
	}
	return 0
}

// GetGroupByFieldId returns the first group-by field id, or -1 when none is set.
// Kept as a single-field convenience for call sites that have not yet migrated
// to the multi-field plural accessor.
func (r *rankParams) GetGroupByFieldId() int64 {
	if r != nil && len(r.groupByFieldIds) > 0 {
		return r.groupByFieldIds[0]
	}
	return -1
}

// GetGroupByFieldName returns the first group-by field name, or "" when none is set.
func (r *rankParams) GetGroupByFieldName() string {
	if r != nil && len(r.groupByFieldNames) > 0 {
		return r.groupByFieldNames[0]
	}
	return ""
}

func (r *rankParams) GetGroupByFieldIds() []int64 {
	if r != nil {
		return r.groupByFieldIds
	}
	return nil
}

func (r *rankParams) GetGroupByFieldNames() []string {
	if r != nil {
		return r.groupByFieldNames
	}
	return nil
}

func (r *rankParams) GetGroupSize() int64 {
	if r != nil {
		return r.groupSize
	}
	return 1
}

func (r *rankParams) GetStrictGroupSize() bool {
	if r != nil {
		return r.strictGroupSize
	}
	return false
}

func (r *rankParams) String() string {
	return fmt.Sprintf("limit: %d, offset: %d, roundDecimal: %d", r.GetLimit(), r.GetOffset(), r.GetRoundDecimal())
}

// OrderByField represents a field to order by with its direction
// Supports JSON subfield paths like metadata["price"] and dynamic fields
//
// Dynamic field handling:
// When ordering by a dynamic field (e.g., "age" stored in $meta), the field lookup uses FieldName ("$meta"),
// NOT OutputFieldName ("age"). This is because:
//  1. C++ segcore creates DataArray with field_id only (no field_name)
//  2. Delegator reduction copies the empty field_name from source
//  3. Proxy's default_limit_reducer sets FieldName from schema (field.GetName() = "$meta")
//
// So after requery, the returned FieldData has FieldName="$meta", and JSONPath="/age" is used
// to extract the actual value from the JSON data for comparison.
type OrderByField struct {
	FieldName       string // Top-level field name for result lookup (e.g., "metadata" or "$meta" for dynamic fields)
	FieldID         int64  // Field ID for validation
	JSONPath        string // JSON Pointer format: "/price" or "/user/age" (empty for non-JSON fields)
	Ascending       bool   // true for ASC, false for DESC
	OutputFieldName string // Field name to request in requery (e.g., "age" for dynamic fields, "metadata" for JSON fields)
	IsDynamicField  bool   // true if this is a dynamic field (uses $meta extraction at QueryNode)
}

type SearchInfo struct {
	planInfo        *planpb.QueryInfo
	offset          int64
	isIterator      bool
	collectionID    int64
	orderByFields   []OrderByField
	iterativeFilter bool
}

// DetermineSearchType classifies the search based on the parsed search info
// and whether a filter expression is present. The caller supplies hasFilter
// because the DSL/expression is not available inside parseSearchInfo.
func (s *SearchInfo) DetermineSearchType(hasFilter bool) internalpb.SearchType {
	isRangeSearch := gjson.Get(s.planInfo.GetSearchParams(), radiusKey).Exists()
	hasGroupBy := s.planInfo.GetGroupByFieldId() > 0 || len(s.planInfo.GetGroupByFieldIds()) > 0
	if isRangeSearch || hasGroupBy || s.isIterator || s.iterativeFilter {
		return internalpb.SearchType_DEFAULT
	}
	if hasFilter {
		return internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER
	}
	return internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER
}

// parseOrderByFields parses the order_by_fields parameter from search params.
// Format: "field1:asc,field2:desc" or "field1,field2" (default is asc)
// Supports JSON subfield paths: metadata["price"]:asc, metadata["user"]["score"]:desc
// Supports dynamic fields: age:desc (maps to $meta["age"])
// Validates that fields exist in schema and are sortable types.
func parseOrderByFields(searchParamsPair []*commonpb.KeyValuePair, schema *schemapb.CollectionSchema) ([]OrderByField, error) {
	orderByStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OrderByFieldsKey, searchParamsPair)
	if err != nil || orderByStr == "" {
		return nil, nil
	}

	// Build field name to schema map and find dynamic field
	fieldSchemaMap := make(map[string]*schemapb.FieldSchema)
	var dynamicField *schemapb.FieldSchema
	for _, field := range schema.GetFields() {
		fieldSchemaMap[field.GetName()] = field
		if field.GetIsDynamic() {
			dynamicField = field
		}
	}

	var orderByFields []OrderByField
	pairs := strings.Split(orderByStr, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		// Split field spec and direction, handling brackets in field spec
		// e.g., "metadata[\"price\"]:asc" -> fieldSpec="metadata[\"price\"]", direction="asc"
		fieldSpec, direction := splitOrderByFieldAndDirection(pair)
		if fieldSpec == "" {
			return nil, fmt.Errorf("empty field name in order_by_fields")
		}

		// Parse direction
		ascending := true // default is ascending
		if direction != "" {
			switch strings.ToLower(direction) {
			case "asc", "ascending":
				ascending = true
			case "desc", "descending":
				ascending = false
			default:
				return nil, fmt.Errorf("invalid order direction '%s' for field '%s', expected 'asc' or 'desc'", direction, fieldSpec)
			}
		}

		// Parse field spec to extract field name, field ID, JSON path, and requery info
		fieldName, fieldID, jsonPath, outputFieldName, isDynamic, err := parseOrderByFieldSpec(fieldSpec, fieldSchemaMap, dynamicField, schema)
		if err != nil {
			return nil, err
		}

		orderByFields = append(orderByFields, OrderByField{
			FieldName:       fieldName,
			FieldID:         fieldID,
			JSONPath:        jsonPath,
			Ascending:       ascending,
			OutputFieldName: outputFieldName,
			IsDynamicField:  isDynamic,
		})
	}

	return orderByFields, nil
}

// splitOrderByFieldAndDirection splits "fieldSpec:direction" handling brackets in fieldSpec
// e.g., "metadata[\"price\"]:asc" -> ("metadata[\"price\"]", "asc")
// e.g., "name:desc" -> ("name", "desc")
// e.g., "name" -> ("name", "")
//
// Limitation: This simple bracket-depth tracking does not handle:
//   - Brackets inside quoted strings: metadata["key]value"] would incorrectly parse
//   - Escaped quotes inside strings: metadata["key\"with\"quotes"] may misbehave
//
// These edge cases are rare in practice. Field names containing unbalanced brackets
// or complex escape sequences are not supported.
func splitOrderByFieldAndDirection(pair string) (fieldSpec, direction string) {
	// Find the last colon that's not inside brackets
	bracketDepth := 0
	lastColonIdx := -1
	for i, ch := range pair {
		switch ch {
		case '[':
			bracketDepth++
		case ']':
			bracketDepth--
		case ':':
			if bracketDepth == 0 {
				lastColonIdx = i
			}
		}
	}

	if lastColonIdx == -1 {
		return strings.TrimSpace(pair), ""
	}
	return strings.TrimSpace(pair[:lastColonIdx]), strings.TrimSpace(pair[lastColonIdx+1:])
}

// parseOrderByFieldSpec parses a field specification and returns field name, ID, JSON path, and requery info
// Handles: regular fields, JSON fields with paths, and dynamic fields
// Returns:
//   - fieldName: top-level field name ($meta for dynamic, actual name for others)
//   - fieldID: field ID
//   - jsonPath: JSON Pointer format path (empty for non-JSON fields)
//   - outputFieldName: field name to use in requery (original key for dynamic fields)
//   - isDynamicField: true if this uses dynamic field extraction at QueryNode
func parseOrderByFieldSpec(fieldSpec string, fieldSchemaMap map[string]*schemapb.FieldSchema, dynamicField *schemapb.FieldSchema, schema *schemapb.CollectionSchema) (fieldName string, fieldID int64, jsonPath string, outputFieldName string, isDynamicField bool, err error) {
	// Check for JSON path syntax (brackets)
	hasBrackets := strings.Contains(fieldSpec, "[") && strings.Contains(fieldSpec, "]")

	if hasBrackets {
		// Extract base field name (part before the first '[')
		baseName := strings.Split(fieldSpec, "[")[0]
		field, exists := fieldSchemaMap[baseName]

		if exists {
			// Field exists in schema
			if field.GetIsDynamic() {
				// This is $meta["key"] - explicit dynamic field access
				// Use QueryNode-level extraction for dynamic fields
				fieldName = common.MetaFieldName
				fieldID = field.GetFieldID()
				jsonPath, err = typeutil2.ParseAndVerifyNestedPath(fieldSpec, schema, fieldID)
				if err != nil {
					return "", 0, "", "", false, fmt.Errorf("invalid JSON path in order_by field '%s': %w", fieldSpec, err)
				}
				outputFieldName = fieldSpec // Explicit $meta["key"] path; single-level, parser accepts it
				isDynamicField = true
			} else if typeutil.IsJSONType(field.GetDataType()) {
				// Regular JSON field with path: metadata["price"]
				// Regular JSON fields don't support QueryNode-level extraction
				fieldName = baseName
				fieldID = field.GetFieldID()
				jsonPath, err = typeutil2.ParseAndVerifyNestedPath(fieldSpec, schema, fieldID)
				if err != nil {
					return "", 0, "", "", false, fmt.Errorf("invalid JSON path in order_by field '%s': %w", fieldSpec, err)
				}
				outputFieldName = baseName // Request the whole JSON field
				isDynamicField = false
			} else {
				// Non-JSON field with brackets - not supported
				return "", 0, "", "", false, fmt.Errorf("order_by field '%s' has brackets but is not a JSON type", fieldSpec)
			}
		} else if dynamicField != nil {
			// Unknown field name with brackets, treat as dynamic field path
			// e.g., unknown["key"] -> $meta with path /unknown/key
			fieldName = common.MetaFieldName
			fieldID = dynamicField.GetFieldID()
			jsonPath, err = typeutil2.ParseAndVerifyNestedPath(fieldSpec, schema, fieldID)
			if err != nil {
				return "", 0, "", "", false, fmt.Errorf("invalid JSON path in order_by field '%s': %w", fieldSpec, err)
			}
			// Request the base dynamic field; full path is in jsonPath
			outputFieldName = baseName
			isDynamicField = true
		} else {
			return "", 0, "", "", false, fmt.Errorf("order_by field '%s' not found in schema and no dynamic field available", baseName)
		}
	} else {
		// No brackets - regular field name or dynamic field key
		field, exists := fieldSchemaMap[fieldSpec]
		if exists {
			// Regular field
			fieldName = fieldSpec
			fieldID = field.GetFieldID()
			outputFieldName = fieldSpec
			isDynamicField = false
			// Validate sortable type
			if !isSortableFieldType(field.GetDataType()) {
				return "", 0, "", "", false, fmt.Errorf("order_by field '%s' has unsortable type %s; supported types: bool, int8/16/32/64, float, double, string, varchar; for JSON fields use path syntax like field[\"key\"]",
					fieldSpec, field.GetDataType().String())
			}
		} else if dynamicField != nil {
			// Treat as dynamic field key: age -> $meta["age"]
			fieldName = common.MetaFieldName
			fieldID = dynamicField.GetFieldID()
			jsonPath, err = typeutil2.ParseAndVerifyNestedPath(fieldSpec, schema, fieldID)
			if err != nil {
				return "", 0, "", "", false, fmt.Errorf("invalid dynamic field key '%s': %w", fieldSpec, err)
			}
			// For dynamic fields, pass the original key so translateOutputFields can extract it
			outputFieldName = fieldSpec
			isDynamicField = true
		} else {
			return "", 0, "", "", false, fmt.Errorf("order_by field '%s' does not exist in collection schema", fieldSpec)
		}
	}

	return fieldName, fieldID, jsonPath, outputFieldName, isDynamicField, nil
}

// isSortableFieldType returns true if the data type can be used for order_by
// Note: JSON type is not directly sortable. Use JSON path syntax (e.g., metadata["price"])
// to sort by specific JSON subfields, or use dynamic fields for schema-less data.
func isSortableFieldType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_String,
		schemapb.DataType_VarChar:
		return true
	default:
		// Vectors, Arrays, JSON (without path), etc. are not sortable
		return false
	}
}

func parseSearchIteratorV2Info(searchParamsPair []*commonpb.KeyValuePair, groupByFieldId int64, isIterator bool, offset int64, queryTopK *int64, largeTopKEnabled bool) (*planpb.SearchIteratorV2Info, error) {
	isIteratorV2Str, _ := funcutil.GetAttrByKeyFromRepeatedKV(SearchIterV2Key, searchParamsPair)
	isIteratorV2, _ := strconv.ParseBool(isIteratorV2Str)
	if !isIteratorV2 {
		return nil, nil
	}

	// iteratorV1 and iteratorV2 should be set together for compatibility
	if !isIterator {
		return nil, fmt.Errorf("both %s and %s must be set in the SDK", IteratorField, SearchIterV2Key)
	}

	// disable groupBy when doing iteratorV2
	// same behavior with V1
	if isIteratorV2 && groupByFieldId > 0 {
		return nil, merr.WrapErrParameterInvalid("", "",
			"GroupBy is not permitted when using a search iterator")
	}

	// disable offset when doing iteratorV2
	if isIteratorV2 && offset > 0 {
		return nil, merr.WrapErrParameterInvalid("", "",
			"Setting an offset is not permitted when using a search iterator v2")
	}

	// parse token, generate if not exist
	token, _ := funcutil.GetAttrByKeyFromRepeatedKV(SearchIterIdKey, searchParamsPair)
	if token == "" {
		generatedToken, err := uuid.NewRandom()
		if err != nil {
			return nil, err
		}
		token = generatedToken.String()
	} else {
		// Validate existing token is a valid UUID
		if _, err := uuid.Parse(token); err != nil {
			return nil, errors.New("invalid token format")
		}
	}

	// parse batch size, required non-zero value
	batchSizeStr, _ := funcutil.GetAttrByKeyFromRepeatedKV(SearchIterBatchSizeKey, searchParamsPair)
	if batchSizeStr == "" {
		return nil, errors.New("batch size is required")
	}
	batchSize, err := strconv.ParseInt(batchSizeStr, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("batch size is invalid, %w", err)
	}
	// use the same validation logic as topk
	if err := validateLimit(batchSize, largeTopKEnabled); err != nil {
		return nil, fmt.Errorf("batch size is invalid, %w", err)
	}
	*queryTopK = batchSize // for compatibility

	// prepare plan iterator v2 info proto
	planIteratorV2Info := &planpb.SearchIteratorV2Info{
		Token:     token,
		BatchSize: uint32(batchSize),
	}

	// append optional last bound if applicable
	lastBoundStr, _ := funcutil.GetAttrByKeyFromRepeatedKV(SearchIterLastBoundKey, searchParamsPair)
	if lastBoundStr != "" {
		lastBound, err := strconv.ParseFloat(lastBoundStr, 32)
		if err != nil {
			return nil, fmt.Errorf("failed to parse input last bound, %w", err)
		}
		lastBoundFloat32 := float32(lastBound)
		planIteratorV2Info.LastBound = &lastBoundFloat32 // escape pointer
	}

	return planIteratorV2Info, nil
}

// parseSearchInfo returns QueryInfo and offset
func parseSearchInfo(searchParamsPair []*commonpb.KeyValuePair, schema *schemapb.CollectionSchema, rankParams *rankParams, largeTopKEnabled bool) (*SearchInfo, error) {
	var topK int64
	isAdvanced := rankParams != nil
	externalLimit := rankParams.GetLimit() + rankParams.GetOffset()
	topKStr, err := funcutil.GetAttrByKeyFromRepeatedKV(TopKKey, searchParamsPair)
	if err != nil {
		if externalLimit <= 0 {
			return nil, fmt.Errorf("%s is required", TopKKey)
		}
		topK = externalLimit
	} else {
		topKInParam, err := strconv.ParseInt(topKStr, 0, 64)
		if err != nil {
			if externalLimit <= 0 {
				return nil, fmt.Errorf("%s [%s] is invalid", TopKKey, topKStr)
			}
			topK = externalLimit
		} else {
			topK = topKInParam
		}
	}

	isIteratorStr, _ := funcutil.GetAttrByKeyFromRepeatedKV(IteratorField, searchParamsPair)
	isIterator := (isIteratorStr == "True") || (isIteratorStr == "true")

	collectionIDStr, _ := funcutil.GetAttrByKeyFromRepeatedKV(CollectionID, searchParamsPair)
	collectionId, _ := strconv.ParseInt(collectionIDStr, 0, 64)

	if err := validateLimit(topK, largeTopKEnabled); err != nil {
		if isIterator {
			// 1. if the request is from iterator, we set topK to QuotaLimit as the iterator can resolve too large topK problem
			// 2. GetAsInt64 has cached inside, no need to worry about cpu cost for parsing here
			if largeTopKEnabled {
				topK = Params.QuotaConfig.LargeTopKLimit.GetAsInt64()
			} else {
				topK = Params.QuotaConfig.TopKLimit.GetAsInt64()
			}
		} else {
			return nil, fmt.Errorf("%s [%d] is invalid, %w", TopKKey, topK, err)
		}
	}

	var offset int64
	// ignore offset if isAdvanced
	if !isAdvanced {
		offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, searchParamsPair)
		if err == nil {
			offset, err = strconv.ParseInt(offsetStr, 0, 64)
			if err != nil {
				return nil, fmt.Errorf("%s [%s] is invalid", OffsetKey, offsetStr)
			}

			if offset != 0 {
				if err := validateLimit(offset, largeTopKEnabled); err != nil {
					return nil, fmt.Errorf("%s [%d] is invalid, %w", OffsetKey, offset, err)
				}
			}
		}
	}

	queryTopK := topK + offset
	if err := validateLimit(queryTopK, largeTopKEnabled); err != nil {
		return nil, fmt.Errorf("%s+%s [%d] is invalid, %w", OffsetKey, TopKKey, queryTopK, err)
	}

	// 2. parse metrics type
	metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.MetricTypeKey, searchParamsPair)
	if err != nil {
		metricType = ""
	}

	// 3. parse round decimal
	roundDecimalStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RoundDecimalKey, searchParamsPair)
	if err != nil {
		roundDecimalStr = "-1"
	}

	hints, err := funcutil.GetAttrByKeyFromRepeatedKV(common.HintsKey, searchParamsPair)
	if err != nil {
		hints = ""
	}

	roundDecimal, err := strconv.ParseInt(roundDecimalStr, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	if roundDecimal != -1 && (roundDecimal > 6 || roundDecimal < 0) {
		return nil, fmt.Errorf("%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	// 4. parse search param str
	searchParamStr, err := funcutil.GetAttrByKeyFromRepeatedKV(ParamsKey, searchParamsPair)
	if err != nil {
		searchParamStr = ""
	}

	// 5. parse group by field and group by size
	var groupByFieldId, groupSize int64
	var groupByFieldIds []int64
	var strictGroupSize bool
	var jsonPath string
	var jsonType schemapb.DataType
	var strictCast bool
	var isRangeSearch bool
	var isIterativeFilter bool
	if isAdvanced {
		groupByFieldId, groupByFieldIds, groupSize, strictGroupSize = rankParams.GetGroupByFieldId(), rankParams.GetGroupByFieldIds(), rankParams.GetGroupSize(), rankParams.GetStrictGroupSize()
	} else {
		groupByInfo, err := parseGroupByInfo(searchParamsPair, schema)
		if err != nil {
			return nil, err
		}
		groupByFieldId, groupByFieldIds, groupSize, strictGroupSize = groupByInfo.GetGroupByFieldId(), groupByInfo.GetGroupByFieldIds(), groupByInfo.GetGroupSize(), groupByInfo.GetStrictGroupSize()
		jsonPath, jsonType, strictCast = groupByInfo.GetJSONPath(), groupByInfo.GetJSONType(), groupByInfo.GetStrictCast()
		if jsonPath != "" {
			jsonPath, err = typeutil2.ParseAndVerifyNestedPath(jsonPath, schema, groupByFieldId)
			if err != nil {
				return nil, err
			}
		}
	}

	// 6. parse iterator tag, prevent trying to groupBy when doing iteration or doing range-search
	if isIterator && groupByFieldId > 0 {
		return nil, merr.WrapErrParameterInvalid("", "",
			"Not allowed to do groupBy when doing iteration")
	}

	isRangeSearch = gjson.Get(searchParamStr, radiusKey).Exists()
	isIterativeFilter = (hints == iterativeFilterKey) || strings.Contains(searchParamStr, iterativeFilterKey)
	if !isRangeSearch && gjson.Get(searchParamStr, rangeFilterKey).Exists() {
		return nil, merr.WrapErrParameterInvalid("range_filter", "",
			"range_filter requires radius to be set; range_filter alone is not a valid range search parameter")
	}
	if isRangeSearch && groupByFieldId > 0 {
		return nil, merr.WrapErrParameterInvalid("", "",
			"Not allowed to do range-search when doing search-group-by")
	}

	planSearchIteratorV2Info, err := parseSearchIteratorV2Info(searchParamsPair, groupByFieldId, isIterator, offset, &queryTopK, largeTopKEnabled)
	if err != nil {
		return nil, fmt.Errorf("parse iterator v2 info failed: %w", err)
	}

	// 7. parse order_by_fields
	orderByFields, err := parseOrderByFields(searchParamsPair, schema)
	if err != nil {
		return nil, err
	}

	// 8. validate iterator + order_by combination is not allowed
	if isIterator && len(orderByFields) > 0 {
		return nil, merr.WrapErrParameterInvalid("", "",
			"order_by is not supported when using search iterator")
	}

	return &SearchInfo{
		planInfo: &planpb.QueryInfo{
			Topk:                 queryTopK,
			MetricType:           metricType,
			SearchParams:         searchParamStr,
			RoundDecimal:         roundDecimal,
			GroupByFieldId:       groupByFieldId,
			GroupByFieldIds:      groupByFieldIds,
			GroupSize:            groupSize,
			StrictGroupSize:      strictGroupSize,
			Hints:                hints,
			SearchIteratorV2Info: planSearchIteratorV2Info,
			JsonPath:             jsonPath,
			JsonType:             jsonType,
			StrictCast:           strictCast,
		},
		offset:          offset,
		isIterator:      isIterator,
		collectionID:    collectionId,
		orderByFields:   orderByFields,
		iterativeFilter: isIterativeFilter,
	}, nil
}

func getOutputFieldIDs(schema *schemaInfo, outputFields []string) (outputFieldIDs []UniqueID, err error) {
	outputFieldIDs = make([]UniqueID, 0, len(outputFields))
	for _, name := range outputFields {
		id, ok := schema.MapFieldID(name)
		if !ok {
			return nil, fmt.Errorf("field %s not exist", name)
		}
		outputFieldIDs = append(outputFieldIDs, id)
	}
	return outputFieldIDs, nil
}

func getNqFromSubSearch(req *milvuspb.SubSearchRequest) (int64, error) {
	if req.GetNq() == 0 {
		// keep compatible with older client version.
		x := &commonpb.PlaceholderGroup{}
		err := proto.Unmarshal(req.GetPlaceholderGroup(), x)
		if err != nil {
			return 0, err
		}
		total := int64(0)
		for _, h := range x.GetPlaceholders() {
			total += int64(len(h.Values))
		}
		return total, nil
	}
	return req.GetNq(), nil
}

func getNq(req *milvuspb.SearchRequest) (int64, error) {
	if req.GetNq() == 0 {
		// keep compatible with older client version.
		x := &commonpb.PlaceholderGroup{}
		err := proto.Unmarshal(req.GetPlaceholderGroup(), x)
		if err != nil {
			return 0, err
		}
		total := int64(0)
		for _, h := range x.GetPlaceholders() {
			total += int64(len(h.Values))
		}
		return total, nil
	}
	return req.GetNq(), nil
}

func getPartitionIDs(ctx context.Context, dbName string, collectionName string, partitionNames []string) (partitionIDs []UniqueID, err error) {
	for _, tag := range partitionNames {
		if err := validatePartitionTag(tag, false); err != nil {
			return nil, err
		}
	}

	partitionsMap, err := globalMetaCache.GetPartitions(ctx, dbName, collectionName)
	if err != nil {
		return nil, err
	}

	useRegexp := Params.ProxyCfg.PartitionNameRegexp.GetAsBool()

	partitionsSet := typeutil.NewUniqueSet()
	for _, partitionName := range partitionNames {
		if useRegexp {
			// Legacy feature, use partition name as regexp
			pattern := fmt.Sprintf("^%s$", partitionName)
			re, err := regexp.Compile(pattern)
			if err != nil {
				return nil, fmt.Errorf("invalid partition: %s", partitionName)
			}
			var found bool
			for name, pID := range partitionsMap {
				if re.MatchString(name) {
					partitionsSet.Insert(pID)
					found = true
				}
			}
			if !found {
				return nil, fmt.Errorf("partition name %s not found", partitionName)
			}
		} else {
			partitionID, found := partitionsMap[partitionName]
			if !found {
				// TODO change after testcase updated: return nil, merr.WrapErrPartitionNotFound(partitionName)
				return nil, fmt.Errorf("partition name %s not found", partitionName)
			}
			partitionsSet.Insert(partitionID)
		}
	}
	return partitionsSet.Collect(), nil
}

type groupByInfo struct {
	groupByFieldIds   []int64
	groupByFieldNames []string
	groupSize         int64
	strictGroupSize   bool
	jsonPath          string
	jsonType          schemapb.DataType
	strictCast        bool
}

// GetGroupByFieldId returns the first group-by field id, or -1 when none is set.
// The -1 sentinel is required by plan_parser_v2 (segment-scorer vs group_by
// mutex check). Kept aligned with rankParams.GetGroupByFieldId().
func (g *groupByInfo) GetGroupByFieldId() int64 {
	if g != nil && len(g.groupByFieldIds) > 0 {
		return g.groupByFieldIds[0]
	}
	return -1
}

// GetGroupByFieldName returns the first group-by field name, or "" when none is set.
func (g *groupByInfo) GetGroupByFieldName() string {
	if g != nil && len(g.groupByFieldNames) > 0 {
		return g.groupByFieldNames[0]
	}
	return ""
}

func (g *groupByInfo) GetGroupByFieldIds() []int64 {
	if g != nil {
		return g.groupByFieldIds
	}
	return nil
}

func (g *groupByInfo) GetGroupByFieldNames() []string {
	if g != nil {
		return g.groupByFieldNames
	}
	return nil
}

func (g *groupByInfo) GetGroupSize() int64 {
	if g != nil {
		return g.groupSize
	}
	return 0
}

func (g *groupByInfo) GetStrictGroupSize() bool {
	if g != nil {
		return g.strictGroupSize
	}
	return false
}

func (g *groupByInfo) GetJSONPath() string {
	if g != nil {
		return g.jsonPath
	}
	return ""
}

func (g *groupByInfo) GetJSONType() schemapb.DataType {
	if g != nil {
		return g.jsonType
	}
	return schemapb.DataType_None
}

func (g *groupByInfo) GetStrictCast() bool {
	if g != nil {
		return g.strictCast
	}
	return false
}

// parseGroupByField parses the groupByFieldName and returns groupByFieldId and jsonPath.
// It handles the following cases:
// 1. Field exists in schema: use the field's ID, set jsonPath if it's a JSON field with brackets
// 2. Field doesn't exist but dynamic field exists: use dynamic field's ID and set jsonPath
// 3. Field doesn't exist and no dynamic field: return error
func parseGroupByField(groupByFieldName string, schema *schemapb.CollectionSchema) (groupByFieldId int64, jsonPath string, err error) {
	if groupByFieldName == "" {
		return -1, "", nil
	}

	// Build field name to field map to avoid repeated loops
	fields := schema.GetFields()
	fieldNameMap := make(map[string]*schemapb.FieldSchema, len(fields))
	var dynamicField *schemapb.FieldSchema
	for _, field := range fields {
		fieldNameMap[field.Name] = field
		if field.GetIsDynamic() {
			dynamicField = field
		}
	}

	// Check if groupByFieldName matches JSON field access pattern (e.g., metadata["product_info"])
	// Pattern: fieldName["key"] or fieldName["key1"]["key2"]...
	hasBrackets := strings.Contains(groupByFieldName, "[") && strings.Contains(groupByFieldName, "]")

	if hasBrackets {
		// Extract field name (part before the first '[')
		fieldName := strings.Split(groupByFieldName, "[")[0]
		if field, exists := fieldNameMap[fieldName]; exists {
			// Field exists in schema
			groupByFieldId = field.FieldID
			// If the field is JSON type, set jsonPath to the full groupByFieldName
			if typeutil.IsJSONType(field.DataType) {
				// Case 2.1: groupByField is JSON column + brackets pattern
				// Set jsonPath to the full groupByFieldName (e.g., metadata["product_info"])
				jsonPath = groupByFieldName
			}
			// If field exists but is not JSON type, still use the field but don't set jsonPath
		} else {
			// Field name doesn't exist in schema
			if dynamicField != nil {
				// Case 2.2: Use dynamic field
				groupByFieldId = dynamicField.FieldID
				jsonPath = groupByFieldName
			} else {
				// Case 2.3: Field not found and no dynamic field
				return -1, "", merr.WrapErrFieldNotFound(groupByFieldName, "groupBy field not found in schema")
			}
		}
	} else {
		// Case 1: Regular field name (no brackets)
		if field, exists := fieldNameMap[groupByFieldName]; exists {
			groupByFieldId = field.FieldID
		} else {
			// Field not found
			if dynamicField != nil {
				// Case 2.2: Use dynamic field
				groupByFieldId = dynamicField.FieldID
				jsonPath = groupByFieldName
			} else {
				// Case 2.3: Field not found and no dynamic field
				return -1, "", merr.WrapErrFieldNotFound(groupByFieldName, "groupBy field not found in schema")
			}
		}
	}

	return groupByFieldId, jsonPath, nil
}

func parseGroupByInfo(searchParamsPair []*commonpb.KeyValuePair, schema *schemapb.CollectionSchema) (*groupByInfo, error) {
	ret := &groupByInfo{}

	// 1. parse group-by field name(s).
	// `group_by_field` (singular, legacy SDK) wins over `group_by_fields` (plural, new SDK).
	// When both are set the plural list is silently ignored to preserve old-client behavior.
	var groupByFieldNames []string
	if legacy, err := funcutil.GetAttrByKeyFromRepeatedKV(GroupByFieldKey, searchParamsPair); err == nil {
		if trimmed := strings.TrimSpace(legacy); trimmed != "" {
			groupByFieldNames = []string{trimmed}
		}
	}
	if len(groupByFieldNames) == 0 {
		if plural, err := funcutil.GetAttrByKeyFromRepeatedKV(GroupByFieldsKey, searchParamsPair); err == nil {
			for _, f := range strings.Split(plural, ",") {
				if trimmed := strings.TrimSpace(f); trimmed != "" {
					groupByFieldNames = append(groupByFieldNames, trimmed)
				}
			}
		}
	}

	// Resolve each name to fieldId (and optional jsonPath).
	// Multi-field + jsonPath is rejected because this layer carries a single jsonPath.
	for _, name := range groupByFieldNames {
		fieldId, jsonPath, err := parseGroupByField(name, schema)
		if err != nil {
			return nil, err
		}
		ret.groupByFieldIds = append(ret.groupByFieldIds, fieldId)
		ret.groupByFieldNames = append(ret.groupByFieldNames, name)
		if jsonPath != "" {
			if len(groupByFieldNames) > 1 {
				return nil, merr.WrapErrParameterInvalidMsg(
					fmt.Sprintf("group_by with json path is not supported for multi-field group_by, field:%s", name))
			}
			ret.jsonPath = jsonPath
		}
	}

	// 2. parse group size
	var groupSize int64
	groupSizeStr, err := funcutil.GetAttrByKeyFromRepeatedKV(GroupSizeKey, searchParamsPair)
	if err != nil {
		groupSize = 1
	} else {
		groupSize, err = strconv.ParseInt(groupSizeStr, 0, 64)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("failed to parse input group size:%s", groupSizeStr))
		}
		if groupSize <= 0 {
			return nil, merr.WrapErrParameterInvalidMsg(
				fmt.Sprintf("input group size:%d is negative, failed to do search_groupby", groupSize))
		}
	}
	if groupSize > Params.QuotaConfig.MaxGroupSize.GetAsInt64() {
		return nil, merr.WrapErrParameterInvalidMsg(
			fmt.Sprintf("input group size:%d exceeds configured max group size:%d", groupSize, Params.QuotaConfig.MaxGroupSize.GetAsInt64()))
	}
	ret.groupSize = groupSize

	// 3.  parse group strict size
	var strictGroupSize bool
	strictGroupSizeStr, err := funcutil.GetAttrByKeyFromRepeatedKV(StrictGroupSize, searchParamsPair)
	if err != nil {
		strictGroupSize = false
	} else {
		strictGroupSize, err = strconv.ParseBool(strictGroupSizeStr)
		if err != nil {
			strictGroupSize = false
		}
	}
	ret.strictGroupSize = strictGroupSize

	// 4. parse json path
	// If jsonPath was already set from groupByFieldName parsing, it will be overridden if explicitly provided
	explicitJSONPath, err := funcutil.GetAttrByKeyFromRepeatedKV(JSONPath, searchParamsPair)
	if err == nil {
		ret.jsonPath = explicitJSONPath
	}

	// 5. parse json type
	jsonTypeStr, err := funcutil.GetAttrByKeyFromRepeatedKV(JSONType, searchParamsPair)
	if err == nil {
		dataTypeVal, ok := schemapb.DataType_value[jsonTypeStr]
		if ok {
			ret.jsonType = schemapb.DataType(dataTypeVal)
		}
	}

	// 6. parse strict cast
	strictCastStr, err := funcutil.GetAttrByKeyFromRepeatedKV(StrictCastKey, searchParamsPair)
	if err == nil {
		strictCast, err := strconv.ParseBool(strictCastStr)
		if err != nil {
			strictCast = false
		}
		ret.strictCast = strictCast
	}

	return ret, nil
}

// parseRankParams get limit and offset from rankParams, both are optional.
func parseRankParams(rankParamsPair []*commonpb.KeyValuePair, schema *schemapb.CollectionSchema, largeTopKEnabled bool) (*rankParams, error) {
	var (
		limit        int64
		offset       int64
		roundDecimal int64
		err          error
	)

	limitStr, err := funcutil.GetAttrByKeyFromRepeatedKV(LimitKey, rankParamsPair)
	if err != nil {
		return nil, errors.New(LimitKey + " not found in rank_params")
	}
	limit, err = strconv.ParseInt(limitStr, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("%s [%s] is invalid", LimitKey, limitStr)
	}

	offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, rankParamsPair)
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

	roundDecimalStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RoundDecimalKey, rankParamsPair)
	if err != nil {
		roundDecimalStr = "-1"
	}

	roundDecimal, err = strconv.ParseInt(roundDecimalStr, 0, 64)
	if err != nil {
		return nil, fmt.Errorf("%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	if roundDecimal != -1 && (roundDecimal > 6 || roundDecimal < 0) {
		return nil, fmt.Errorf("%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	// parse group_by parameters from main request body for hybrid search
	groupByInfo, err := parseGroupByInfo(rankParamsPair, schema)
	if err != nil {
		return nil, err
	}

	return &rankParams{
		limit:             limit,
		offset:            offset,
		roundDecimal:      roundDecimal,
		groupByFieldIds:   groupByInfo.GetGroupByFieldIds(),
		groupByFieldNames: groupByInfo.GetGroupByFieldNames(),
		groupSize:         groupByInfo.GetGroupSize(),
		strictGroupSize:   groupByInfo.GetStrictGroupSize(),
	}, nil
}

func parseTimeFields(params []*commonpb.KeyValuePair) []string {
	timeFields, err := funcutil.GetAttrByKeyFromRepeatedKV(TimefieldsKey, params)
	if err != nil {
		return nil
	}
	return strings.FieldsFunc(timeFields, func(r rune) bool {
		return r == ',' || r == ' '
	})
}

func getGroupScorerStr(params []*commonpb.KeyValuePair) string {
	groupScorerStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RankGroupScorer, params)
	if err != nil {
		groupScorerStr = MaxScorer
	}
	return groupScorerStr
}

func convertHybridSearchToSearch(req *milvuspb.HybridSearchRequest) *milvuspb.SearchRequest {
	ret := &milvuspb.SearchRequest{
		Base:                  req.GetBase(),
		DbName:                req.GetDbName(),
		CollectionName:        req.GetCollectionName(),
		PartitionNames:        req.GetPartitionNames(),
		OutputFields:          req.GetOutputFields(),
		SearchParams:          req.GetRankParams(),
		TravelTimestamp:       req.GetTravelTimestamp(),
		GuaranteeTimestamp:    req.GetGuaranteeTimestamp(),
		Nq:                    0,
		NotReturnAllMeta:      req.GetNotReturnAllMeta(),
		ConsistencyLevel:      req.GetConsistencyLevel(),
		UseDefaultConsistency: req.GetUseDefaultConsistency(),
		SearchByPrimaryKeys:   false,
		SubReqs:               nil,
		FunctionScore:         req.FunctionScore,
	}

	for _, sub := range req.GetRequests() {
		subReq := &milvuspb.SubSearchRequest{
			Dsl:                sub.GetDsl(),
			PlaceholderGroup:   sub.GetPlaceholderGroup(),
			DslType:            sub.GetDslType(),
			SearchParams:       sub.GetSearchParams(),
			Nq:                 sub.GetNq(),
			ExprTemplateValues: sub.GetExprTemplateValues(),
		}
		ret.SubReqs = append(ret.SubReqs, subReq)
	}
	return ret
}

func getMetricType(toReduceResults []*internalpb.SearchResults) string {
	for _, r := range toReduceResults {
		if m := r.GetMetricType(); m != "" {
			return m
		}
	}
	return ""
}
