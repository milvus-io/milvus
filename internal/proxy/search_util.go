package proxy

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type rankParams struct {
	limit           int64
	offset          int64
	roundDecimal    int64
	groupByFieldId  int64
	groupSize       int64
	strictGroupSize bool
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

func (r *rankParams) GetGroupByFieldId() int64 {
	if r != nil {
		return r.groupByFieldId
	}
	return -1
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

type SearchInfo struct {
	planInfo     *planpb.QueryInfo
	offset       int64
	isIterator   bool
	collectionID int64
}

func parseSearchIteratorV2Info(searchParamsPair []*commonpb.KeyValuePair, groupByFieldId int64, isIterator bool, offset int64, queryTopK *int64) (*planpb.SearchIteratorV2Info, error) {
	isIteratorV2Str, _ := funcutil.GetAttrByKeyFromRepeatedKV(SearchIterV2Key, searchParamsPair)
	isIteratorV2, _ := strconv.ParseBool(isIteratorV2Str)
	if !isIteratorV2 {
		return nil, nil
	}

	// iteratorV1 and iteratorV2 should be set together for compatibility
	if !isIterator {
		return nil, merr.WrapErrParameterMissingMsg("both %s and %s must be set in the SDK", IteratorField, SearchIterV2Key)
	}

	// disable groupBy when doing iteratorV2
	// same behavior with V1
	if isIteratorV2 && groupByFieldId > 0 {
		return nil, merr.WrapErrParameterInvalidMsg("GroupBy is not permitted when using a search iterator")
	}

	// disable offset when doing iteratorV2
	if isIteratorV2 && offset > 0 {
		return nil, merr.WrapErrParameterInvalidMsg("Setting an offset is not permitted when using a search iterator v2")
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
			return nil, merr.WrapErrParameterInvalidMsg("invalid token format")
		}
	}

	// parse batch size, required non-zero value
	batchSizeStr, _ := funcutil.GetAttrByKeyFromRepeatedKV(SearchIterBatchSizeKey, searchParamsPair)
	if batchSizeStr == "" {
		return nil, merr.WrapErrParameterInvalidMsg("batch size is required")
	}
	batchSize, err := strconv.ParseInt(batchSizeStr, 0, 64)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "batch size is invalid")
	}
	// use the same validation logic as topk
	if err := validateLimit(batchSize); err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "batch size is invalid")
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
			return nil, merr.WrapErrParameterInvalidErr(err, "failed to parse input last bound")
		}
		lastBoundFloat32 := float32(lastBound)
		planIteratorV2Info.LastBound = &lastBoundFloat32 // escape pointer
	}

	return planIteratorV2Info, nil
}

// parseSearchInfo returns QueryInfo and offset
func parseSearchInfo(searchParamsPair []*commonpb.KeyValuePair, schema *schemapb.CollectionSchema, rankParams *rankParams) (*SearchInfo, error) {
	var topK int64
	isAdvanced := rankParams != nil
	externalLimit := rankParams.GetLimit() + rankParams.GetOffset()
	topKStr, err := funcutil.GetAttrByKeyFromRepeatedKV(TopKKey, searchParamsPair)
	if err != nil {
		if externalLimit <= 0 {
			return nil, merr.WrapErrParameterMissingMsg("%s is required", TopKKey)
		}
		topK = externalLimit
	} else {
		topKInParam, err := strconv.ParseInt(topKStr, 0, 64)
		if err != nil {
			if externalLimit <= 0 {
				return nil, merr.WrapErrParameterInvalidMsg("%s [%s] is invalid", TopKKey, topKStr)
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

	if err := validateLimit(topK); err != nil {
		if isIterator {
			// 1. if the request is from iterator, we set topK to QuotaLimit as the iterator can resolve too large topK problem
			// 2. GetAsInt64 has cached inside, no need to worry about cpu cost for parsing here
			topK = Params.QuotaConfig.TopKLimit.GetAsInt64()
		} else {
			return nil, merr.WrapErrParameterInvalidErr(err, "%s [%d] is invalid", TopKKey, topK)
		}
	}

	var offset int64
	// ignore offset if isAdvanced
	if !isAdvanced {
		offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, searchParamsPair)
		if err == nil {
			offset, err = strconv.ParseInt(offsetStr, 0, 64)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidErr(err, "%s [%s] is invalid", OffsetKey, offsetStr)
			}

			if offset != 0 {
				if err := validateLimit(offset); err != nil {
					return nil, merr.WrapErrParameterInvalidErr(err, "%s [%d] is invalid", OffsetKey, offset)
				}
			}
		}
	}

	queryTopK := topK + offset
	if err := validateLimit(queryTopK); err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "%s+%s [%d] is invalid", OffsetKey, TopKKey, queryTopK)
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
		return nil, merr.WrapErrParameterInvalidErr(err, "%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	if roundDecimal != -1 && (roundDecimal > 6 || roundDecimal < 0) {
		return nil, merr.WrapErrParameterInvalidErr(err, "%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	// 4. parse search param str
	searchParamStr, err := funcutil.GetAttrByKeyFromRepeatedKV(ParamsKey, searchParamsPair)
	if err != nil {
		searchParamStr = ""
	}

	// 5. parse group by field and group by size
	var groupByFieldId, groupSize int64
	var strictGroupSize bool
	var jsonPath string
	var jsonType schemapb.DataType
	var strictCast bool
	if isAdvanced {
		groupByFieldId, groupSize, strictGroupSize = rankParams.GetGroupByFieldId(), rankParams.GetGroupSize(), rankParams.GetStrictGroupSize()
	} else {
		groupByInfo, err := parseGroupByInfo(searchParamsPair, schema)
		if err != nil {
			return nil, err
		}
		groupByFieldId, groupSize, strictGroupSize = groupByInfo.GetGroupByFieldId(), groupByInfo.GetGroupSize(), groupByInfo.GetStrictGroupSize()
		jsonPath, jsonType, strictCast = groupByInfo.GetJSONPath(), groupByInfo.GetJSONType(), groupByInfo.GetStrictCast()
		if jsonPath != "" {
			jsonPath, err = typeutil2.ParseAndVerifyNestedPath(jsonPath, schema, groupByFieldId)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidErr(err, "parse nested path '%s' failed", jsonPath)
			}
		}
	}

	// 6. parse iterator tag, prevent trying to groupBy when doing iteration or doing range-search
	if isIterator && groupByFieldId > 0 {
		return nil, merr.WrapErrParameterInvalid("", "",
			"Not allowed to do groupBy when doing iteration")
	}
	if strings.Contains(searchParamStr, radiusKey) && groupByFieldId > 0 {
		return nil, merr.WrapErrParameterInvalid("", "",
			"Not allowed to do range-search when doing search-group-by")
	}

	planSearchIteratorV2Info, err := parseSearchIteratorV2Info(searchParamsPair, groupByFieldId, isIterator, offset, &queryTopK)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "parse iterator v2 info failed")
	}

	// 7. check search for embedding list
	annsFieldName, _ := funcutil.GetAttrByKeyFromRepeatedKV(AnnsFieldKey, searchParamsPair)
	if annsFieldName != "" {
		annField := typeutil.GetFieldByName(schema, annsFieldName)
		if annField != nil && annField.GetDataType() == schemapb.DataType_ArrayOfVector {
			if strings.Contains(searchParamStr, radiusKey) {
				return nil, merr.WrapErrParameterInvalid("", "",
					"range search is not supported for vector array (embedding list) fields, fieldName:", annsFieldName)
			}

			if groupByFieldId > 0 {
				return nil, merr.WrapErrParameterInvalid("", "",
					"group by search is not supported for vector array (embedding list) fields, fieldName:", annsFieldName)
			}

			if isIterator {
				return nil, merr.WrapErrParameterInvalid("", "",
					"search iterator is not supported for vector array (embedding list) fields, fieldName:", annsFieldName)
			}
		}
	}

	return &SearchInfo{
		planInfo: &planpb.QueryInfo{
			Topk:                 queryTopK,
			MetricType:           metricType,
			SearchParams:         searchParamStr,
			RoundDecimal:         roundDecimal,
			GroupByFieldId:       groupByFieldId,
			GroupSize:            groupSize,
			StrictGroupSize:      strictGroupSize,
			Hints:                hints,
			SearchIteratorV2Info: planSearchIteratorV2Info,
			JsonPath:             jsonPath,
			JsonType:             jsonType,
			StrictCast:           strictCast,
		},
		offset:       offset,
		isIterator:   isIterator,
		collectionID: collectionId,
	}, nil
}

func getOutputFieldIDs(schema *schemaInfo, outputFields []string) (outputFieldIDs []UniqueID, err error) {
	outputFieldIDs = make([]UniqueID, 0, len(outputFields))
	for _, name := range outputFields {
		id, ok := schema.MapFieldID(name)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("Field %s not exist", name)
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
				return nil, merr.WrapErrParameterInvalidErr(err, "invalid partition: %s", partitionName)
			}
			var found bool
			for name, pID := range partitionsMap {
				if re.MatchString(name) {
					partitionsSet.Insert(pID)
					found = true
				}
			}
			if !found {
				return nil, merr.WrapErrParameterInvalidMsg("partition name %s not found", partitionName)
			}
		} else {
			partitionID, found := partitionsMap[partitionName]
			if !found {
				// TODO change after testcase updated: return nil, merr.WrapErrPartitionNotFound(partitionName)
				return nil, merr.WrapErrParameterInvalidMsg("partition name %s not found", partitionName)
			}
			partitionsSet.Insert(partitionID)
		}
	}
	return partitionsSet.Collect(), nil
}

type groupByInfo struct {
	groupByFieldId  int64
	groupSize       int64
	strictGroupSize bool
	jsonPath        string
	jsonType        schemapb.DataType
	strictCast      bool
}

func (g *groupByInfo) GetGroupByFieldId() int64 {
	if g != nil {
		return g.groupByFieldId
	}
	return 0
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

	// 1. parse group_by_field
	groupByFieldName, err := funcutil.GetAttrByKeyFromRepeatedKV(GroupByFieldKey, searchParamsPair)
	if err != nil {
		groupByFieldName = ""
	}
	groupByFieldId, jsonPath, err := parseGroupByField(groupByFieldName, schema)
	if err != nil {
		return nil, err
	}
	ret.groupByFieldId = groupByFieldId
	if jsonPath != "" {
		ret.jsonPath = jsonPath
	}

	// 2. parse group size
	var groupSize int64
	groupSizeStr, err := funcutil.GetAttrByKeyFromRepeatedKV(GroupSizeKey, searchParamsPair)
	if err != nil {
		groupSize = 1
	} else {
		groupSize, err = strconv.ParseInt(groupSizeStr, 0, 64)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("failed to parse input group size:%s", groupSizeStr)
		}
		if groupSize <= 0 {
			return nil, merr.WrapErrParameterInvalidMsg("input group size:%d is negative, failed to do search_groupby", groupSize)
		}
	}
	if groupSize > Params.QuotaConfig.MaxGroupSize.GetAsInt64() {
		return nil, merr.WrapErrParameterInvalidMsg("input group size:%d exceeds configured max group size:%d", groupSize, Params.QuotaConfig.MaxGroupSize.GetAsInt64())
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
func parseRankParams(rankParamsPair []*commonpb.KeyValuePair, schema *schemapb.CollectionSchema) (*rankParams, error) {
	var (
		limit        int64
		offset       int64
		roundDecimal int64
		err          error
	)

	limitStr, err := funcutil.GetAttrByKeyFromRepeatedKV(LimitKey, rankParamsPair)
	if err != nil {
		return nil, merr.WrapErrParameterMissingMsg("%s not found in rank_params", LimitKey)
	}
	limit, err = strconv.ParseInt(limitStr, 0, 64)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("%s [%s] is invalid", LimitKey, limitStr)
	}

	offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, rankParamsPair)
	if err == nil {
		offset, err = strconv.ParseInt(offsetStr, 0, 64)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidErr(err, "%s [%s] is invalid", OffsetKey, offsetStr)
		}
	}

	// validate max result window.
	if err = validateMaxQueryResultWindow(offset, limit); err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "invalid max query result window")
	}

	roundDecimalStr, err := funcutil.GetAttrByKeyFromRepeatedKV(RoundDecimalKey, rankParamsPair)
	if err != nil {
		roundDecimalStr = "-1"
	}

	roundDecimal, err = strconv.ParseInt(roundDecimalStr, 0, 64)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidErr(err, "%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	if roundDecimal != -1 && (roundDecimal > 6 || roundDecimal < 0) {
		return nil, merr.WrapErrParameterInvalidMsg("%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	// parse group_by parameters from main request body for hybrid search
	groupByInfo, err := parseGroupByInfo(rankParamsPair, schema)
	if err != nil {
		return nil, err
	}

	return &rankParams{
		limit:           limit,
		offset:          offset,
		roundDecimal:    roundDecimal,
		groupByFieldId:  groupByInfo.GetGroupByFieldId(),
		groupSize:       groupByInfo.GetGroupSize(),
		strictGroupSize: groupByInfo.GetStrictGroupSize(),
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
