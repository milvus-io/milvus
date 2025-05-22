package proxy

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
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
	if err := validateLimit(batchSize); err != nil {
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
func parseSearchInfo(searchParamsPair []*commonpb.KeyValuePair, schema *schemapb.CollectionSchema, rankParams *rankParams) (*SearchInfo, error) {
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

	if err := validateLimit(topK); err != nil {
		if isIterator {
			// 1. if the request is from iterator, we set topK to QuotaLimit as the iterator can resolve too large topK problem
			// 2. GetAsInt64 has cached inside, no need to worry about cpu cost for parsing here
			topK = Params.QuotaConfig.TopKLimit.GetAsInt64()
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
				if err := validateLimit(offset); err != nil {
					return nil, fmt.Errorf("%s [%d] is invalid, %w", OffsetKey, offset, err)
				}
			}
		}
	}

	queryTopK := topK + offset
	if err := validateLimit(queryTopK); err != nil {
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
	searchParamStr, err := funcutil.GetAttrByKeyFromRepeatedKV(SearchParamsKey, searchParamsPair)
	if err != nil {
		searchParamStr = ""
	}

	// 5. parse group by field and group by size
	var groupByFieldId, groupSize int64
	var strictGroupSize bool
	if isAdvanced {
		groupByFieldId, groupSize, strictGroupSize = rankParams.GetGroupByFieldId(), rankParams.GetGroupSize(), rankParams.GetStrictGroupSize()
	} else {
		groupByInfo, err := parseGroupByInfo(searchParamsPair, schema)
		if err != nil {
			return nil, err
		}
		groupByFieldId, groupSize, strictGroupSize = groupByInfo.GetGroupByFieldId(), groupByInfo.GetGroupSize(), groupByInfo.GetStrictGroupSize()
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
		return nil, fmt.Errorf("parse iterator v2 info failed: %w", err)
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
			return nil, fmt.Errorf("Field %s not exist", name)
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
	groupByFieldId  int64
	groupSize       int64
	strictGroupSize bool
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

func parseGroupByInfo(searchParamsPair []*commonpb.KeyValuePair, schema *schemapb.CollectionSchema) (*groupByInfo, error) {
	ret := &groupByInfo{}

	// 1. parse group_by_field
	groupByFieldName, err := funcutil.GetAttrByKeyFromRepeatedKV(GroupByFieldKey, searchParamsPair)
	if err != nil {
		groupByFieldName = ""
	}
	var groupByFieldId int64 = -1
	if groupByFieldName != "" {
		fields := schema.GetFields()
		for _, field := range fields {
			if field.Name == groupByFieldName {
				groupByFieldId = field.FieldID
				break
			}
		}
		if groupByFieldId == -1 {
			return nil, merr.WrapErrFieldNotFound(groupByFieldName, "groupBy field not found in schema")
		}
	}
	ret.groupByFieldId = groupByFieldId

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
	if err = validateMaxQueryResultWindow(offset, limit); err != nil {
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
		limit:           limit,
		offset:          offset,
		roundDecimal:    roundDecimal,
		groupByFieldId:  groupByInfo.GetGroupByFieldId(),
		groupSize:       groupByInfo.GetGroupSize(),
		strictGroupSize: groupByInfo.GetStrictGroupSize(),
	}, nil
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
