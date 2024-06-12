package proxy

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type rankParams struct {
	limit        int64
	offset       int64
	roundDecimal int64
}

// parseSearchInfo returns QueryInfo and offset
func parseSearchInfo(searchParamsPair []*commonpb.KeyValuePair, schema *schemapb.CollectionSchema, ignoreOffset bool) (*planpb.QueryInfo, int64, error) {
	// 0. parse iterator field
	isIterator, _ := funcutil.GetAttrByKeyFromRepeatedKV(IteratorField, searchParamsPair)

	// 1. parse offset and real topk
	topKStr, err := funcutil.GetAttrByKeyFromRepeatedKV(TopKKey, searchParamsPair)
	if err != nil {
		return nil, 0, errors.New(TopKKey + " not found in search_params")
	}
	topK, err := strconv.ParseInt(topKStr, 0, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("%s [%s] is invalid", TopKKey, topKStr)
	}
	if err := validateLimit(topK); err != nil {
		if isIterator == "True" {
			// 1. if the request is from iterator, we set topK to QuotaLimit as the iterator can resolve too large topK problem
			// 2. GetAsInt64 has cached inside, no need to worry about cpu cost for parsing here
			topK = Params.QuotaConfig.TopKLimit.GetAsInt64()
		} else {
			return nil, 0, fmt.Errorf("%s [%d] is invalid, %w", TopKKey, topK, err)
		}
	}

	var offset int64
	if !ignoreOffset {
		offsetStr, err := funcutil.GetAttrByKeyFromRepeatedKV(OffsetKey, searchParamsPair)
		if err == nil {
			offset, err = strconv.ParseInt(offsetStr, 0, 64)
			if err != nil {
				return nil, 0, fmt.Errorf("%s [%s] is invalid", OffsetKey, offsetStr)
			}

			if offset != 0 {
				if err := validateLimit(offset); err != nil {
					return nil, 0, fmt.Errorf("%s [%d] is invalid, %w", OffsetKey, offset, err)
				}
			}
		}
	}

	queryTopK := topK + offset
	if err := validateLimit(queryTopK); err != nil {
		return nil, 0, fmt.Errorf("%s+%s [%d] is invalid, %w", OffsetKey, TopKKey, queryTopK, err)
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

	roundDecimal, err := strconv.ParseInt(roundDecimalStr, 0, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	if roundDecimal != -1 && (roundDecimal > 6 || roundDecimal < 0) {
		return nil, 0, fmt.Errorf("%s [%s] is invalid, should be -1 or an integer in range [0, 6]", RoundDecimalKey, roundDecimalStr)
	}

	// 4. parse search param str
	searchParamStr, err := funcutil.GetAttrByKeyFromRepeatedKV(SearchParamsKey, searchParamsPair)
	if err != nil {
		searchParamStr = ""
	}

	// 5. parse group by field and group by size
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
			return nil, 0, merr.WrapErrFieldNotFound(groupByFieldName, "groupBy field not found in schema")
		}
	}

	var groupSize int64
	groupSizeStr, err := funcutil.GetAttrByKeyFromRepeatedKV(GroupSizeKey, searchParamsPair)
	if err != nil {
		groupSize = 1
	} else {
		groupSize, err = strconv.ParseInt(groupSizeStr, 0, 64)
		if err != nil || groupSize <= 0 {
			groupSize = 1
		}
	}

	// 6. parse iterator tag, prevent trying to groupBy when doing iteration or doing range-search
	if isIterator == "True" && groupByFieldId > 0 {
		return nil, 0, merr.WrapErrParameterInvalid("", "",
			"Not allowed to do groupBy when doing iteration")
	}
	if strings.Contains(searchParamStr, radiusKey) && groupByFieldId > 0 {
		return nil, 0, merr.WrapErrParameterInvalid("", "",
			"Not allowed to do range-search when doing search-group-by")
	}

	return &planpb.QueryInfo{
		Topk:           queryTopK,
		MetricType:     metricType,
		SearchParams:   searchParamStr,
		RoundDecimal:   roundDecimal,
		GroupByFieldId: groupByFieldId,
		GroupSize:      groupSize,
	}, offset, nil
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

	partitionsSet := typeutil.NewSet[int64]()
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
			if !partitionsSet.Contain(partitionID) {
				partitionsSet.Insert(partitionID)
			}
		}
	}
	return partitionsSet.Collect(), nil
}

// parseRankParams get limit and offset from rankParams, both are optional.
func parseRankParams(rankParamsPair []*commonpb.KeyValuePair) (*rankParams, error) {
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

	return &rankParams{
		limit:        limit,
		offset:       offset,
		roundDecimal: roundDecimal,
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
	}

	for _, sub := range req.GetRequests() {
		subReq := &milvuspb.SubSearchRequest{
			Dsl:              sub.GetDsl(),
			PlaceholderGroup: sub.GetPlaceholderGroup(),
			DslType:          sub.GetDslType(),
			SearchParams:     sub.GetSearchParams(),
			Nq:               sub.GetNq(),
		}
		ret.SubReqs = append(ret.SubReqs, subReq)
	}
	return ret
}
