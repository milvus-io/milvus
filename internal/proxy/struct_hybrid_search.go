package proxy

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	elementScopeKey          = "element_scope"
	elementCollapseMax       = "max"
	elementCollapseSum       = "sum"
	elementCollapseAvg       = "avg"
	elementCollapseTopKSum   = "topk_sum"
	elementCollapseTopKAvg   = "topk_avg"
	hybridElementKeyPrefix   = "__milvus_element_key"
	hybridElementKeySep      = "\x1f"
	hybridElementKeyIntPK    = "i"
	hybridElementKeyStringPK = "s"
)

type elementCollapseConfig struct {
	Strategy string
	TopK     int
}

type hybridSubSearchKind int

const (
	hybridSubSearchNormal hybridSubSearchKind = iota
	hybridSubSearchStructEmbList
	hybridSubSearchStructElement
)

type hybridSubSearchInfo struct {
	Kind                  hybridSubSearchKind
	ParentStructFieldName string
	ElementScopeProvided  bool
	Collapse              elementCollapseConfig
}

func defaultElementCollapseConfig() elementCollapseConfig {
	return elementCollapseConfig{Strategy: elementCollapseMax}
}

func parseAndRemoveElementScope(searchParamStr string) (elementCollapseConfig, bool, string, error) {
	if !gjson.Get(searchParamStr, elementScopeKey).Exists() {
		return elementCollapseConfig{}, false, searchParamStr, nil
	}
	if !gjson.Valid(searchParamStr) {
		return elementCollapseConfig{}, false, "", merr.WrapErrParameterInvalidMsg("%s must be valid JSON", ParamsKey)
	}

	var root map[string]json.RawMessage
	if err := json.Unmarshal([]byte(searchParamStr), &root); err != nil {
		return elementCollapseConfig{}, false, "", merr.WrapErrParameterInvalidMsg("%s must be a JSON object: %v", ParamsKey, err)
	}
	scopeRaw, ok := root[elementScopeKey]
	if !ok {
		return elementCollapseConfig{}, false, searchParamStr, nil
	}

	cfg, err := parseElementScope(scopeRaw)
	if err != nil {
		return elementCollapseConfig{}, false, "", err
	}
	delete(root, elementScopeKey)
	sanitized, err := json.Marshal(root)
	if err != nil {
		return elementCollapseConfig{}, false, "", merr.WrapErrServiceInternalMsg("failed to rewrite search params without %s: %v", elementScopeKey, err)
	}
	return cfg, true, string(sanitized), nil
}

func parseElementScope(scopeRaw json.RawMessage) (elementCollapseConfig, error) {
	var scope map[string]json.RawMessage
	if err := json.Unmarshal(scopeRaw, &scope); err != nil {
		return elementCollapseConfig{}, merr.WrapErrParameterInvalidMsg("%s must be a JSON object: %v", elementScopeKey, err)
	}
	for key := range scope {
		if key != "collapse" {
			return elementCollapseConfig{}, merr.WrapErrParameterInvalidMsg("unsupported %s key: %s", elementScopeKey, key)
		}
	}
	collapseRaw, ok := scope["collapse"]
	if !ok {
		return elementCollapseConfig{}, merr.WrapErrParameterMissingMsg("%s.collapse is required", elementScopeKey)
	}

	var collapse map[string]json.RawMessage
	if err := json.Unmarshal(collapseRaw, &collapse); err != nil {
		return elementCollapseConfig{}, merr.WrapErrParameterInvalidMsg("%s.collapse must be a JSON object: %v", elementScopeKey, err)
	}
	for key := range collapse {
		if key != "strategy" && key != "topk" {
			return elementCollapseConfig{}, merr.WrapErrParameterInvalidMsg("unsupported %s.collapse key: %s", elementScopeKey, key)
		}
	}

	var strategy string
	if raw, ok := collapse["strategy"]; ok {
		if err := json.Unmarshal(raw, &strategy); err != nil {
			return elementCollapseConfig{}, merr.WrapErrParameterInvalidMsg("%s.collapse.strategy must be a string", elementScopeKey)
		}
	}
	strategy = strings.TrimSpace(strategy)
	if strategy == "" {
		return elementCollapseConfig{}, merr.WrapErrParameterMissingMsg("%s.collapse.strategy is required", elementScopeKey)
	}
	if !isSupportedElementCollapseStrategy(strategy) {
		return elementCollapseConfig{}, merr.WrapErrParameterInvalidMsg("unsupported %s.collapse.strategy: %s", elementScopeKey, strategy)
	}

	cfg := elementCollapseConfig{Strategy: strategy}
	if raw, ok := collapse["topk"]; ok {
		if err := json.Unmarshal(raw, &cfg.TopK); err != nil {
			return elementCollapseConfig{}, merr.WrapErrParameterInvalidMsg("%s.collapse.topk must be an integer", elementScopeKey)
		}
		if cfg.TopK <= 0 {
			return elementCollapseConfig{}, merr.WrapErrParameterInvalidMsg("%s.collapse.topk must be positive", elementScopeKey)
		}
	}

	switch strategy {
	case elementCollapseTopKSum, elementCollapseTopKAvg:
		if cfg.TopK <= 0 {
			return elementCollapseConfig{}, merr.WrapErrParameterMissingMsg("%s.collapse.topk is required for strategy %s", elementScopeKey, strategy)
		}
	default:
		if cfg.TopK != 0 {
			return elementCollapseConfig{}, merr.WrapErrParameterInvalidMsg("%s.collapse.topk is only valid for topk strategies", elementScopeKey)
		}
	}
	return cfg, nil
}

func isSupportedElementCollapseStrategy(strategy string) bool {
	switch strategy {
	case elementCollapseMax, elementCollapseSum, elementCollapseAvg, elementCollapseTopKSum, elementCollapseTopKAvg:
		return true
	default:
		return false
	}
}

func isElementCollapseSumFamily(strategy string) bool {
	return strategy == elementCollapseSum || strategy == elementCollapseTopKSum
}

func validateElementCollapseMetricType(config elementCollapseConfig, metricType string) error {
	if config.Strategy == "" ||
		!isElementCollapseSumFamily(config.Strategy) ||
		strings.TrimSpace(metricType) == "" ||
		metric.PositivelyRelated(metricType) {
		return nil
	}
	return merr.WrapErrParameterInvalidMsg(
		"%s.collapse.strategy %s is only supported for positively related metrics",
		elementScopeKey, config.Strategy)
}

func resolveElementCollapseMetricType(requestMetricType string, field *schemapb.FieldSchema) string {
	if strings.TrimSpace(requestMetricType) != "" || field == nil {
		return requestMetricType
	}
	indexMetricType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.MetricTypeKey, field.GetIndexParams())
	if err != nil {
		return ""
	}
	return indexMetricType
}

func getStructParentFieldName(schema *schemapb.CollectionSchema, fieldID int64) (string, bool) {
	for _, structField := range schema.GetStructArrayFields() {
		for _, subField := range structField.GetFields() {
			if subField.GetFieldID() == fieldID {
				return structField.GetName(), true
			}
		}
	}
	return "", false
}

func classifyHybridSubSearch(schema *schemapb.CollectionSchema, fieldID int64, placeholderType commonpb.PlaceholderType) hybridSubSearchInfo {
	field := typeutil.GetField(schema, fieldID)
	if field == nil || field.GetDataType() != schemapb.DataType_ArrayOfVector {
		return hybridSubSearchInfo{Kind: hybridSubSearchNormal}
	}
	parent, ok := getStructParentFieldName(schema, fieldID)
	if !ok {
		return hybridSubSearchInfo{Kind: hybridSubSearchNormal}
	}
	if placeholderType == 0 || isEmbeddingListPlaceholderType(placeholderType) {
		return hybridSubSearchInfo{Kind: hybridSubSearchStructEmbList, ParentStructFieldName: parent}
	}
	return hybridSubSearchInfo{Kind: hybridSubSearchStructElement, ParentStructFieldName: parent}
}

func inferElementLevelHybrid(infos []hybridSubSearchInfo) bool {
	if len(infos) == 0 {
		return false
	}
	parent := ""
	for _, info := range infos {
		if info.Kind != hybridSubSearchStructElement {
			return false
		}
		if parent == "" {
			parent = info.ParentStructFieldName
			continue
		}
		if info.ParentStructFieldName != parent {
			return false
		}
	}
	return true
}

func makeHybridElementKey(pk any, elementIndex int64) string {
	switch v := pk.(type) {
	case int64:
		return fmt.Sprintf("%s%s%s%s%d%s%d", hybridElementKeyPrefix, hybridElementKeySep, hybridElementKeyIntPK, hybridElementKeySep, v, hybridElementKeySep, elementIndex)
	case string:
		return fmt.Sprintf("%s%s%s%s%s%s%d", hybridElementKeyPrefix, hybridElementKeySep, hybridElementKeyStringPK, hybridElementKeySep, base64.RawStdEncoding.EncodeToString([]byte(v)), hybridElementKeySep, elementIndex)
	default:
		return fmt.Sprintf("%s%s%T%s%v%s%d", hybridElementKeyPrefix, hybridElementKeySep, pk, hybridElementKeySep, pk, hybridElementKeySep, elementIndex)
	}
}

func parseHybridElementKey(key string) (any, int64, bool) {
	parts := strings.Split(key, hybridElementKeySep)
	if len(parts) != 4 || parts[0] != hybridElementKeyPrefix {
		return nil, 0, false
	}
	elementIndex, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return nil, 0, false
	}
	switch parts[1] {
	case hybridElementKeyIntPK:
		pk, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return nil, 0, false
		}
		return pk, elementIndex, true
	case hybridElementKeyStringPK:
		decoded, err := base64.RawStdEncoding.DecodeString(parts[2])
		if err != nil {
			return nil, 0, false
		}
		return string(decoded), elementIndex, true
	default:
		return nil, 0, false
	}
}
