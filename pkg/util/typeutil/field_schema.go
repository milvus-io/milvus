package typeutil

import (
	"fmt"
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

type FieldSchemaHelper struct {
	schema      *schemapb.FieldSchema
	typeParams  *kvPairsHelper[string, string]
	indexParams *kvPairsHelper[string, string]
}

func (h *FieldSchemaHelper) GetDim() (int64, error) {
	if !IsVectorType(h.schema.GetDataType()) {
		return 0, fmt.Errorf("%s is not of vector type", h.schema.GetDataType())
	}
	if IsSparseFloatVectorType(h.schema.GetDataType()) {
		return 0, errors.New("typeutil.GetDim should not invoke on sparse vector type")
	}

	getDim := func(kvPairs *kvPairsHelper[string, string]) (int64, error) {
		dimStr, err := kvPairs.Get(common.DimKey)
		if err != nil {
			return 0, errors.New("dim not found")
		}
		dim, err := strconv.Atoi(dimStr)
		if err != nil {
			return 0, fmt.Errorf("invalid dimension: %s", dimStr)
		}
		return int64(dim), nil
	}

	if dim, err := getDim(h.typeParams); err == nil {
		return dim, nil
	}

	return getDim(h.indexParams)
}

func (h *FieldSchemaHelper) EnableMatch() bool {
	if !IsStringType(h.schema.GetDataType()) {
		return false
	}
	s, err := h.typeParams.Get("enable_match")
	if err != nil {
		return false
	}
	enable, err := strconv.ParseBool(s)
	return err == nil && enable
}

func (h *FieldSchemaHelper) EnableJSONKeyStatsIndex() bool {
	return IsJSONType(h.schema.GetDataType())
}

func (h *FieldSchemaHelper) EnableAnalyzer() bool {
	if !IsStringType(h.schema.GetDataType()) {
		return false
	}
	s, err := h.typeParams.Get("enable_analyzer")
	if err != nil {
		return false
	}
	enable, err := strconv.ParseBool(s)
	return err == nil && enable
}

func (h *FieldSchemaHelper) GetMultiAnalyzerParams() (string, bool) {
	if !IsStringType(h.schema.GetDataType()) {
		return "", false
	}
	value, err := h.typeParams.Get("multi_analyzer_params")
	return value, err == nil
}

func (h *FieldSchemaHelper) HasAnalyzerParams() bool {
	if !IsStringType(h.schema.GetDataType()) {
		return false
	}
	_, err := h.typeParams.Get("analyzer_params")
	return err == nil
}

func CreateFieldSchemaHelper(schema *schemapb.FieldSchema) *FieldSchemaHelper {
	return &FieldSchemaHelper{
		schema:      schema,
		typeParams:  NewKvPairs(schema.GetTypeParams()),
		indexParams: NewKvPairs(schema.GetIndexParams()),
	}
}
