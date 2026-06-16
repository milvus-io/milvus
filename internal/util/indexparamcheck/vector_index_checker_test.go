package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
)

func TestVecIndexChecker_StaticCheck(t *testing.T) {
	checker := newVecIndexChecker()

	tests := []struct {
		name     string
		dataType schemapb.DataType
		elemType schemapb.DataType
		params   map[string]string
		wantErr  bool
	}{
		{
			name:     "Valid IVF_FLAT index",
			dataType: schemapb.DataType_FloatVector,
			params: map[string]string{
				"index_type":  "IVF_FLAT",
				"metric_type": "L2",
				"nlist":       "1024",
			},
			wantErr: false,
		},
		{
			name:     "Invalid index type",
			dataType: schemapb.DataType_FloatVector,
			params: map[string]string{
				"index_type": "INVALID_INDEX",
			},
			wantErr: true,
		},
		{
			name:     "Missing index type",
			dataType: schemapb.DataType_FloatVector,
			params:   map[string]string{},
			wantErr:  true,
		},
		{
			name:     "Sparse with invalid metric",
			dataType: schemapb.DataType_SparseFloatVector,
			params: map[string]string{
				"index_type":  "SPARSE_INVERTED_INDEX",
				"metric_type": "L2",
			},
			wantErr: true,
		},
		{
			name:     "Sparse with valid metric and invalid inverted_index_algo",
			dataType: schemapb.DataType_SparseFloatVector,
			params: map[string]string{
				"index_type":            "SPARSE_INVERTED_INDEX",
				"metric_type":           "IP",
				SparseInvertedIndexAlgo: "INVALID_ALGO",
			},
			wantErr: true,
		},
		{
			name:     "Sparse WAND with invalid inverted_index_algo",
			dataType: schemapb.DataType_SparseFloatVector,
			params: map[string]string{
				"index_type":            "SPARSE_WAND",
				"metric_type":           "IP",
				SparseInvertedIndexAlgo: "NOT_AN_ALGO",
			},
			wantErr: true,
		},
		{
			name:     "ArrayOfVector float accepts MaxSimCosine",
			dataType: schemapb.DataType_ArrayOfVector,
			elemType: schemapb.DataType_FloatVector,
			params: map[string]string{
				common.IndexTypeKey:  "HNSW",
				common.MetricTypeKey: metric.MaxSimCosine,
				HNSWM:                "16",
				EFConstruction:       "200",
			},
			wantErr: false,
		},
		{
			name:     "ArrayOfVector float rejects MaxSimHamming",
			dataType: schemapb.DataType_ArrayOfVector,
			elemType: schemapb.DataType_FloatVector,
			params: map[string]string{
				common.IndexTypeKey:  "HNSW_SQ",
				common.MetricTypeKey: metric.MaxSimHamming,
			},
			wantErr: true,
		},
		{
			name:     "ArrayOfVector binary accepts MaxSimHamming",
			dataType: schemapb.DataType_ArrayOfVector,
			elemType: schemapb.DataType_BinaryVector,
			params: map[string]string{
				common.IndexTypeKey:  "HNSW",
				common.MetricTypeKey: metric.MaxSimHamming,
				HNSWM:                "16",
				EFConstruction:       "200",
			},
			wantErr: false,
		},
		{
			name:     "ArrayOfVector binary rejects MaxSimCosine",
			dataType: schemapb.DataType_ArrayOfVector,
			elemType: schemapb.DataType_BinaryVector,
			params: map[string]string{
				common.IndexTypeKey:  "HNSW",
				common.MetricTypeKey: metric.MaxSimCosine,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checker.StaticCheck(tt.dataType, tt.elemType, tt.params)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestVecIndexChecker_CheckValidDataType(t *testing.T) {
	checker := newVecIndexChecker()

	tests := []struct {
		name      string
		indexType IndexType
		field     *schemapb.FieldSchema
		wantErr   bool
	}{
		{
			name:      "Valid float vector",
			indexType: "IVF_FLAT",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_FloatVector,
			},
			wantErr: false,
		},
		{
			name:      "Invalid data type",
			indexType: "IVF_FLAT",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int64,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checker.CheckValidDataType(tt.indexType, tt.field)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestVecIndexChecker_SetDefaultMetricTypeIfNotExist(t *testing.T) {
	checker := newVecIndexChecker()

	tests := []struct {
		name         string
		dataType     schemapb.DataType
		params       map[string]string
		expectedType string
	}{
		{
			name:         "Float vector",
			dataType:     schemapb.DataType_FloatVector,
			params:       map[string]string{},
			expectedType: FloatVectorDefaultMetricType,
		},
		{
			name:         "Binary vector",
			dataType:     schemapb.DataType_BinaryVector,
			params:       map[string]string{},
			expectedType: BinaryVectorDefaultMetricType,
		},
		{
			name:         "int vector",
			dataType:     schemapb.DataType_Int8Vector,
			params:       map[string]string{},
			expectedType: IntVectorDefaultMetricType,
		},
		{
			name:         "Existing metric type",
			dataType:     schemapb.DataType_FloatVector,
			params:       map[string]string{"metric_type": "IP"},
			expectedType: "IP",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker.SetDefaultMetricTypeIfNotExist(tt.dataType, tt.params)
			assert.Equal(t, tt.expectedType, tt.params["metric_type"])
		})
	}
}
