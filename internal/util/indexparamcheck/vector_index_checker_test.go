package indexparamcheck

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestVecIndexChecker_StaticCheck(t *testing.T) {
	checker := newVecIndexChecker()

	tests := []struct {
		name     string
		dataType schemapb.DataType
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checker.StaticCheck(tt.dataType, tt.params)
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
