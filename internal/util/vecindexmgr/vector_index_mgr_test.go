package vecindexmgr

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_VecIndex_DataType_Support(t *testing.T) {
	type testCase struct {
		indexType IndexType
		dataTypes []schemapb.DataType
		wants     []bool
	}

	tests := []testCase{
		{
			indexType: "FLAT",
			dataTypes: []schemapb.DataType{
				schemapb.DataType_FloatVector,
				schemapb.DataType_BinaryVector,
				schemapb.DataType_BFloat16Vector,
				schemapb.DataType_Float16Vector,
				schemapb.DataType_SparseFloatVector,
			},
			wants: []bool{true, false, true, true, false},
		},
		{
			indexType: "BIN_FLAT",
			dataTypes: []schemapb.DataType{
				schemapb.DataType_FloatVector,
				schemapb.DataType_BinaryVector,
				schemapb.DataType_BFloat16Vector,
				schemapb.DataType_Float16Vector,
				schemapb.DataType_SparseFloatVector,
			},
			wants: []bool{false, true, false, false, false},
		},
		{
			indexType: "IVF_FLAT",
			dataTypes: []schemapb.DataType{
				schemapb.DataType_FloatVector,
				schemapb.DataType_BinaryVector,
				schemapb.DataType_BFloat16Vector,
				schemapb.DataType_Float16Vector,
				schemapb.DataType_SparseFloatVector,
			},
			wants: []bool{true, false, true, true, false},
		},
		{
			indexType: "IVF_PQ",
			dataTypes: []schemapb.DataType{
				schemapb.DataType_FloatVector,
				schemapb.DataType_BinaryVector,
				schemapb.DataType_BFloat16Vector,
				schemapb.DataType_Float16Vector,
				schemapb.DataType_SparseFloatVector,
			},
			wants: []bool{true, false, true, true, false},
		},
		{
			indexType: "HNSW",
			dataTypes: []schemapb.DataType{
				schemapb.DataType_FloatVector,
				schemapb.DataType_BinaryVector,
				schemapb.DataType_BFloat16Vector,
				schemapb.DataType_Float16Vector,
				schemapb.DataType_SparseFloatVector,
			},
			wants: []bool{true, true, true, true, false},
		},
		{
			indexType: "DISKANN",
			dataTypes: []schemapb.DataType{
				schemapb.DataType_FloatVector,
				schemapb.DataType_BinaryVector,
				schemapb.DataType_BFloat16Vector,
				schemapb.DataType_Float16Vector,
				schemapb.DataType_SparseFloatVector,
			},
			wants: []bool{true, false, true, true, false},
		},
		{
			indexType: "UNKNOWN",
			dataTypes: []schemapb.DataType{
				schemapb.DataType_FloatVector,
				schemapb.DataType_BinaryVector,
				schemapb.DataType_BFloat16Vector,
				schemapb.DataType_Float16Vector,
				schemapb.DataType_SparseFloatVector,
			},
			wants: []bool{false, false, false, false, false},
		},
	}

	mgr := GetVecIndexMgrInstance()

	for _, tt := range tests {
		t.Run(string(tt.indexType), func(t *testing.T) {
			for i, dataType := range tt.dataTypes {
				got := mgr.IsDataTypeSupport(tt.indexType, dataType, schemapb.DataType_None)
				if got != tt.wants[i] {
					t.Errorf("IsDataTypeSupport(%v, %v) = %v, want %v", tt.indexType, dataType, got, tt.wants[i])
				}
			}
		})
	}
}

func Test_VecIndex_IsNoTrainIndex(t *testing.T) {
	mgr := GetVecIndexMgrInstance()
	tests := []struct {
		indexType IndexType
		want      bool
	}{
		{
			indexType: "FLAT",
			want:      true,
		},
		{
			indexType: "BIN_FLAT",
			want:      true,
		},
		{
			indexType: "IVF_FLAT",
			want:      false,
		},
		{
			indexType: "IVF_SQ8",
			want:      false,
		},
		{
			indexType: "IVF_PQ",
			want:      false,
		},
		{
			indexType: "HNSW",
			want:      false,
		},
		{
			indexType: "DISKANN",
			want:      false,
		},
		{
			indexType: "UNKNOWN",
			want:      false,
		},
	}

	for _, test := range tests {
		got := mgr.IsNoTrainIndex(test.indexType)
		if got != test.want {
			t.Errorf("IsNoTrainIndex(%v) = %v, 期望 %v", test.indexType, got, test.want)
		}
	}
}

func Test_VecIndex_IsDiskVecIndex(t *testing.T) {
	mgr := GetVecIndexMgrInstance()
	tests := []struct {
		indexType IndexType
		want      bool
	}{
		{
			indexType: "FLAT",
			want:      false,
		},
		{
			indexType: "BIN_FLAT",
			want:      false,
		},
		{
			indexType: "IVF_FLAT",
			want:      false,
		},
		{
			indexType: "DISKANN",
			want:      true,
		},
		{
			indexType: "HNSW",
			want:      false,
		},
		{
			indexType: "UNKNOWN",
			want:      false,
		},
	}

	for _, test := range tests {
		got := mgr.IsDiskVecIndex(test.indexType)
		if got != test.want {
			t.Errorf("IsDiskVecIndex(%v) = %v, want %v", test.indexType, got, test.want)
		}
	}
}

func Test_VecIndex_IsMvSupported(t *testing.T) {
	mgr := GetVecIndexMgrInstance()
	tests := []struct {
		indexType IndexType
		want      bool
	}{
		{
			indexType: "FLAT",
			want:      false,
		},
		{
			indexType: "IVF_FLAT",
			want:      false,
		},
		{
			indexType: "DISKANN",
			want:      false,
		},
		{
			indexType: "HNSW",
			want:      true,
		},
		{
			indexType: "UNKNOWN",
			want:      false,
		},
	}

	for _, test := range tests {
		got := mgr.IsMvSupported(test.indexType)
		if got != test.want {
			t.Errorf("IsMvSupported(%v) = %v, want %v", test.indexType, got, test.want)
		}
	}
}
