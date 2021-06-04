package querynode

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func TestRetrieve_Merge(t *testing.T) {

	col1 := &schemapb.FieldData{
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{1, 2, 3},
					},
				},
			},
		},
	}

	col2 := &schemapb.FieldData{
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: []float32{1, 1, 2, 2, 3, 3},
					},
				},
			},
		},
	}

	subRes := &planpb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3},
				},
			},
		},
		FieldsData: []*schemapb.FieldData{
			col1,
			col2,
		},
	}
	finalRes, err := mergeRetrieveResults([]*planpb.RetrieveResults{subRes, subRes})
	assert.NoError(t, err)
	println(finalRes.String())
}
