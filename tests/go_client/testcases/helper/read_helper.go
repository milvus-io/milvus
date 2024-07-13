package helper

import (
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

type LoadParams struct {
	CollectionName string
	Replica        int
}

func NewLoadParams(collectionName string) *LoadParams {
	return &LoadParams{
		CollectionName: collectionName,
	}
}

func (opt *LoadParams) TWithReplica(replica int) *LoadParams {
	opt.Replica = replica
	return opt
}

// GenSearchVectors gen search vectors
func GenSearchVectors(nq int, dim int, dataType entity.FieldType) []entity.Vector {
	vectors := make([]entity.Vector, 0, nq)
	switch dataType {
	case entity.FieldTypeFloatVector:
		for i := 0; i < nq; i++ {
			vector := common.GenFloatVector(dim)
			vectors = append(vectors, entity.FloatVector(vector))
		}
	case entity.FieldTypeBinaryVector:
		for i := 0; i < nq; i++ {
			vector := common.GenBinaryVector(dim)
			vectors = append(vectors, entity.BinaryVector(vector))
		}
	case entity.FieldTypeFloat16Vector:
		for i := 0; i < nq; i++ {
			vector := common.GenFloat16Vector(dim)
			vectors = append(vectors, entity.Float16Vector(vector))
		}
	case entity.FieldTypeBFloat16Vector:
		for i := 0; i < nq; i++ {
			vector := common.GenBFloat16Vector(dim)
			vectors = append(vectors, entity.BFloat16Vector(vector))
		}
	case entity.FieldTypeSparseVector:
		for i := 0; i < nq; i++ {
			vec := common.GenSparseVector(dim)
			vectors = append(vectors, vec)
		}
	}
	return vectors
}
