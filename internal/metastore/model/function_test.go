package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

var (
	functionSchemaPb = &schemapb.FunctionSchema{
		Id:               functionID,
		Name:             functionName,
		Type:             schemapb.FunctionType_BM25,
		InputFieldIds:    []int64{101},
		InputFieldNames:  []string{"text"},
		OutputFieldIds:   []int64{103},
		OutputFieldNames: []string{"sparse"},
	}

	functionModel = &Function{
		ID:               functionID,
		Name:             functionName,
		Type:             schemapb.FunctionType_BM25,
		InputFieldIDs:    []int64{101},
		InputFieldNames:  []string{"text"},
		OutputFieldIDs:   []int64{103},
		OutputFieldNames: []string{"sparse"},
	}
)

func TestMarshalFunctionModel(t *testing.T) {
	ret := MarshalFunctionModel(functionModel)
	assert.Equal(t, functionSchemaPb, ret)
	assert.Nil(t, MarshalFunctionModel(nil))
}

func TestMarshalFunctionModels(t *testing.T) {
	ret := MarshalFunctionModels([]*Function{functionModel})
	assert.Equal(t, []*schemapb.FunctionSchema{functionSchemaPb}, ret)
	assert.Nil(t, MarshalFunctionModels(nil))
}

func TestUnmarshalFunctionModel(t *testing.T) {
	ret := UnmarshalFunctionModel(functionSchemaPb)
	assert.Equal(t, functionModel, ret)
	assert.Nil(t, UnmarshalFunctionModel(nil))
}

func TestUnmarshalFunctionModels(t *testing.T) {
	ret := UnmarshalFunctionModels([]*schemapb.FunctionSchema{functionSchemaPb})
	assert.Equal(t, []*Function{functionModel}, ret)
	assert.Nil(t, UnmarshalFunctionModels(nil))
}

func TestFunctionEqual(t *testing.T) {
	EqualFunction := Function{
		ID:               functionID,
		Name:             functionName,
		Type:             schemapb.FunctionType_BM25,
		InputFieldIDs:    []int64{101},
		InputFieldNames:  []string{"text"},
		OutputFieldIDs:   []int64{103},
		OutputFieldNames: []string{"sparse"},
	}

	NoEqualFunction := Function{
		ID:               functionID,
		Name:             functionName,
		Type:             schemapb.FunctionType_BM25,
		InputFieldIDs:    []int64{101},
		InputFieldNames:  []string{"text"},
		OutputFieldIDs:   []int64{102},
		OutputFieldNames: []string{"sparse"},
	}

	assert.True(t, functionModel.Equal(EqualFunction))
	assert.True(t, functionModel.Equal(*functionModel.Clone()))
	assert.False(t, functionModel.Equal(NoEqualFunction))
}
