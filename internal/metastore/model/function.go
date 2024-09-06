package model

import (
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type Function struct {
	Name string
	ID   int64
	Type schemapb.FunctionType

	InputFieldIDs   []int64
	InputFieldNames []string

	OutputFieldIDs   []int64
	OutputFieldNames []string

	Params []*commonpb.KeyValuePair
}

func (f *Function) Clone() *Function {
	return &Function{
		Name: f.Name,
		Type: f.Type,
		ID:   f.ID,

		InputFieldIDs:   f.InputFieldIDs,
		InputFieldNames: f.InputFieldNames,

		OutputFieldIDs:   f.OutputFieldIDs,
		OutputFieldNames: f.OutputFieldNames,
		Params:           f.Params,
	}
}

func (f *Function) Equal(other Function) bool {
	return f.Name == other.Name &&
		f.Type == other.Type &&
		slices.Equal(f.InputFieldNames, other.InputFieldNames) &&
		slices.Equal(f.InputFieldIDs, other.InputFieldIDs) &&
		slices.Equal(f.OutputFieldNames, other.OutputFieldNames) &&
		slices.Equal(f.OutputFieldIDs, other.OutputFieldIDs) &&
		slices.Equal(f.Params, other.Params)
}

func CloneFunctions(functions []*Function) []*Function {
	clone := make([]*Function, len(functions))
	for i, function := range functions {
		clone[i] = function.Clone()
	}
	return functions
}

func MarshalFunctionModel(function *Function) *schemapb.FunctionSchema {
	if function == nil {
		return nil
	}

	return &schemapb.FunctionSchema{
		Name:             function.Name,
		Type:             function.Type,
		Id:               function.ID,
		InputFieldIds:    function.InputFieldIDs,
		InputFieldNames:  function.InputFieldNames,
		OutputFieldIds:   function.OutputFieldIDs,
		OutputFieldNames: function.OutputFieldNames,
		Params:           function.Params,
	}
}

func UnmarshalFunctionModel(schema *schemapb.FunctionSchema) *Function {
	if schema == nil {
		return nil
	}
	return &Function{
		Name: schema.GetName(),
		ID:   schema.GetId(),
		Type: schema.GetType(),

		InputFieldIDs:   schema.GetInputFieldIds(),
		InputFieldNames: schema.GetInputFieldNames(),

		OutputFieldIDs:   schema.GetOutputFieldIds(),
		OutputFieldNames: schema.GetOutputFieldNames(),
		Params:           schema.GetParams(),
	}
}

func MarshalFunctionModels(functions []*Function) []*schemapb.FunctionSchema {
	if functions == nil {
		return nil
	}

	functionSchemas := make([]*schemapb.FunctionSchema, len(functions))
	for idx, function := range functions {
		functionSchemas[idx] = MarshalFunctionModel(function)
	}
	return functionSchemas
}

func UnmarshalFunctionModels(functions []*schemapb.FunctionSchema) []*Function {
	if functions == nil {
		return nil
	}

	functionSchemas := make([]*Function, len(functions))
	for idx, function := range functions {
		functionSchemas[idx] = UnmarshalFunctionModel(function)
	}
	return functionSchemas
}
