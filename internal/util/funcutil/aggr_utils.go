package funcutil

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/samber/lo"
)

type AggrReducer interface {
	Reduce(data []*internalpb.AggrData) (*internalpb.AggrData, error)
	ReduceFinal(data []*internalpb.AggrData) (*schemapb.FieldData, error)
}

func NewAggrReducer(fnName string, inputTypes []schemapb.DataType) (AggrReducer, error) {
	// TODO:ashkrisk can probably use a map here instead
	switch fnName {
	case "max":
		return newMaxReducer(inputTypes)
		// case "min":
		// 	return newMinReducer(inputTypes)
		// case "avg":
		// 	return newAvgReducer(inputTypes)
	}

	return nil, fmt.Errorf("Unknown aggregation function '%s'", fnName)
}

func newMaxReducer(inputTypes []schemapb.DataType) (AggrReducer, error) {
	if len(inputTypes) != 1 {
		return nil, fmt.Errorf("max reducer expects single input, got %d", len(inputTypes))
	}
	switch inputTypes[0] {
	case schemapb.DataType_Int8:
		fallthrough
	case schemapb.DataType_Int16:
		fallthrough
	case schemapb.DataType_Int32:
		fallthrough
	case schemapb.DataType_Int64:
		return newMaxReducerInt(), nil
	case schemapb.DataType_Float:
		fallthrough
	case schemapb.DataType_Double:
		return newMaxReducerFloat(), nil
	case schemapb.DataType_String:
		fallthrough
	case schemapb.DataType_VarChar:
		return newMaxReducerString(), nil
	}

	return nil, fmt.Errorf("Unsupported type for maxReducer: %s", inputTypes[0].String())
}

type maxReducerInt struct{}

type maxReducerFloat struct{}

type maxReducerString struct{}

func newMaxReducerInt() AggrReducer {
	return maxReducerInt{}
}

func newMaxReducerFloat() AggrReducer {
	return maxReducerFloat{}
}

func newMaxReducerString() AggrReducer {
	return maxReducerString{}
}

func (r maxReducerInt) Reduce(data []*internalpb.AggrData) (*internalpb.AggrData, error) {
	vals := lo.Map(data, func(d *internalpb.AggrData, _ int) int64 {
		return d.GetMaxIr().GetInt()
	})
	max := lo.Max(vals)
	return &internalpb.AggrData{
		Data: &internalpb.AggrData_MaxIr{
			MaxIr: &internalpb.MaxIR{
				Max: &internalpb.MaxIR_Int{
					Int: max,
				},
			},
		},
	}, nil
}

func (r maxReducerFloat) Reduce(data []*internalpb.AggrData) (*internalpb.AggrData, error) {
	vals := lo.Map(data, func(d *internalpb.AggrData, _ int) float64 {
		return d.GetMaxIr().GetFloat()
	})
	max := lo.Max(vals)
	return &internalpb.AggrData{
		Data: &internalpb.AggrData_MaxIr{
			MaxIr: &internalpb.MaxIR{
				Max: &internalpb.MaxIR_Float{
					Float: max,
				},
			},
		},
	}, nil
}

func (r maxReducerString) Reduce(data []*internalpb.AggrData) (*internalpb.AggrData, error) {
	vals := lo.Map(data, func(d *internalpb.AggrData, _ int) string {
		return d.GetMaxIr().GetStr()
	})
	max := lo.Max(vals)
	return &internalpb.AggrData{
		Data: &internalpb.AggrData_MaxIr{
			MaxIr: &internalpb.MaxIR{
				Max: &internalpb.MaxIR_Str{
					Str: max,
				},
			},
		},
	}, nil
}

func (r maxReducerInt) ReduceFinal(data []*internalpb.AggrData) (*schemapb.FieldData, error) {
	ir, err := r.Reduce(data)
	if err != nil {
		return nil, err
	}
	max := ir.GetMaxIr().GetInt()
	return &schemapb.FieldData{
		Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{max},
					},
				},
			},
		},
	}, nil
}

func (r maxReducerFloat) ReduceFinal(data []*internalpb.AggrData) (*schemapb.FieldData, error) {
	ir, err := r.Reduce(data)
	if err != nil {
		return nil, err
	}
	max := ir.GetMaxIr().GetFloat()
	return &schemapb.FieldData{
		Type: schemapb.DataType_Double,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: []float64{max},
					},
				},
			},
		},
	}, nil
}

func (r maxReducerString) ReduceFinal(data []*internalpb.AggrData) (*schemapb.FieldData, error) {
	ir, err := r.Reduce(data)
	if err != nil {
		return nil, err
	}
	max := ir.GetMaxIr().GetStr()
	return &schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{max},
					},
				},
			},
		},
	}, nil
}
