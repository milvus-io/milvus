package expr

import "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"

func boolParam(value bool) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: value}}
}

func intParam(value int64) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: value}}
}

func doubleParam(value float64) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_DoubleValue{DoubleValue: value}}
}

func stringParam(value string) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_StringValue{StringValue: value}}
}

func arrayParam(values ...*schemapb.FunctionParamValue) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_ArrayValue{ArrayValue: &schemapb.FunctionParamArray{Values: values}}}
}
