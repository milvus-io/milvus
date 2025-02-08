package helper

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

type GetFieldNameOpt func(opt *getFieldNameOpt)

type getFieldNameOpt struct {
	elementType entity.FieldType
	isDynamic   bool
}

func TWithElementType(eleType entity.FieldType) GetFieldNameOpt {
	return func(opt *getFieldNameOpt) {
		opt.elementType = eleType
	}
}

func TWithIsDynamic(isDynamic bool) GetFieldNameOpt {
	return func(opt *getFieldNameOpt) {
		opt.isDynamic = isDynamic
	}
}

func GetFieldNameByElementType(t entity.FieldType) string {
	switch t {
	case entity.FieldTypeBool:
		return common.DefaultBoolArrayField
	case entity.FieldTypeInt8:
		return common.DefaultInt8ArrayField
	case entity.FieldTypeInt16:
		return common.DefaultInt16ArrayField
	case entity.FieldTypeInt32:
		return common.DefaultInt32ArrayField
	case entity.FieldTypeInt64:
		return common.DefaultInt64ArrayField
	case entity.FieldTypeFloat:
		return common.DefaultFloatArrayField
	case entity.FieldTypeDouble:
		return common.DefaultDoubleArrayField
	case entity.FieldTypeVarChar:
		return common.DefaultVarcharArrayField
	default:
		log.Warn("GetFieldNameByElementType", zap.Any("ElementType", t))
		return common.DefaultArrayFieldName
	}
}

func GetFieldNameByFieldType(t entity.FieldType, opts ...GetFieldNameOpt) string {
	opt := &getFieldNameOpt{}
	for _, o := range opts {
		o(opt)
	}
	switch t {
	case entity.FieldTypeBool:
		return common.DefaultBoolFieldName
	case entity.FieldTypeInt8:
		return common.DefaultInt8FieldName
	case entity.FieldTypeInt16:
		return common.DefaultInt16FieldName
	case entity.FieldTypeInt32:
		return common.DefaultInt32FieldName
	case entity.FieldTypeInt64:
		return common.DefaultInt64FieldName
	case entity.FieldTypeFloat:
		return common.DefaultFloatFieldName
	case entity.FieldTypeDouble:
		return common.DefaultDoubleFieldName
	case entity.FieldTypeVarChar:
		return common.DefaultVarcharFieldName
	case entity.FieldTypeJSON:
		if opt.isDynamic {
			return common.DefaultDynamicFieldName
		}
		return common.DefaultJSONFieldName
	case entity.FieldTypeArray:
		return GetFieldNameByElementType(opt.elementType)
	case entity.FieldTypeBinaryVector:
		return common.DefaultBinaryVecFieldName
	case entity.FieldTypeFloatVector:
		return common.DefaultFloatVecFieldName
	case entity.FieldTypeFloat16Vector:
		return common.DefaultFloat16VecFieldName
	case entity.FieldTypeBFloat16Vector:
		return common.DefaultBFloat16VecFieldName
	case entity.FieldTypeSparseVector:
		return common.DefaultSparseVecFieldName
	default:
		return ""
	}
}

type CollectionFieldsType int32

const (
	// FieldTypeNone zero value place holder
	Int64Vec              CollectionFieldsType = 1 // int64 + floatVec
	VarcharBinary         CollectionFieldsType = 2 // varchar + binaryVec
	Int64VecJSON          CollectionFieldsType = 3 // int64 + floatVec + json
	Int64VecArray         CollectionFieldsType = 4 // int64 + floatVec + array
	Int64VarcharSparseVec CollectionFieldsType = 5 // int64 + varchar + sparse vector
	Int64MultiVec         CollectionFieldsType = 6 // int64 + floatVec + binaryVec + fp16Vec + bf16vec
	AllFields             CollectionFieldsType = 7 // all fields excepted sparse
	Int64VecAllScalar     CollectionFieldsType = 8 // int64 + floatVec + all scalar fields
	FullTextSearch        CollectionFieldsType = 9 // int64 + varchar + sparse vector + analyzer + function
)

type GenFieldsOption struct {
	AutoID         bool // is auto id
	Dim            int64
	IsDynamic      bool
	MaxLength      int64 // varchar len or array capacity
	MaxCapacity    int64
	IsPartitionKey bool
	EnableAnalyzer bool
	AnalyzerParams map[string]any
	ElementType    entity.FieldType
}

func TNewFieldsOption() *GenFieldsOption {
	return &GenFieldsOption{
		AutoID:         false,
		Dim:            common.DefaultDim,
		MaxLength:      common.TestMaxLen,
		MaxCapacity:    common.TestCapacity,
		IsDynamic:      false,
		IsPartitionKey: false,
		EnableAnalyzer: false,
		AnalyzerParams: make(map[string]any),
		ElementType:    entity.FieldTypeNone,
	}
}

func (opt *GenFieldsOption) TWithAutoID(autoID bool) *GenFieldsOption {
	opt.AutoID = autoID
	return opt
}

func (opt *GenFieldsOption) TWithDim(dim int64) *GenFieldsOption {
	opt.Dim = dim
	return opt
}

func (opt *GenFieldsOption) TWithIsDynamic(isDynamic bool) *GenFieldsOption {
	opt.IsDynamic = isDynamic
	return opt
}

func (opt *GenFieldsOption) TWithIsPartitionKey(isPartitionKey bool) *GenFieldsOption {
	opt.IsPartitionKey = isPartitionKey
	return opt
}

func (opt *GenFieldsOption) TWithElementType(elementType entity.FieldType) *GenFieldsOption {
	opt.ElementType = elementType
	return opt
}

func (opt *GenFieldsOption) TWithMaxLen(maxLen int64) *GenFieldsOption {
	opt.MaxLength = maxLen
	return opt
}

func (opt *GenFieldsOption) TWithMaxCapacity(maxCapacity int64) *GenFieldsOption {
	opt.MaxCapacity = maxCapacity
	return opt
}

func (opt *GenFieldsOption) TWithEnableAnalyzer(enableAnalyzer bool) *GenFieldsOption {
	opt.EnableAnalyzer = enableAnalyzer
	return opt
}

func (opt *GenFieldsOption) TWithAnalyzerParams(analyzerParams map[string]any) *GenFieldsOption {
	opt.AnalyzerParams = analyzerParams
	return opt
}

// factory
type FieldsFactory struct{}

// product
type CollectionFields interface {
	GenFields(opts GenFieldsOption) []*entity.Field
}

type FieldsInt64Vec struct{}

func (cf FieldsInt64Vec) GenFields(option GenFieldsOption) []*entity.Field {
	pkField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeInt64)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeFloatVector)).WithDataType(entity.FieldTypeFloatVector).WithDim(option.Dim)
	if option.AutoID {
		pkField.WithIsAutoID(option.AutoID)
	}
	return []*entity.Field{pkField, vecField}
}

type FieldsVarcharBinary struct{}

func (cf FieldsVarcharBinary) GenFields(option GenFieldsOption) []*entity.Field {
	pkField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeVarChar)).WithDataType(entity.FieldTypeVarChar).
		WithIsPrimaryKey(true).WithMaxLength(option.MaxLength)
	vecField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeBinaryVector)).WithDataType(entity.FieldTypeBinaryVector).WithDim(option.Dim)
	if option.AutoID {
		pkField.WithIsAutoID(option.AutoID)
	}
	return []*entity.Field{pkField, vecField}
}

type FieldsInt64VecJSON struct{}

func (cf FieldsInt64VecJSON) GenFields(option GenFieldsOption) []*entity.Field {
	pkField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeInt64)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeFloatVector)).WithDataType(entity.FieldTypeFloatVector).WithDim(option.Dim)
	jsonField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeJSON)).WithDataType(entity.FieldTypeJSON)
	if option.AutoID {
		pkField.WithIsAutoID(option.AutoID)
	}
	return []*entity.Field{pkField, vecField, jsonField}
}

type FieldsInt64VecArray struct{}

func (cf FieldsInt64VecArray) GenFields(option GenFieldsOption) []*entity.Field {
	pkField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeInt64)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeFloatVector)).WithDataType(entity.FieldTypeFloatVector).WithDim(option.Dim)
	fields := []*entity.Field{
		pkField, vecField,
	}
	for _, eleType := range GetAllArrayElementType() {
		arrayField := entity.NewField().WithName(GetFieldNameByElementType(eleType)).WithDataType(entity.FieldTypeArray).WithElementType(eleType).WithMaxCapacity(option.MaxCapacity)
		if eleType == entity.FieldTypeVarChar {
			arrayField.WithMaxLength(option.MaxLength)
		}
		fields = append(fields, arrayField)
	}
	if option.AutoID {
		pkField.WithIsAutoID(option.AutoID)
	}
	return fields
}

type FieldsInt64VarcharSparseVec struct{}

func (cf FieldsInt64VarcharSparseVec) GenFields(option GenFieldsOption) []*entity.Field {
	pkField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeInt64)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	varcharField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeVarChar)).WithDataType(entity.FieldTypeVarChar).WithMaxLength(option.MaxLength)
	sparseVecField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeSparseVector)).WithDataType(entity.FieldTypeSparseVector)

	if option.AutoID {
		pkField.WithIsAutoID(option.AutoID)
	}
	return []*entity.Field{pkField, varcharField, sparseVecField}
}

type FieldsInt64MultiVec struct{}

func (cf FieldsInt64MultiVec) GenFields(option GenFieldsOption) []*entity.Field {
	pkField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeInt64)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	fields := []*entity.Field{
		pkField,
	}
	for _, fieldType := range GetAllVectorFieldType() {
		if fieldType == entity.FieldTypeSparseVector {
			continue
		}
		vecField := entity.NewField().WithName(GetFieldNameByFieldType(fieldType)).WithDataType(fieldType).WithDim(option.Dim)
		fields = append(fields, vecField)
	}

	if option.AutoID {
		pkField.WithIsAutoID(option.AutoID)
	}
	return fields
}

type FieldsAllFields struct{} // except sparse vector field
func (cf FieldsAllFields) GenFields(option GenFieldsOption) []*entity.Field {
	pkField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeInt64)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	fields := []*entity.Field{
		pkField,
	}
	// scalar fields and array fields
	for _, fieldType := range GetAllScalarFieldType() {
		switch fieldType {
		case entity.FieldTypeInt64:
			continue
		case entity.FieldTypeArray:
			for _, eleType := range GetAllArrayElementType() {
				arrayField := entity.NewField().WithName(GetFieldNameByElementType(eleType)).WithDataType(entity.FieldTypeArray).WithElementType(eleType).WithMaxCapacity(option.MaxCapacity)
				if eleType == entity.FieldTypeVarChar {
					arrayField.WithMaxLength(option.MaxLength)
				}
				fields = append(fields, arrayField)
			}
		case entity.FieldTypeVarChar:
			varcharField := entity.NewField().WithName(GetFieldNameByFieldType(fieldType)).WithDataType(fieldType).WithMaxLength(option.MaxLength)
			fields = append(fields, varcharField)
		default:
			scalarField := entity.NewField().WithName(GetFieldNameByFieldType(fieldType)).WithDataType(fieldType)
			fields = append(fields, scalarField)
		}
	}
	for _, fieldType := range GetAllVectorFieldType() {
		if fieldType == entity.FieldTypeSparseVector {
			continue
		}
		vecField := entity.NewField().WithName(GetFieldNameByFieldType(fieldType)).WithDataType(fieldType).WithDim(option.Dim)
		fields = append(fields, vecField)
	}

	if option.AutoID {
		pkField.WithIsAutoID(option.AutoID)
	}
	return fields
}

type FieldsInt64VecAllScalar struct{} // except sparse vector field
func (cf FieldsInt64VecAllScalar) GenFields(option GenFieldsOption) []*entity.Field {
	pkField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeInt64)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	fields := []*entity.Field{
		pkField,
	}
	// scalar fields and array fields
	for _, fieldType := range GetAllScalarFieldType() {
		switch fieldType {
		case entity.FieldTypeInt64:
			continue
		case entity.FieldTypeArray:
			for _, eleType := range GetAllArrayElementType() {
				arrayField := entity.NewField().WithName(GetFieldNameByElementType(eleType)).WithDataType(entity.FieldTypeArray).WithElementType(eleType).WithMaxCapacity(option.MaxCapacity)
				if eleType == entity.FieldTypeVarChar {
					arrayField.WithMaxLength(option.MaxLength)
				}
				fields = append(fields, arrayField)
			}
		case entity.FieldTypeVarChar:
			varcharField := entity.NewField().WithName(GetFieldNameByFieldType(fieldType)).WithDataType(fieldType).WithMaxLength(option.MaxLength)
			fields = append(fields, varcharField)
		default:
			scalarField := entity.NewField().WithName(GetFieldNameByFieldType(fieldType)).WithDataType(fieldType)
			fields = append(fields, scalarField)
		}
	}
	vecField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeFloatVector)).WithDataType(entity.FieldTypeFloatVector).WithDim(option.Dim)
	fields = append(fields, vecField)

	if option.AutoID {
		pkField.WithIsAutoID(option.AutoID)
	}
	return fields
}

type FieldsFullTextSearch struct{}

func (cf FieldsFullTextSearch) GenFields(option GenFieldsOption) []*entity.Field {
	pkField := entity.NewField().WithName(GetFieldNameByFieldType(entity.FieldTypeInt64)).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	textField := entity.NewField().WithName(common.DefaultTextFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(option.MaxLength).WithIsPartitionKey(option.IsPartitionKey).WithEnableAnalyzer(true).WithAnalyzerParams(option.AnalyzerParams)
	sparseVecField := entity.NewField().WithName(common.DefaultTextSparseVecFieldName).WithDataType(entity.FieldTypeSparseVector)
	if option.AutoID {
		pkField.WithIsAutoID(option.AutoID)
	}
	fields := []*entity.Field{
		pkField,
		textField,
		sparseVecField,
	}
	return fields
}

func (ff FieldsFactory) GenFieldsForCollection(collectionFieldsType CollectionFieldsType, option *GenFieldsOption) []*entity.Field {
	log.Info("GenFieldsForCollection", zap.Any("GenFieldsOption", option))
	switch collectionFieldsType {
	case Int64Vec:
		return FieldsInt64Vec{}.GenFields(*option)
	case VarcharBinary:
		return FieldsVarcharBinary{}.GenFields(*option)
	case Int64VecJSON:
		return FieldsInt64VecJSON{}.GenFields(*option)
	case Int64VecArray:
		return FieldsInt64VecArray{}.GenFields(*option)
	case Int64VarcharSparseVec:
		return FieldsInt64VarcharSparseVec{}.GenFields(*option)
	case Int64MultiVec:
		return FieldsInt64MultiVec{}.GenFields(*option)
	case AllFields:
		return FieldsAllFields{}.GenFields(*option)
	case Int64VecAllScalar:
		return FieldsInt64VecAllScalar{}.GenFields(*option)
	case FullTextSearch:
		return FieldsFullTextSearch{}.GenFields(*option)
	default:
		return FieldsInt64Vec{}.GenFields(*option)
	}
}
