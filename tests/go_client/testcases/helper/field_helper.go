package helper

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

/*
Field-level option system usage examples:

// usage: Set different options for multiple fields
fieldOpts := TNewFieldOptions().
	WithFieldOption("pk", TNewFieldsOption().TWithAutoID(true)).
	WithFieldOption("floatVec", TNewFieldsOption().TWithDim(512)).
	WithFieldOption("varchar", TNewFieldsOption().TWithMaxLen(500).TWithNullable(true))

// Use when creating collections
cp := NewCreateCollectionParams(Int64Vec)
_, schema := CollPrepare.CreateCollectionWithFieldOptions(ctx, t, mc, cp, fieldOpts, TNewSchemaOption())
*/

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
	case entity.FieldTypeGeometry:
		return common.DefaultGeometryFieldName
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
	Int64Vec              CollectionFieldsType = 1  // int64 + floatVec
	VarcharBinary         CollectionFieldsType = 2  // varchar + binaryVec
	Int64VecJSON          CollectionFieldsType = 3  // int64 + floatVec + json
	Int64VecArray         CollectionFieldsType = 4  // int64 + floatVec + array
	Int64VarcharSparseVec CollectionFieldsType = 5  // int64 + varchar + sparse vector
	Int64MultiVec         CollectionFieldsType = 6  // int64 + floatVec + binaryVec + fp16Vec + bf16vec
	AllFields             CollectionFieldsType = 7  // all fields excepted sparse
	Int64VecAllScalar     CollectionFieldsType = 8  // int64 + floatVec + all scalar fields
	FullTextSearch        CollectionFieldsType = 9  // int64 + varchar + sparse vector + analyzer + function
	TextEmbedding         CollectionFieldsType = 10 // int64 + varchar + float_vector + text_embedding_function
	Int64VecGeometry      CollectionFieldsType = 11 // int64 + floatVec + geometry
)

type GenFieldsOption struct {
	AutoID          bool // is auto id
	Dim             int64
	IsDynamic       bool
	MaxLength       int64 // varchar len or array capacity
	MaxCapacity     int64
	IsPartitionKey  bool
	IsClusteringKey bool
	EnableAnalyzer  bool
	EnableMatch     bool
	AnalyzerParams  map[string]any
	ElementType     entity.FieldType
	Nullable        bool
	DefaultValue    interface{}
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
		Nullable:       false,
		DefaultValue:   nil,
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

func (opt *GenFieldsOption) TWithIsClusteringKey(isClusteringKey bool) *GenFieldsOption {
	opt.IsClusteringKey = isClusteringKey
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

func (opt *GenFieldsOption) TWithEnableMatch(enableMatch bool) *GenFieldsOption {
	opt.EnableMatch = enableMatch
	return opt
}

func (opt *GenFieldsOption) TWithAnalyzerParams(analyzerParams map[string]any) *GenFieldsOption {
	opt.AnalyzerParams = analyzerParams
	return opt
}

func (opt *GenFieldsOption) TWithNullable(nullable bool) *GenFieldsOption {
	opt.Nullable = nullable
	return opt
}

func (opt *GenFieldsOption) TWithDefaultValue(defaultValue interface{}) *GenFieldsOption {
	opt.DefaultValue = defaultValue
	return opt
}

// Field-level option system
type FieldOption struct {
	FieldName string
	Options   *GenFieldsOption
}

type FieldOptions []FieldOption

func TNewFieldOptions() FieldOptions {
	return make(FieldOptions, 0)
}

func (fos FieldOptions) WithFieldOption(fieldName string, options *GenFieldsOption) FieldOptions {
	return append(fos, FieldOption{
		FieldName: fieldName,
		Options:   options,
	})
}

func (fos FieldOptions) GetFieldOption(fieldName string) *GenFieldsOption {
	for _, fo := range fos {
		if fo.FieldName == fieldName {
			return fo.Options
		}
	}
	return nil
}

// factory
type FieldsFactory struct{}

// Redesign: Create field combinations based on CollectionFieldsType and set properties based on FieldOptions
func (ff FieldsFactory) GenFieldsForCollection(collectionFieldsType CollectionFieldsType, fieldOpts FieldOptions) []*entity.Field {
	log.Info("GenFieldsForCollectionWithOptions", zap.Any("CollectionFieldsType", collectionFieldsType), zap.Any("FieldOptions", fieldOpts))

	switch collectionFieldsType {
	case Int64Vec:
		return ff.createInt64VecFields(fieldOpts)
	case VarcharBinary:
		return ff.createVarcharBinaryFields(fieldOpts)
	case Int64VecJSON:
		return ff.createInt64VecJSONFields(fieldOpts)
	case Int64VecArray:
		return ff.createInt64VecArrayFields(fieldOpts)
	case Int64VarcharSparseVec:
		return ff.createInt64VarcharSparseVecFields(fieldOpts)
	case Int64MultiVec:
		return ff.createInt64MultiVecFields(fieldOpts)
	case AllFields:
		return ff.createAllFields(fieldOpts)
	case Int64VecAllScalar:
		return ff.createInt64VecAllScalarFields(fieldOpts)
	case FullTextSearch:
		return ff.createFullTextSearchFields(fieldOpts)
	case TextEmbedding:
		return ff.createTextEmbeddingFields(fieldOpts)
	case Int64VecGeometry:
		return ff.createInt64VecGeometryFields(fieldOpts)
	default:
		return ff.createInt64VecFields(fieldOpts)
	}
}

// Create Int64Vec field combination
func (ff FieldsFactory) createInt64VecFields(fieldOpts FieldOptions) []*entity.Field {
	// pkName := GetFieldNameByFieldType(entity.FieldTypeInt64)
	pkName := GetFieldNameByFieldType(entity.FieldTypeInt64)
	vecName := GetFieldNameByFieldType(entity.FieldTypeFloatVector)

	// Create base fields
	pkField := entity.NewField().WithName(pkName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(vecName).WithDataType(entity.FieldTypeFloatVector)

	// Apply field options
	ff.applyFieldOptions(pkField, fieldOpts.GetFieldOption(pkName))
	ff.applyFieldOptions(vecField, fieldOpts.GetFieldOption(vecName))

	return []*entity.Field{pkField, vecField}
}

// Create VarcharBinary field combination
func (ff FieldsFactory) createVarcharBinaryFields(fieldOpts FieldOptions) []*entity.Field {
	pkName := GetFieldNameByFieldType(entity.FieldTypeVarChar)
	vecName := GetFieldNameByFieldType(entity.FieldTypeBinaryVector)

	// Create base fields
	pkField := entity.NewField().WithName(pkName).WithDataType(entity.FieldTypeVarChar).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(vecName).WithDataType(entity.FieldTypeBinaryVector)

	// Apply field options
	ff.applyFieldOptions(pkField, fieldOpts.GetFieldOption(pkName))
	ff.applyFieldOptions(vecField, fieldOpts.GetFieldOption(vecName))

	return []*entity.Field{pkField, vecField}
}

// Create Int64VecJSON field combination
func (ff FieldsFactory) createInt64VecJSONFields(fieldOpts FieldOptions) []*entity.Field {
	pkName := GetFieldNameByFieldType(entity.FieldTypeInt64)
	vecName := GetFieldNameByFieldType(entity.FieldTypeFloatVector)
	jsonName := GetFieldNameByFieldType(entity.FieldTypeJSON)

	// Create base fields
	pkField := entity.NewField().WithName(pkName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(vecName).WithDataType(entity.FieldTypeFloatVector)
	jsonField := entity.NewField().WithName(jsonName).WithDataType(entity.FieldTypeJSON)

	// Apply field options
	ff.applyFieldOptions(pkField, fieldOpts.GetFieldOption(pkName))
	ff.applyFieldOptions(vecField, fieldOpts.GetFieldOption(vecName))
	ff.applyFieldOptions(jsonField, fieldOpts.GetFieldOption(jsonName))

	return []*entity.Field{pkField, vecField, jsonField}
}

// Create Int64VecArray field combination
func (ff FieldsFactory) createInt64VecArrayFields(fieldOpts FieldOptions) []*entity.Field {
	pkName := GetFieldNameByFieldType(entity.FieldTypeInt64)
	vecName := GetFieldNameByFieldType(entity.FieldTypeFloatVector)

	// Create base fields
	pkField := entity.NewField().WithName(pkName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(vecName).WithDataType(entity.FieldTypeFloatVector)
	fields := []*entity.Field{pkField, vecField}

	// Add array fields
	for _, eleType := range GetAllArrayElementType() {
		arrayName := GetFieldNameByElementType(eleType)
		arrayField := entity.NewField().WithName(arrayName).WithDataType(entity.FieldTypeArray).WithElementType(eleType)

		// Apply field options
		ff.applyFieldOptions(arrayField, fieldOpts.GetFieldOption(arrayName))
		fields = append(fields, arrayField)
	}

	// Apply primary key and vector field options
	ff.applyFieldOptions(pkField, fieldOpts.GetFieldOption(pkName))
	ff.applyFieldOptions(vecField, fieldOpts.GetFieldOption(vecName))

	return fields
}

// Create Int64VarcharSparseVec field combination
func (ff FieldsFactory) createInt64VarcharSparseVecFields(fieldOpts FieldOptions) []*entity.Field {
	pkName := GetFieldNameByFieldType(entity.FieldTypeInt64)
	varcharName := GetFieldNameByFieldType(entity.FieldTypeVarChar)
	sparseName := GetFieldNameByFieldType(entity.FieldTypeSparseVector)

	// Create base fields
	pkField := entity.NewField().WithName(pkName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	varcharField := entity.NewField().WithName(varcharName).WithDataType(entity.FieldTypeVarChar)
	sparseVecField := entity.NewField().WithName(sparseName).WithDataType(entity.FieldTypeSparseVector)

	// Apply field options
	ff.applyFieldOptions(pkField, fieldOpts.GetFieldOption(pkName))
	ff.applyFieldOptions(varcharField, fieldOpts.GetFieldOption(varcharName))
	ff.applyFieldOptions(sparseVecField, fieldOpts.GetFieldOption(sparseName))

	return []*entity.Field{pkField, varcharField, sparseVecField}
}

func (ff FieldsFactory) createInt64VecGeometryFields(fieldOpts FieldOptions) []*entity.Field {
	pkName := GetFieldNameByFieldType(entity.FieldTypeInt64)
	vecName := GetFieldNameByFieldType(entity.FieldTypeFloatVector)
	geometryName := GetFieldNameByFieldType(entity.FieldTypeGeometry)

	// Create base fields
	pkField := entity.NewField().WithName(pkName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	vecField := entity.NewField().WithName(vecName).WithDataType(entity.FieldTypeFloatVector)
	geometryField := entity.NewField().WithName(geometryName).WithDataType(entity.FieldTypeGeometry)

	// Apply field options
	ff.applyFieldOptions(pkField, fieldOpts.GetFieldOption(pkName))
	ff.applyFieldOptions(vecField, fieldOpts.GetFieldOption(vecName))
	ff.applyFieldOptions(geometryField, fieldOpts.GetFieldOption(geometryName))

	return []*entity.Field{pkField, vecField, geometryField}
}

// Create Int64MultiVec field combination
func (ff FieldsFactory) createInt64MultiVecFields(fieldOpts FieldOptions) []*entity.Field {
	pkName := GetFieldNameByFieldType(entity.FieldTypeInt64)

	// Create primary key field
	pkField := entity.NewField().WithName(pkName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	fields := []*entity.Field{pkField}

	// Add all vector fields
	for _, fieldType := range GetAllVectorFieldType() {
		if fieldType == entity.FieldTypeSparseVector {
			continue
		}
		vecName := GetFieldNameByFieldType(fieldType)
		vecField := entity.NewField().WithName(vecName).WithDataType(fieldType)

		// Apply field options
		ff.applyFieldOptions(vecField, fieldOpts.GetFieldOption(vecName))
		fields = append(fields, vecField)
	}

	// Apply primary key field options
	ff.applyFieldOptions(pkField, fieldOpts.GetFieldOption(pkName))

	return fields
}

// Create AllFields field combination
func (ff FieldsFactory) createAllFields(fieldOpts FieldOptions) []*entity.Field {
	pkName := GetFieldNameByFieldType(entity.FieldTypeInt64)

	// Create primary key field
	pkField := entity.NewField().WithName(pkName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	fields := []*entity.Field{pkField}

	// Add scalar fields and array fields
	for _, fieldType := range GetAllScalarFieldType() {
		switch fieldType {
		case entity.FieldTypeInt64:
			continue
		case entity.FieldTypeArray:
			for _, eleType := range GetAllArrayElementType() {
				arrayName := GetFieldNameByElementType(eleType)
				arrayField := entity.NewField().WithName(arrayName).WithDataType(entity.FieldTypeArray).WithElementType(eleType)

				// Apply field options
				ff.applyFieldOptions(arrayField, fieldOpts.GetFieldOption(arrayName))
				fields = append(fields, arrayField)
			}
		default:
			scalarName := GetFieldNameByFieldType(fieldType)
			scalarField := entity.NewField().WithName(scalarName).WithDataType(fieldType)

			// Apply field options
			ff.applyFieldOptions(scalarField, fieldOpts.GetFieldOption(scalarName))
			fields = append(fields, scalarField)
		}
	}

	// Add vector fields
	for _, fieldType := range GetAllVectorFieldType() {
		if fieldType == entity.FieldTypeSparseVector {
			continue
		}
		vecName := GetFieldNameByFieldType(fieldType)
		vecField := entity.NewField().WithName(vecName).WithDataType(fieldType)

		// Apply field options
		ff.applyFieldOptions(vecField, fieldOpts.GetFieldOption(vecName))
		fields = append(fields, vecField)
	}

	// Apply primary key field options
	ff.applyFieldOptions(pkField, fieldOpts.GetFieldOption(pkName))

	return fields
}

// Create Int64VecAllScalar field combination
func (ff FieldsFactory) createInt64VecAllScalarFields(fieldOpts FieldOptions) []*entity.Field {
	pkName := GetFieldNameByFieldType(entity.FieldTypeInt64)
	vecName := GetFieldNameByFieldType(entity.FieldTypeFloatVector)

	// Create primary key field
	pkField := entity.NewField().WithName(pkName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	fields := []*entity.Field{pkField}

	// Add scalar fields and array fields
	for _, fieldType := range GetAllScalarFieldType() {
		switch fieldType {
		case entity.FieldTypeInt64:
			continue
		case entity.FieldTypeArray:
			for _, eleType := range GetAllArrayElementType() {
				arrayName := GetFieldNameByElementType(eleType)
				arrayField := entity.NewField().WithName(arrayName).WithDataType(entity.FieldTypeArray).WithElementType(eleType)

				// Apply field options
				ff.applyFieldOptions(arrayField, fieldOpts.GetFieldOption(arrayName))
				fields = append(fields, arrayField)
			}
		default:
			scalarName := GetFieldNameByFieldType(fieldType)
			scalarField := entity.NewField().WithName(scalarName).WithDataType(fieldType)

			// Apply field options
			ff.applyFieldOptions(scalarField, fieldOpts.GetFieldOption(scalarName))
			fields = append(fields, scalarField)
		}
	}

	// Add vector field
	vecField := entity.NewField().WithName(vecName).WithDataType(entity.FieldTypeFloatVector)
	ff.applyFieldOptions(vecField, fieldOpts.GetFieldOption(vecName))
	fields = append(fields, vecField)

	// Apply primary key field options
	ff.applyFieldOptions(pkField, fieldOpts.GetFieldOption(pkName))

	return fields
}

// Create FullTextSearch field combination
func (ff FieldsFactory) createFullTextSearchFields(fieldOpts FieldOptions) []*entity.Field {
	pkName := GetFieldNameByFieldType(entity.FieldTypeInt64)
	textName := common.DefaultTextFieldName
	sparseName := common.DefaultTextSparseVecFieldName

	// Create base fields
	pkField := entity.NewField().WithName(pkName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	textField := entity.NewField().WithName(textName).WithDataType(entity.FieldTypeVarChar).WithEnableAnalyzer(true).WithEnableMatch(true)
	sparseVecField := entity.NewField().WithName(sparseName).WithDataType(entity.FieldTypeSparseVector)

	// Apply field options
	ff.applyFieldOptions(pkField, fieldOpts.GetFieldOption(pkName))
	ff.applyFieldOptions(textField, fieldOpts.GetFieldOption(textName))
	ff.applyFieldOptions(sparseVecField, fieldOpts.GetFieldOption(sparseName))

	return []*entity.Field{pkField, textField, sparseVecField}
}

// Create TextEmbedding field combination
func (ff FieldsFactory) createTextEmbeddingFields(fieldOpts FieldOptions) []*entity.Field {
	pkName := GetFieldNameByFieldType(entity.FieldTypeInt64)
	textFieldName := "document"
	denseFieldName := "dense"

	// Create base fields
	pkField := entity.NewField().WithName(pkName).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)
	textField := entity.NewField().WithName(textFieldName).WithDataType(entity.FieldTypeVarChar).WithMaxLength(fieldOpts.GetFieldOption(textFieldName).MaxLength)
	vecField := entity.NewField().WithName(denseFieldName).WithDataType(entity.FieldTypeFloatVector).WithDim(fieldOpts.GetFieldOption(denseFieldName).Dim)

	// Apply field options
	ff.applyFieldOptions(pkField, fieldOpts.GetFieldOption(pkName))
	ff.applyFieldOptions(textField, fieldOpts.GetFieldOption(textFieldName))
	ff.applyFieldOptions(vecField, fieldOpts.GetFieldOption(denseFieldName))

	return []*entity.Field{pkField, textField, vecField}
}

// Apply field options to entity.Field
func (ff FieldsFactory) applyFieldOptions(field *entity.Field, fieldOpt *GenFieldsOption) {
	// Use default options if no field options provided
	if fieldOpt == nil {
		fieldOpt = TNewFieldsOption()
	}

	// WithDim
	if fieldOpt.Dim > 0 && (field.DataType == entity.FieldTypeFloatVector ||
		field.DataType == entity.FieldTypeBinaryVector ||
		field.DataType == entity.FieldTypeFloat16Vector ||
		field.DataType == entity.FieldTypeBFloat16Vector) {
		field.WithDim(fieldOpt.Dim)
	}

	// WithMaxLength
	if fieldOpt.MaxLength > 0 && (field.DataType == entity.FieldTypeVarChar ||
		(field.DataType == entity.FieldTypeArray && field.ElementType == entity.FieldTypeVarChar)) {
		field.WithMaxLength(fieldOpt.MaxLength)
	}

	// WithMaxCapacity
	if fieldOpt.MaxCapacity > 0 && field.DataType == entity.FieldTypeArray {
		field.WithMaxCapacity(fieldOpt.MaxCapacity)
	}

	// WithIsPartitionKey
	if fieldOpt.IsPartitionKey {
		field.WithIsPartitionKey(true)
	}

	// WithAnalyzerParams
	if fieldOpt.EnableAnalyzer {
		field.WithEnableAnalyzer(true)
	}
	if fieldOpt.EnableMatch {
		field.WithEnableMatch(true)
	}
	if len(fieldOpt.AnalyzerParams) > 0 {
		field.WithAnalyzerParams(fieldOpt.AnalyzerParams)
	}

	// WithNullable
	if fieldOpt.Nullable {
		field.WithNullable(true)
	}

	// WithIsAutoID
	if field.PrimaryKey && fieldOpt.AutoID {
		field.WithIsAutoID(true)
	}

	// WithDefaultValue
	if fieldOpt.DefaultValue != nil {
		ff.applyDefaultValue(field, fieldOpt.DefaultValue)
	}
}

// WithDefaultValue
// Note: When using TWithDefaultValue, you must explicitly specify the data type
// For example: TWithDefaultValue(int32(2)) instead of TWithDefaultValue(2)
// This is because Go's interface{} type inference defaults to int, not int32
func (ff FieldsFactory) applyDefaultValue(field *entity.Field, defaultValue interface{}) {
	log.Info("applyDefaultValue", zap.Any("defaultValue", defaultValue), zap.String("fieldType", field.DataType.String()))
	switch field.DataType {
	case entity.FieldTypeBool:
		if val, ok := defaultValue.(bool); ok {
			field.WithDefaultValueBool(val)
		} else {
			log.Fatal("applyDefaultValue failed: type assertion failed",
				zap.Any("defaultValue", defaultValue),
				zap.String("expectedType", "bool"),
				zap.String("actualType", fmt.Sprintf("%T", defaultValue)))
		}
	case entity.FieldTypeInt8, entity.FieldTypeInt16, entity.FieldTypeInt32:
		// int8, int16, int32 are all converted to int32
		var val int32
		if v, ok := defaultValue.(int32); ok {
			val = v
			field.WithDefaultValueInt(val)
		} else if v, ok := defaultValue.(int16); ok {
			val = int32(v)
			field.WithDefaultValueInt(val)
		} else if v, ok := defaultValue.(int8); ok {
			val = int32(v)
			field.WithDefaultValueInt(val)
		} else {
			log.Fatal("applyDefaultValue failed: type assertion failed",
				zap.Any("defaultValue", defaultValue),
				zap.String("expectedType", "int8 or int16 or int32"),
				zap.String("actualType", fmt.Sprintf("%T", defaultValue)))
		}
	case entity.FieldTypeInt64:
		if val, ok := defaultValue.(int64); ok {
			field.WithDefaultValueLong(val)
		} else {
			log.Fatal("applyDefaultValue failed: type assertion failed",
				zap.Any("defaultValue", defaultValue),
				zap.String("expectedType", "int64"),
				zap.String("actualType", fmt.Sprintf("%T", defaultValue)))
		}
	case entity.FieldTypeFloat:
		if val, ok := defaultValue.(float32); ok {
			field.WithDefaultValueFloat(val)
		} else {
			log.Fatal("applyDefaultValue failed: type assertion failed",
				zap.Any("defaultValue", defaultValue),
				zap.String("expectedType", "float32"),
				zap.String("actualType", fmt.Sprintf("%T", defaultValue)))
		}
	case entity.FieldTypeDouble:
		if val, ok := defaultValue.(float64); ok {
			field.WithDefaultValueDouble(val)
		} else {
			log.Fatal("applyDefaultValue failed: type assertion failed",
				zap.Any("defaultValue", defaultValue),
				zap.String("expectedType", "float64"),
				zap.String("actualType", fmt.Sprintf("%T", defaultValue)))
		}
	case entity.FieldTypeVarChar:
		if val, ok := defaultValue.(string); ok {
			field.WithDefaultValueString(val)
		} else {
			log.Fatal("applyDefaultValue failed: type assertion failed",
				zap.Any("defaultValue", defaultValue),
				zap.String("expectedType", "string"),
				zap.String("actualType", fmt.Sprintf("%T", defaultValue)))
		}
	default:
		log.Fatal("applyDefaultValue: unsupported field type for default value",
			zap.String("fieldType", field.DataType.String()),
			zap.Any("defaultValue", defaultValue))
	}
}
