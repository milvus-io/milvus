// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/endian"
	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	"github.com/milvus-io/milvus/internal/storagev2/common/constant"
	"github.com/milvus-io/milvus/internal/storagev2/common/log"
	"github.com/milvus-io/milvus/pkg/proto/storagev2pb"
)

var ErrInvalidArgument = errors.New("invalid argument")

func ToProtobufType(dataType arrow.Type) (storagev2pb.LogicType, error) {
	typeId := int(dataType)
	if typeId < 0 || typeId >= int(storagev2pb.LogicType_MAX_ID) {
		return storagev2pb.LogicType_NA, fmt.Errorf("parse data type %v: %w", dataType, ErrInvalidArgument)
	}
	return storagev2pb.LogicType(typeId), nil
}

func ToProtobufMetadata(metadata *arrow.Metadata) (*storagev2pb.KeyValueMetadata, error) {
	keys := metadata.Keys()
	values := metadata.Values()
	return &storagev2pb.KeyValueMetadata{Keys: keys, Values: values}, nil
}

func ToProtobufDataType(dataType arrow.DataType) (*storagev2pb.DataType, error) {
	protoType := &storagev2pb.DataType{}
	err := SetTypeValues(protoType, dataType)
	if err != nil {
		return nil, err
	}
	logicType, err := ToProtobufType(dataType.ID())
	if err != nil {
		return nil, err
	}
	protoType.LogicType = logicType

	if len(GetFields(dataType)) > 0 {
		for _, field := range GetFields(dataType) {
			fieldCopy := field
			protoFieldType, err := ToProtobufField(&fieldCopy)
			if err != nil {
				return nil, err
			}
			protoType.Children = append(protoType.Children, protoFieldType)
		}
	}

	return protoType, nil
}

// GetFields TODO CHECK MORE TYPES
func GetFields(dataType arrow.DataType) []arrow.Field {
	switch dataType.ID() {
	case arrow.LIST:
		listType, _ := dataType.(*arrow.ListType)
		return listType.Fields()
	case arrow.STRUCT:
		structType, _ := dataType.(*arrow.StructType)
		return structType.Fields()
	case arrow.MAP:
		mapType, _ := dataType.(*arrow.MapType)
		return mapType.Fields()
	case arrow.FIXED_SIZE_LIST:
		listType, _ := dataType.(*arrow.FixedSizeListType)
		return listType.Fields()
	default:
		return nil
	}
}

func ToProtobufField(field *arrow.Field) (*storagev2pb.Field, error) {
	protoField := &storagev2pb.Field{}
	protoField.Name = field.Name
	protoField.Nullable = field.Nullable

	if field.Metadata.Len() != 0 {
		fieldMetadata, err := ToProtobufMetadata(&field.Metadata)
		if err != nil {
			return nil, fmt.Errorf("convert to protobuf field: %w", err)
		}
		protoField.Metadata = fieldMetadata
	}

	dataType, err := ToProtobufDataType(field.Type)
	if err != nil {
		return nil, fmt.Errorf("convert to protobuf field: %w", err)
	}
	protoField.DataType = dataType
	return protoField, nil
}

func SetTypeValues(protoType *storagev2pb.DataType, dataType arrow.DataType) error {
	switch dataType.ID() {
	case arrow.FIXED_SIZE_BINARY:
		realType, ok := dataType.(*arrow.FixedSizeBinaryType)
		if !ok {
			return fmt.Errorf("convert to fixed size binary type: %w", ErrInvalidArgument)
		}
		fixedSizeBinaryType := &storagev2pb.FixedSizeBinaryType{}
		fixedSizeBinaryType.ByteWidth = int32(realType.ByteWidth)
		protoType.TypeRelatedValues = &storagev2pb.DataType_FixedSizeBinaryType{FixedSizeBinaryType: fixedSizeBinaryType}
	case arrow.FIXED_SIZE_LIST:
		realType, ok := dataType.(*arrow.FixedSizeListType)
		if !ok {
			return fmt.Errorf("convert to fixed size list type: %w", ErrInvalidArgument)
		}
		fixedSizeListType := &storagev2pb.FixedSizeListType{}
		fixedSizeListType.ListSize = realType.Len()
		protoType.TypeRelatedValues = &storagev2pb.DataType_FixedSizeListType{FixedSizeListType: fixedSizeListType}
	case arrow.DICTIONARY:
		realType, ok := dataType.(*arrow.DictionaryType)
		if !ok {
			return fmt.Errorf("convert to dictionary type: %w", ErrInvalidArgument)
		}
		dictionaryType := &storagev2pb.DictionaryType{}
		indexType, err := ToProtobufDataType(realType.IndexType)
		if err != nil {
			return err
		}
		dictionaryType.IndexType = indexType
		valueType, err := ToProtobufDataType(realType.ValueType)
		if err != nil {
			return err
		}
		dictionaryType.ValueType = valueType
		dictionaryType.Ordered = realType.Ordered
		protoType.TypeRelatedValues = &storagev2pb.DataType_DictionaryType{DictionaryType: dictionaryType}

	case arrow.MAP:
		realType, ok := dataType.(*arrow.MapType)
		if !ok {
			return fmt.Errorf("convert to map type: %w", ErrInvalidArgument)
		}
		mapType := &storagev2pb.MapType{}
		mapType.KeysSorted = realType.KeysSorted
		protoType.TypeRelatedValues = &storagev2pb.DataType_MapType{MapType: mapType}

	default:
	}

	return nil
}

func ToProtobufSchema(schema *arrow.Schema) (*storagev2pb.ArrowSchema, error) {
	protoSchema := &storagev2pb.ArrowSchema{}
	for _, field := range schema.Fields() {
		fieldCopy := field
		protoField, err := ToProtobufField(&fieldCopy)
		if err != nil {
			return nil, err
		}
		protoSchema.Fields = append(protoSchema.Fields, protoField)
	}
	if schema.Endianness() == endian.LittleEndian {
		protoSchema.Endianness = storagev2pb.Endianness_Little
	} else if schema.Endianness() == endian.BigEndian {
		protoSchema.Endianness = storagev2pb.Endianness_Big
	}

	// TODO FIX ME: golang proto not support proto_schema->mutable_metadata()->add_keys(key);
	if schema.HasMetadata() && !schema.HasMetadata() {
		for _, key := range schema.Metadata().Keys() {
			protoKeyValue := protoSchema.GetMetadata()
			protoKeyValue.Keys = append(protoKeyValue.Keys, key)
		}
		for _, value := range schema.Metadata().Values() {
			protoKeyValue := protoSchema.GetMetadata()
			protoKeyValue.Values = append(protoKeyValue.Values, value)
		}
	}

	return protoSchema, nil
}

func FromProtobufSchema(schema *storagev2pb.ArrowSchema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, 0, len(schema.Fields))
	for _, field := range schema.Fields {
		tmp, err := FromProtobufField(field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, *tmp)
	}
	tmp, err := FromProtobufKeyValueMetadata(schema.Metadata)
	if err != nil {
		return nil, err
	}
	newSchema := arrow.NewSchema(fields, tmp)
	return newSchema, nil
}

func FromProtobufField(field *storagev2pb.Field) (*arrow.Field, error) {
	datatype, err := FromProtobufDataType(field.DataType)
	if err != nil {
		return nil, err
	}

	metadata, err := FromProtobufKeyValueMetadata(field.GetMetadata())
	if err != nil {
		return nil, err
	}

	return &arrow.Field{Name: field.Name, Type: datatype, Nullable: field.Nullable, Metadata: *metadata}, nil
}

func FromProtobufKeyValueMetadata(metadata *storagev2pb.KeyValueMetadata) (*arrow.Metadata, error) {
	keys := make([]string, 0)
	values := make([]string, 0)
	if metadata != nil {
		keys = metadata.Keys
		values = metadata.Values
	}
	newMetadata := arrow.NewMetadata(keys, values)
	return &newMetadata, nil
}

func FromProtobufDataType(dataType *storagev2pb.DataType) (arrow.DataType, error) {
	switch dataType.LogicType {
	case storagev2pb.LogicType_NA:
		return &arrow.NullType{}, nil
	case storagev2pb.LogicType_BOOL:
		return &arrow.BooleanType{}, nil
	case storagev2pb.LogicType_UINT8:
		return &arrow.Uint8Type{}, nil
	case storagev2pb.LogicType_INT8:
		return &arrow.Int8Type{}, nil
	case storagev2pb.LogicType_UINT16:
		return &arrow.Uint16Type{}, nil
	case storagev2pb.LogicType_INT16:
		return &arrow.Int16Type{}, nil
	case storagev2pb.LogicType_UINT32:
		return &arrow.Uint32Type{}, nil
	case storagev2pb.LogicType_INT32:
		return &arrow.Int32Type{}, nil
	case storagev2pb.LogicType_UINT64:
		return &arrow.Uint64Type{}, nil
	case storagev2pb.LogicType_INT64:
		return &arrow.Int64Type{}, nil
	case storagev2pb.LogicType_HALF_FLOAT:
		return &arrow.Float16Type{}, nil
	case storagev2pb.LogicType_FLOAT:
		return &arrow.Float32Type{}, nil
	case storagev2pb.LogicType_DOUBLE:
		return &arrow.Float64Type{}, nil
	case storagev2pb.LogicType_STRING:
		return &arrow.StringType{}, nil
	case storagev2pb.LogicType_BINARY:
		return &arrow.BinaryType{}, nil

	case storagev2pb.LogicType_LIST:
		fieldType, err := FromProtobufField(dataType.Children[0])
		if err != nil {
			return nil, err
		}
		listType := arrow.ListOf(fieldType.Type)
		return listType, nil

	case storagev2pb.LogicType_STRUCT:
		fields := make([]arrow.Field, 0, len(dataType.Children))
		for _, child := range dataType.Children {
			field, err := FromProtobufField(child)
			if err != nil {
				return nil, err
			}
			fields = append(fields, *field)
		}
		structType := arrow.StructOf(fields...)
		return structType, nil

	case storagev2pb.LogicType_DICTIONARY:
		keyType, err := FromProtobufField(dataType.Children[0])
		if err != nil {
			return nil, err
		}
		valueType, err := FromProtobufField(dataType.Children[1])
		if err != nil {
			return nil, err
		}
		dictType := &arrow.DictionaryType{
			IndexType: keyType.Type,
			ValueType: valueType.Type,
		}
		return dictType, nil

	case storagev2pb.LogicType_MAP:
		fieldType, err := FromProtobufField(dataType.Children[0])
		if err != nil {
			return nil, err
		}
		// TODO FIX ME
		return arrow.MapOf(fieldType.Type, fieldType.Type), nil

	case storagev2pb.LogicType_FIXED_SIZE_BINARY:

		sizeBinaryType := arrow.FixedSizeBinaryType{ByteWidth: int(dataType.GetFixedSizeBinaryType().ByteWidth)}
		return &sizeBinaryType, nil

	case storagev2pb.LogicType_FIXED_SIZE_LIST:
		fieldType, err := FromProtobufField(dataType.Children[0])
		if err != nil {
			return nil, err
		}
		fixedSizeListType := arrow.FixedSizeListOf(int32(int(dataType.GetFixedSizeListType().ListSize)), fieldType.Type)
		return fixedSizeListType, nil

	default:
		return nil, fmt.Errorf("parse protobuf datatype: %w", ErrInvalidArgument)
	}
}

func GetNewParquetFilePath(path string) string {
	scalarFileId := uuid.New()
	path = filepath.Join(path, scalarFileId.String()+constant.ParquetDataFileSuffix)
	return path
}

func GetManifestFilePath(path string, version int64) string {
	path = filepath.Join(path, constant.ManifestDir, strconv.FormatInt(version, 10)+constant.ManifestFileSuffix)
	return path
}

func GetManifestTmpFilePath(path string, version int64) string {
	path = filepath.Join(path, constant.ManifestDir, strconv.FormatInt(version, 10)+constant.ManifestTempFileSuffix)
	return path
}

func GetBlobFilePath(path string) string {
	blobId := uuid.New()
	return filepath.Join(GetBlobDir(path), blobId.String())
}

func GetManifestDir(path string) string {
	path = filepath.Join(path, constant.ManifestDir)
	return path
}

func GetVectorDataDir(path string) string {
	return filepath.Join(path, constant.VectorDataDir)
}

func GetScalarDataDir(path string) string {
	return filepath.Join(path, constant.ScalarDataDir)
}

func GetBlobDir(path string) string {
	return filepath.Join(path, constant.BlobDir)
}

func GetDeleteDataDir(path string) string {
	return filepath.Join(path, constant.DeleteDataDir)
}

func ParseVersionFromFileName(path string) int64 {
	pos := strings.Index(path, constant.ManifestFileSuffix)
	if pos == -1 || !strings.HasSuffix(path, constant.ManifestFileSuffix) {
		log.Warn("manifest file suffix not match", log.String("path", path))
		return -1
	}
	version := path[0:pos]
	versionInt, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		log.Error("parse version from file name error", log.String("path", path), log.String("version", version))
		return -1
	}
	return versionInt
}

func ProjectSchema(sc *arrow.Schema, columns []string) *arrow.Schema {
	var fields []arrow.Field
	for _, field := range sc.Fields() {
		for _, column := range columns {
			if field.Name == column {
				fields = append(fields, field)
				break
			}
		}
	}

	return arrow.NewSchema(fields, nil)
}
