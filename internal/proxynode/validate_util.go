package proxynode

import (
	"strconv"
	"strings"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func isAlpha(c uint8) bool {
	if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') {
		return false
	}
	return true
}

func isNumber(c uint8) bool {
	if c < '0' || c > '9' {
		return false
	}
	return true
}

func ValidateCollectionName(collName string) error {
	collName = strings.TrimSpace(collName)

	if collName == "" {
		return errors.New("Collection name should not be empty")
	}

	invalidMsg := "Invalid collection name: " + collName + ". "
	if int64(len(collName)) > Params.MaxNameLength() {
		msg := invalidMsg + "The length of a collection name must be less than " +
			strconv.FormatInt(Params.MaxNameLength(), 10) + " characters."
		return errors.New(msg)
	}

	firstChar := collName[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		msg := invalidMsg + "The first character of a collection name must be an underscore or letter."
		return errors.New(msg)
	}

	for i := 1; i < len(collName); i++ {
		c := collName[i]
		if c != '_' && c != '$' && !isAlpha(c) && !isNumber(c) {
			msg := invalidMsg + "Collection name can only contain numbers, letters, dollars and underscores."
			return errors.New(msg)
		}
	}
	return nil
}

func ValidatePartitionTag(partitionTag string, strictCheck bool) error {
	partitionTag = strings.TrimSpace(partitionTag)

	invalidMsg := "Invalid partition tag: " + partitionTag + ". "
	if partitionTag == "" {
		msg := invalidMsg + "Partition tag should not be empty."
		return errors.New(msg)
	}

	if int64(len(partitionTag)) > Params.MaxNameLength() {
		msg := invalidMsg + "The length of a partition tag must be less than " +
			strconv.FormatInt(Params.MaxNameLength(), 10) + " characters."
		return errors.New(msg)
	}

	if strictCheck {
		firstChar := partitionTag[0]
		if firstChar != '_' && !isAlpha(firstChar) && !isNumber(firstChar) {
			msg := invalidMsg + "The first character of a partition tag must be an underscore or letter."
			return errors.New(msg)
		}

		tagSize := len(partitionTag)
		for i := 1; i < tagSize; i++ {
			c := partitionTag[i]
			if c != '_' && c != '$' && !isAlpha(c) && !isNumber(c) {
				msg := invalidMsg + "Partition tag can only contain numbers, letters, dollars and underscores."
				return errors.New(msg)
			}
		}
	}

	return nil
}

func ValidateFieldName(fieldName string) error {
	fieldName = strings.TrimSpace(fieldName)

	if fieldName == "" {
		return errors.New("Field name should not be empty")
	}

	invalidMsg := "Invalid field name: " + fieldName + ". "
	if int64(len(fieldName)) > Params.MaxNameLength() {
		msg := invalidMsg + "The length of a field name must be less than " +
			strconv.FormatInt(Params.MaxNameLength(), 10) + " characters."
		return errors.New(msg)
	}

	firstChar := fieldName[0]
	if firstChar != '_' && !isAlpha(firstChar) {
		msg := invalidMsg + "The first character of a field name must be an underscore or letter."
		return errors.New(msg)
	}

	fieldNameSize := len(fieldName)
	for i := 1; i < fieldNameSize; i++ {
		c := fieldName[i]
		if c != '_' && !isAlpha(c) && !isNumber(c) {
			msg := invalidMsg + "Field name cannot only contain numbers, letters, and underscores."
			return errors.New(msg)
		}
	}
	return nil
}

func ValidateDimension(dim int64, isBinary bool) error {
	if dim <= 0 || dim > Params.MaxDimension() {
		return errors.New("invalid dimension: " + strconv.FormatInt(dim, 10) + ". should be in range 1 ~ " +
			strconv.FormatInt(Params.MaxDimension(), 10) + ".")
	}
	if isBinary && dim%8 != 0 {
		return errors.New("invalid dimension: " + strconv.FormatInt(dim, 10) + ". should be multiple of 8.")
	}
	return nil
}

func ValidateVectorFieldMetricType(field *schemapb.FieldSchema) error {
	if (field.DataType != schemapb.DataType_VECTOR_FLOAT) && (field.DataType != schemapb.DataType_VECTOR_BINARY) {
		return nil
	}
	for _, params := range field.IndexParams {
		if params.Key == "metric_type" {
			return nil
		}
	}
	return errors.New("vector float without metric_type")
}

func ValidateDuplicatedFieldName(fields []*schemapb.FieldSchema) error {
	names := make(map[string]bool)
	for _, field := range fields {
		_, ok := names[field.Name]
		if ok {
			return errors.New("duplicated filed name")
		}
		names[field.Name] = true
	}
	return nil
}

func ValidatePrimaryKey(coll *schemapb.CollectionSchema) error {
	//no primary key for auto id
	if coll.AutoID {
		for _, field := range coll.Fields {
			if field.IsPrimaryKey {
				return errors.Errorf("collection %s is auto id, so filed %s should not defined as primary key", coll.Name, field.Name)
			}
		}
		return nil
	}
	idx := -1
	for i, field := range coll.Fields {
		if field.IsPrimaryKey {
			if idx != -1 {
				return errors.Errorf("there are more than one primary key, filed name = %s, %s", coll.Fields[idx].Name, field.Name)
			}
			if field.DataType != schemapb.DataType_INT64 {
				return errors.Errorf("the data type of primary key should be int64")
			}
			idx = i
		}
	}
	if idx == -1 {
		return errors.Errorf("primay key is undefined")
	}
	return nil
}
