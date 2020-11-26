package proxy

import (
	"strconv"
	"strings"

	"github.com/zilliztech/milvus-distributed/internal/errors"
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
