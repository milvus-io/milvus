// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func CheckVarcharLength(str string, maxLength int64, field *schemapb.FieldSchema) error {
	if (int64)(len(str)) > maxLength {
		return fmt.Errorf("value length(%d) for field %s exceeds max_length(%d)", len(str), field.GetName(), maxLength)
	}
	return nil
}

func CheckArrayCapacity(arrLength int, maxCapacity int64, field *schemapb.FieldSchema) error {
	if (int64)(arrLength) > maxCapacity {
		return fmt.Errorf("array capacity(%d) for field %s exceeds max_capacity(%d)", arrLength, field.GetName(), maxCapacity)
	}
	return nil
}

func EstimateReadCountPerBatch(bufferSize int, schema *schemapb.CollectionSchema) (int64, error) {
	sizePerRecord, err := typeutil.EstimateMaxSizePerRecord(schema)
	if err != nil {
		return 0, err
	}
	if sizePerRecord <= 0 || bufferSize <= 0 {
		return 0, fmt.Errorf("invalid size, sizePerRecord=%d, bufferSize=%d", sizePerRecord, bufferSize)
	}
	if 1000*sizePerRecord <= bufferSize {
		return 1000, nil
	}
	ret := int64(bufferSize) / int64(sizePerRecord)
	if ret <= 0 {
		return 1, nil
	}
	return ret, nil
}

// SafeStringForError safely converts a string for use in error messages.
// It replaces invalid UTF-8 sequences with their hex representation to avoid
// gRPC serialization errors while still providing useful debugging information.
func SafeStringForError(s string) string {
	if utf8.ValidString(s) {
		return s
	}

	var result strings.Builder
	for i, r := range s {
		if r == utf8.RuneError {
			// Invalid UTF-8 sequence, encode as hex
			result.WriteString(fmt.Sprintf("\\x%02x", s[i]))
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// SafeStringForErrorWithLimit safely converts a string for use in error messages
// with a length limit to prevent extremely long error messages.
func SafeStringForErrorWithLimit(s string, maxLen int) string {
	safe := SafeStringForError(s)
	if len(safe) <= maxLen {
		return safe
	}
	return safe[:maxLen] + "..."
}

func CheckValidUTF8(s string, field *schemapb.FieldSchema) error {
	if !typeutil.IsUTF8(s) {
		// Use safe string representation to avoid gRPC serialization errors
		safeValue := SafeStringForErrorWithLimit(s, 100)
		return fmt.Errorf("field '%s' contains invalid UTF-8 data, value=%s", field.GetName(), safeValue)
	}
	return nil
}

func CheckValidString(s string, maxLength int64, field *schemapb.FieldSchema) error {
	if err := CheckValidUTF8(s, field); err != nil {
		return err
	}
	if err := CheckVarcharLength(s, maxLength, field); err != nil {
		return err
	}
	return nil
}

// GetSchemaTimezone retrieves the timezone string from the CollectionSchema's properties.
// It falls back to common.DefaultTimezone if the key is not found or the value is empty.
func GetSchemaTimezone(schema *schemapb.CollectionSchema) string {
	// 1. Attempt to retrieve the timezone value from the schema's properties.
	// We assume funcutil.TryGetAttrByKeyFromRepeatedKV returns the value and a boolean indicating existence.
	// If the key is not found, the returned timezone string will be the zero value ("").
	timezone, _ := funcutil.TryGetAttrByKeyFromRepeatedKV(common.TimezoneKey, schema.GetProperties())

	// 2. If the retrieved value is empty, use the system default timezone.
	if timezone == "" {
		timezone = common.DefaultTimezone
	}

	return timezone
}
