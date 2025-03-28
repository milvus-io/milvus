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

package numpy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/importutilv2/common"
)

func TestInvalidUTF8(t *testing.T) {
	fieldSchema := &schemapb.FieldSchema{
		FieldID:    100,
		Name:       "str",
		DataType:   schemapb.DataType_VarChar,
		TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "256"}},
	}

	data := []string{
		"aaa",
		"bbb",
		"ccc",
		"ddd",
		"\xc3\x28",
	}

	reader, err := CreateReader(data)
	assert.NoError(t, err)

	fr, err := NewFieldReader(reader, fieldSchema)
	assert.NoError(t, err)

	strs, err := fr.Next(5)
	assert.NoError(t, err)
	for _, str := range strs.([]string) {
		assert.NoError(t, common.CheckValidUTF8(str, fieldSchema))
	}
}
