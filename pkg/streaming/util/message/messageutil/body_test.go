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

package messageutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func TestMustGetSchemaFromCreateCollectionMessageBody(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  100,
				Name:     "ID",
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	request := &message.CreateCollectionRequest{
		CollectionSchema: schema,
	}
	schema2 := MustGetSchemaFromCreateCollectionMessageBody(request)
	assert.True(t, proto.Equal(schema2, schema))

	schemaBytes, err := proto.Marshal(schema)
	assert.NoError(t, err)
	request = &message.CreateCollectionRequest{
		Schema: schemaBytes,
	}
	schema2 = MustGetSchemaFromCreateCollectionMessageBody(request)
	assert.True(t, proto.Equal(schema2, schema))
}
