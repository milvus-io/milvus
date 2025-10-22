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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// MustGetSchemaFromCreateCollectionMessageBody gets the schema from the create collection request.
func MustGetSchemaFromCreateCollectionMessageBody(request *message.CreateCollectionRequest) *schemapb.CollectionSchema {
	if schema := request.GetCollectionSchema(); schema != nil {
		return schema
	}
	// compatible before 2.6.1
	schema := &schemapb.CollectionSchema{}
	if err := proto.Unmarshal(request.GetSchema(), schema); err != nil {
		panic("failed to unmarshal collection schema")
	}
	return schema
}
