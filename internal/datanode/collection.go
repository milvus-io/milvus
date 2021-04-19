// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datanode

import (
	"errors"

	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type Collection struct {
	schema *schemapb.CollectionSchema
	id     UniqueID
}

func (c *Collection) GetName() string {
	if c.schema == nil {
		return ""
	}
	return c.schema.Name
}

func (c *Collection) GetID() UniqueID {
	return c.id
}

func (c *Collection) GetSchema() *schemapb.CollectionSchema {
	return c.schema
}

func newCollection(collectionID UniqueID, schema *schemapb.CollectionSchema) (*Collection, error) {
	if schema == nil {
		return nil, errors.New("invalid schema")
	}

	var newCollection = &Collection{
		schema: schema,
		id:     collectionID,
	}
	return newCollection, nil
}
