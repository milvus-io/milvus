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

// nolint
package milvusclient_test

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func ExampleClient_CreateIndex() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	index := index.NewHNSWIndex(entity.COSINE, 32, 128)
	indexTask, err := cli.CreateIndex(ctx, milvusclient.NewCreateIndexOption("my_collection", "vector", index))
	if err != nil {
		// handler err
	}

	err = indexTask.Await(ctx)
	if err != nil {
		// handler err
	}
}

func ExampleClient_CreateIndex_jsonPathIndex_dynamicField() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	jsonPathIndex := index.NewJSONPathIndex(index.Inverted,
		"varchar", // cast type
		"color",   // json path
	)
	indexTask, err := cli.CreateIndex(ctx, milvusclient.NewCreateIndexOption("my_dynamic_collection", "color", jsonPathIndex))
	if err != nil {
		// handler err
	}

	err = indexTask.Await(ctx)
	if err != nil {
		// handler err
	}
}

func ExampleClient_DescribeIndex() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	indexInfo, err := cli.DescribeIndex(ctx, milvusclient.NewDescribeIndexOption("my_collection", "my_index"))
	if err != nil {
		// handle err
	}
	fmt.Println(indexInfo)
}

func ExampleClient_DropIndex() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	err = cli.DropIndex(ctx, milvusclient.NewDropIndexOption("my_collection", "my_index"))
	if err != nil {
		// handle err
	}
}

func ExampleClient_ListIndexes() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	indexes, err := cli.ListIndexes(ctx, milvusclient.NewListIndexOption("my_collection").WithFieldName("my_vector"))
	if err != nil {
		// handle err
	}
	fmt.Println(indexes)
}
