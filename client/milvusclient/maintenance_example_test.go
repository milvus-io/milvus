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

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func ExampleClient_GetLoadState() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `customized_setup_1`

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	loadState, err := cli.GetLoadState(ctx, milvusclient.NewGetLoadStateOption(collectionName))
	if err != nil {
		// handle err
	}
	fmt.Println(loadState)
}

func ExampleClient_RefreshLoad() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `customized_setup_1`

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	loadTask, err := cli.RefreshLoad(ctx, milvusclient.NewRefreshLoadOption(collectionName))
	if err != nil {
		// handle err
	}
	err = loadTask.Await(ctx)
	if err != nil {
		// handler err
	}
}

func ExampleClient_Compact() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `customized_setup_1`

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	compactID, err := cli.Compact(ctx, milvusclient.NewCompactOption(collectionName))
	if err != nil {
		// handle err
	}
	fmt.Println(compactID)
}

func ExampleClient_GetCompactionState() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	compactID := int64(123)

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	state, err := cli.GetCompactionState(ctx, milvusclient.NewGetCompactionStateOption(compactID))
	if err != nil {
		// handle err
	}
	fmt.Println(state)
}

func ExampleClient_Flush() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	collectionName := `customized_setup_1`

	task, err := cli.Flush(ctx, milvusclient.NewFlushOption(collectionName))
	if err != nil {
		// handle err
	}

	err = task.Await(ctx)
	if err != nil {
		// handle err
	}
}
