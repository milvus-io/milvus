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
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func ExampleClient_CreateResourceGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle error
	}

	defer cli.Close(ctx)

	err = cli.CreateResourceGroup(ctx, milvusclient.NewCreateResourceGroupOption("my_rg"))
	if err != nil {
		// handle error
	}
}

func ExampleClient_UpdateResourceGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle error
	}

	defer cli.Close(ctx)

	err = cli.UpdateResourceGroup(ctx, milvusclient.NewUpdateResourceGroupOption("my_rg", &entity.ResourceGroupConfig{
		Requests: entity.ResourceGroupLimit{NodeNum: 10},
		Limits:   entity.ResourceGroupLimit{NodeNum: 10},
		NodeFilter: entity.ResourceGroupNodeFilter{
			NodeLabels: map[string]string{"my_label1": "a"},
		},
	}))
	if err != nil {
		// handle error
	}
}

func ExampleClient_TransferReplica() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle error
	}

	defer cli.Close(ctx)

	err = cli.TransferReplica(ctx, milvusclient.NewTransferReplicaOption("quick_setup", "rg_1", "rg_2", 1))
	if err != nil {
		// handle error
	}
}

func ExampleClient_DropResourceGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle error
	}

	defer cli.Close(ctx)

	err = cli.DropResourceGroup(ctx, milvusclient.NewDropResourceGroupOption("my_rg"))
	if err != nil {
		// handle error
	}
}

func ExampleClient_DescribeResourceGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle error
	}

	defer cli.Close(ctx)

	rg, err := cli.DescribeResourceGroup(ctx, milvusclient.NewDescribeResourceGroupOption("my_rg"))
	if err != nil {
		// handle error
	}
	fmt.Println(rg)
}

func ExampleClient_ListResourceGroups() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle error
	}

	defer cli.Close(ctx)

	rgNames, err := cli.ListResourceGroups(ctx, milvusclient.NewListResourceGroupsOption())
	if err != nil {
		// handle error
	}
	fmt.Println(rgNames)
}
