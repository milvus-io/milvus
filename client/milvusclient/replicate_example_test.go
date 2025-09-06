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
	"log"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

const (
	exampleMilvusAddr = `127.0.0.1:19530`
)

func ExampleClient_UpdateReplicateConfiguration() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: exampleMilvusAddr,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Use builder pattern to create cluster configuration (chained calls)
	sourceCluster := milvusclient.NewMilvusClusterBuilder("source-cluster").
		WithURI("localhost:19530").
		WithToken("source-token").
		WithPchannels("source-channel-1", "source-channel-2").
		Build()

	targetCluster := milvusclient.NewMilvusClusterBuilder("target-cluster").
		WithURI("localhost:19531").
		WithToken("target-token").
		WithPchannels("target-channel-1", "target-channel-2").
		Build()

	// Use builder pattern to build replicate configuration
	config := milvusclient.NewReplicateConfigurationBuilder().
		WithCluster(sourceCluster).
		WithCluster(targetCluster).
		WithTopology("source-cluster", "target-cluster").
		Build()

	// Update replicate configuration
	err = cli.UpdateReplicateConfiguration(ctx, config)
	if err != nil {
		log.Printf("Failed to update replicate configuration: %v", err)
		return
	}

	log.Println("Replicate configuration updated successfully")
}

func ExampleClient_GetReplicateInfo() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: exampleMilvusAddr,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Get replicate information for a specific source cluster
	resp, err := cli.GetReplicateInfo(ctx, "source-cluster")
	if err != nil {
		log.Printf("Failed to get replicate information: %v", err)
		return
	}

	log.Printf("Replicate information retrieved successfully, checkpoint count: %d", len(resp.GetCheckpoints()))
}

func ExampleClient_CreateReplicateStream() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: exampleMilvusAddr,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Create replicate stream
	stream, err := cli.CreateReplicateStream(ctx)
	if err != nil {
		log.Printf("Failed to create replicate stream: %v", err)
		return
	}
	defer stream.CloseSend()

	log.Println("Replicate stream created successfully")

	// Here you can continue to use the stream for data transmission
	// For example: stream.Send(&milvuspb.ReplicateMessage{...})
}
