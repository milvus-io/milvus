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

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/milvusclient"
)

func ExampleClient_Session() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	milvusAddr := "127.0.0.1:19530"
	token := "root:Milvus"

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
		APIKey:  token,
	})
	if err != nil {
		log.Fatal("failed to connect to milvus server: ", err.Error())
	}
	defer cli.Close(ctx)

	// Pin DQL operations to a target cluster of a global-cluster deployment.
	// Every search/query/hybrid-search/iterator request issued through the
	// session carries the cluster id in its request params.
	session := cli.Session("cluster-a")

	queryVector := []float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835}

	resultSets, err := session.Search(ctx, milvusclient.NewSearchOption(
		"quick_setup", // collectionName
		3,             // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	))
	if err != nil {
		log.Fatal("failed to perform session search: ", err.Error())
	}

	for _, resultSet := range resultSets {
		log.Println("IDs: ", resultSet.IDs)
	}
}
