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

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func ExampleClient_Search_basic() {
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

	queryVector := []float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}

	resultSets, err := cli.Search(ctx, milvusclient.NewSearchOption(
		"quick_setup", // collectionName
		3,             // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	))
	if err != nil {
		log.Fatal("failed to perform basic ANN search collection: ", err.Error())
	}

	for _, resultSet := range resultSets {
		log.Println("IDs: ", resultSet.IDs)
		log.Println("Scores: ", resultSet.Scores)
	}
}

func ExampleClient_Search_multivectors() {
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

	queryVectors := []entity.Vector{
		entity.FloatVector([]float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}),
		entity.FloatVector([]float32{0.19886812562848388, 0.06023560599112088, 0.6976963061752597, 0.2614474506242501, 0.838729485096104}),
	}

	resultSets, err := cli.Search(ctx, milvusclient.NewSearchOption(
		"quick_setup", // collectionName
		3,             // limit
		queryVectors,
	))
	if err != nil {
		log.Fatal("failed to perform basic ANN search collection: ", err.Error())
	}

	for _, resultSet := range resultSets {
		log.Println("IDs: ", resultSet.IDs)
		log.Println("Scores: ", resultSet.Scores)
	}
}

func ExampleClient_Search_partition() {
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

	queryVector := []float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}

	resultSets, err := cli.Search(ctx, milvusclient.NewSearchOption(
		"quick_setup", // collectionName
		3,             // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithPartitions("partitionA"))
	if err != nil {
		log.Fatal("failed to perform basic ANN search collection: ", err.Error())
	}

	for _, resultSet := range resultSets {
		log.Println("IDs: ", resultSet.IDs)
		log.Println("Scores: ", resultSet.Scores)
	}
}

func ExampleClient_Search_outputFields() {
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

	queryVector := []float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}

	resultSets, err := cli.Search(ctx, milvusclient.NewSearchOption(
		"quick_setup", // collectionName
		3,             // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithOutputFields("color"))
	if err != nil {
		log.Fatal("failed to perform basic ANN search collection: ", err.Error())
	}

	for _, resultSet := range resultSets {
		log.Println("IDs: ", resultSet.IDs)
		log.Println("Scores: ", resultSet.Scores)
		log.Println("Colors: ", resultSet.GetColumn("color"))
	}
}

func ExampleClient_Search_offsetLimit() {
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

	queryVector := []float32{0.3580376395471989, -0.6023495712049978, 0.18414012509913835, -0.26286205330961354, 0.9029438446296592}

	resultSets, err := cli.Search(ctx, milvusclient.NewSearchOption(
		"quick_setup", // collectionName
		3,             // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithOffset(10))
	if err != nil {
		log.Fatal("failed to perform basic ANN search collection: ", err.Error())
	}

	for _, resultSet := range resultSets {
		log.Println("IDs: ", resultSet.IDs)
		log.Println("Scores: ", resultSet.Scores)
	}
}

// func ExampleClient_Search_useLevel() {

// }
