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
	"log"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
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

func ExampleClient_Search_jsonExpr() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		log.Fatal("failed to connect to milvus server: ", err.Error())
	}

	defer cli.Close(ctx)

	queryVector := []float32{0.3, -0.6, -0.1}

	annParam := index.NewCustomAnnParam()
	annParam.WithExtraParam("nprobe", 10)
	resultSets, err := cli.Search(ctx, milvusclient.NewSearchOption(
		"my_json_collection", // collectionName
		5,                    // limit
		[]entity.Vector{entity.FloatVector(queryVector)},
	).WithOutputFields("metadata").WithAnnParam(annParam))
	if err != nil {
		log.Fatal("failed to perform basic ANN search collection: ", err.Error())
	}

	for _, resultSet := range resultSets {
		log.Println("IDs: ", resultSet.IDs)
		log.Println("Scores: ", resultSet.Scores)
	}
}

func ExampleClient_Search_binaryVector() {
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

	queryVector := []byte{0b10011011, 0b01010100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	annSearchParams := index.NewCustomAnnParam()
	annSearchParams.WithExtraParam("nprobe", 10)
	resultSets, err := cli.Search(ctx, milvusclient.NewSearchOption(
		"my_binary_collection", // collectionName
		5,                      // limit
		[]entity.Vector{entity.BinaryVector(queryVector)},
	).WithOutputFields("pk").WithAnnParam(annSearchParams))
	if err != nil {
		log.Fatal("failed to perform basic ANN search collection: ", err.Error())
	}

	for _, resultSet := range resultSets {
		log.Println("IDs: ", resultSet.IDs)
		log.Println("Scores: ", resultSet.Scores)
		log.Println("Pks: ", resultSet.GetColumn("pk"))
	}
}

func ExampleClient_Get() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	milvusAddr := "127.0.0.1:19530"

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		log.Fatal("failed to connect to milvus server: ", err.Error())
	}

	defer cli.Close(ctx)

	rs, err := cli.Get(ctx, milvusclient.NewQueryOption("quick_setup").
		WithIDs(column.NewColumnInt64("id", []int64{1, 2, 3})))
	if err != nil {
		// handle error
	}

	fmt.Println(rs.GetColumn("id"))
}

func ExampleClient_Query() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	milvusAddr := "127.0.0.1:19530"

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		log.Fatal("failed to connect to milvus server: ", err.Error())
	}

	defer cli.Close(ctx)

	rs, err := cli.Query(ctx, milvusclient.NewQueryOption("quick_setup").
		WithFilter("emb_type == 3").
		WithOutputFields("id", "emb_type"))
	if err != nil {
		// handle error
	}

	fmt.Println(rs.GetColumn("id"))
}

func ExampleClient_Query_jsonExpr_notnull() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		log.Fatal("failed to connect to milvus server: ", err.Error())
	}

	defer cli.Close(ctx)

	rs, err := cli.Query(ctx, milvusclient.NewQueryOption("my_json_collection").
		WithFilter("metadata is not null").
		WithOutputFields("metadata", "pk"))
	if err != nil {
		// handle error
	}

	fmt.Println(rs.GetColumn("pk"))
	fmt.Println(rs.GetColumn("metadata"))
}

func ExampleClient_Query_jsonExpr_leafChild() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		log.Fatal("failed to connect to milvus server: ", err.Error())
	}

	defer cli.Close(ctx)

	rs, err := cli.Query(ctx, milvusclient.NewQueryOption("my_json_collection").
		WithFilter(`metadata["product_info"]["category"] == "electronics"`).
		WithOutputFields("metadata", "pk"))
	if err != nil {
		// handle error
	}

	fmt.Println(rs.GetColumn("pk"))
	fmt.Println(rs.GetColumn("metadata"))
}

func ExampleClient_HybridSearch() {
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
	sparseVector, _ := entity.NewSliceSparseEmbedding([]uint32{1, 21, 100}, []float32{0.1, 0.2, 0.3})

	resultSets, err := cli.HybridSearch(ctx, milvusclient.NewHybridSearchOption(
		"quick_setup",
		3,
		milvusclient.NewAnnRequest("dense_vector", 10, entity.FloatVector(queryVector)),
		milvusclient.NewAnnRequest("sparse_vector", 10, sparseVector),
	).WithReranker(milvusclient.NewRRFReranker()))
	if err != nil {
		log.Fatal("failed to perform basic ANN search collection: ", err.Error())
	}

	for _, resultSet := range resultSets {
		log.Println("IDs: ", resultSet.IDs)
		log.Println("Scores: ", resultSet.Scores)
	}
}

func ExampleClient_Search_textMatch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := "text_min_match"
	titleField := "title"
	textField := "document_text"
	titleSparse := "title_sparse_vector"
	textSparse := "text_sparse_vector"
	milvusAddr := "127.0.0.1:19530"

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		log.Fatal("failed to connect to milvus server: ", err.Error())
	}
	defer cli.Close(ctx)

	_ = cli.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))

	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)).
		WithField(entity.NewField().WithName(titleField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(512).WithEnableAnalyzer(true).WithEnableMatch(true)).
		WithField(entity.NewField().WithName(textField).WithDataType(entity.FieldTypeVarChar).WithMaxLength(2048).WithEnableAnalyzer(true).WithEnableMatch(true)).
		WithField(entity.NewField().WithName(titleSparse).WithDataType(entity.FieldTypeSparseVector)).
		WithField(entity.NewField().WithName(textSparse).WithDataType(entity.FieldTypeSparseVector)).
		WithFunction(entity.NewFunction().WithName("title_bm25_func").WithType(entity.FunctionTypeBM25).WithInputFields(titleField).WithOutputFields(titleSparse)).
		WithFunction(entity.NewFunction().WithName("text_bm25_func").WithType(entity.FunctionTypeBM25).WithInputFields(textField).WithOutputFields(textSparse))

	idxOpts := []milvusclient.CreateIndexOption{
		milvusclient.NewCreateIndexOption(collectionName, titleField, index.NewInvertedIndex()),
		milvusclient.NewCreateIndexOption(collectionName, textField, index.NewInvertedIndex()),
		milvusclient.NewCreateIndexOption(collectionName, titleSparse, index.NewSparseInvertedIndex(entity.BM25, 0.2)),
		milvusclient.NewCreateIndexOption(collectionName, textSparse, index.NewSparseInvertedIndex(entity.BM25, 0.2)),
	}

	err = cli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema).WithIndexOptions(idxOpts...))
	if err != nil {
		log.Fatal("failed to create collection: ", err.Error())
	}

	_, err = cli.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName).
		WithVarcharColumn(titleField, []string{
			"History of AI",
			"Alan Turing Biography",
			"Machine Learning Overview",
		}).
		WithVarcharColumn(textField, []string{
			"Artificial intelligence was founded in 1956 by computer scientists.",
			"Alan Turing proposed early concepts of AI and machine learning.",
			"Machine learning is a subset of artificial intelligence.",
		}))
	if err != nil {
		log.Fatal("failed to insert data: ", err.Error())
	}

	task, err := cli.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(collectionName))
	if err != nil {
		log.Fatal("failed to load collection: ", err.Error())
	}
	_ = task.Await(ctx)

	q := "artificial intelligence"
	expr := "text_match(" + titleField + ", \"" + q + "\", minimum_should_match=2) OR text_match(" + textField + ", \"" + q + "\", minimum_should_match=2)"

	boost := entity.NewFunction().
		WithName("title_boost").
		WithType(entity.FunctionTypeRerank).
		WithParam("reranker", "boost").
		WithParam("filter", "text_match("+titleField+", \""+q+"\", minimum_should_match=2)").
		WithParam("weight", "2.0")

	vectors := []entity.Vector{entity.Text(q)}
	rs, err := cli.Search(ctx, milvusclient.NewSearchOption(collectionName, 5, vectors).
		WithANNSField(textSparse).
		WithFilter(expr).
		WithOutputFields("id", titleField, textField).
		WithFunctionReranker(boost))
	if err != nil {
		log.Fatal("failed to search: ", err.Error())
	}

	for _, r := range rs {
		_ = r.ResultCount
	}
}
