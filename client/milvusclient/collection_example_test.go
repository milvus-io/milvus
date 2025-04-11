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

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

const (
	milvusAddr = `127.0.0.1:19530`
)

func ExampleClient_CreateCollection_normal() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `customized_setup_1`

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	indexOptions := []milvusclient.CreateIndexOption{
		milvusclient.NewCreateIndexOption(collectionName, "my_vector", index.NewAutoIndex(entity.COSINE)).WithIndexName("my_vector"),
		milvusclient.NewCreateIndexOption(collectionName, "my_id", index.NewSortedIndex()).WithIndexName("my_id"),
	}

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	err = cli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema).
		WithIndexOptions(indexOptions...),
	)
	if err != nil {
		// handle error
	}
}

func ExampleClient_CreateCollection_quick() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `quick_setup`

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	err = cli.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions(collectionName, 5))
	if err != nil {
		// handle error
	}
}

func ExampleClient_CreateCollection_shardNum() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `customized_setup_3`

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	err = cli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema).WithShardNum(1))
	if err != nil {
		// handle error
	}
}

func ExampleClient_CreateCollection_enableMmap() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `customized_setup_4`

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	err = cli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema).WithProperty(common.MmapEnabledKey, true))
	if err != nil {
		// handle error
	}
}

func ExampleClient_CreateCollection_ttl() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `customized_setup_5`

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	err = cli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema).WithProperty(common.CollectionTTLConfigKey, 86400))
	if err != nil {
		// handle error
	}
}

func ExampleClient_CreateCollection_quickSetup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `quick_setup_1`
	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	err = cli.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions(collectionName, 512))
	if err != nil {
		// handle error
	}
}

func ExampleClient_CreateCollection_quickSetupWithIndexParams() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `quick_setup_2`
	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	err = cli.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions(collectionName, 512).WithIndexOptions(
		milvusclient.NewCreateIndexOption(collectionName, "vector", index.NewHNSWIndex(entity.L2, 64, 128)),
	))
	if err != nil {
		log.Println(err.Error())
		// handle error
	}
}

func ExampleClient_CreateCollection_quickSetupCustomize() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	err = cli.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions("custom_quick_setup", 512).
		WithPKFieldName("my_id").
		WithVarcharPK(true, 512).
		WithVectorFieldName("my_vector").
		WithMetricType(entity.L2).
		WithShardNum(5).
		WithAutoID(true),
	)
	if err != nil {
		log.Println(err.Error())
		// handle error
	}
}

func ExampleClient_CreateCollection_consistencyLevel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `customized_setup_5`

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	err = cli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema).WithConsistencyLevel(entity.ClBounded))
	if err != nil {
		// handle error
	}
}

func ExampleClient_CreateCollection_withIndexes() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `customized_setup_5`

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	schema := entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("my_id").WithIsAutoID(true).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("my_vector").WithDataType(entity.FieldTypeFloatVector).WithDim(5)).
		WithField(entity.NewField().WithName("my_varchar").WithDataType(entity.FieldTypeVarChar).WithMaxLength(512))

	idx := index.NewAutoIndex(entity.IP)
	indexOption := milvusclient.NewCreateIndexOption("my_dense_collection", "dense_vector", idx)

	err = cli.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption(collectionName, schema).
			WithIndexOptions(indexOption))
	if err != nil {
		// handle error
	}
}

func ExampleClient_CreateCollection_binaryVector() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := `my_binary_collection`

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	schema := entity.NewSchema()
	schema.WithField(entity.NewField().
		WithName("pk").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(100).
		WithIsAutoID(true),
	).WithField(entity.NewField().
		WithName("binary_vector").
		WithDataType(entity.FieldTypeBinaryVector).
		WithDim(128),
	)

	idx := index.NewAutoIndex(entity.HAMMING)
	indexOption := milvusclient.NewCreateIndexOption("my_binary_collection", "binary_vector", idx)

	err = cli.CreateCollection(ctx,
		milvusclient.NewCreateCollectionOption(collectionName, schema).
			WithIndexOptions(indexOption))
	if err != nil {
		// handle error
	}
}

func ExampleClient_CreateCollection_jsonField() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	schema := entity.NewSchema()
	schema.WithField(entity.NewField().
		WithName("pk").
		WithDataType(entity.FieldTypeInt64).
		WithIsAutoID(true),
	).WithField(entity.NewField().
		WithName("embedding").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(3),
	).WithField(entity.NewField().
		WithName("metadata").
		WithDataType(entity.FieldTypeJSON),
	)

	jsonIndex1 := index.NewJSONPathIndex(index.Inverted, "varchar", `metadata["product_info"]["category"]`)
	jsonIndex2 := index.NewJSONPathIndex(index.Inverted, "double", `metadata["price"]`)
	indexOpt1 := milvusclient.NewCreateIndexOption("my_json_collection", "meta", jsonIndex1)
	indexOpt2 := milvusclient.NewCreateIndexOption("my_json_collection", "meta", jsonIndex2)

	vectorIndex := index.NewAutoIndex(entity.COSINE)
	indexOpt := milvusclient.NewCreateIndexOption("my_json_collection", "embedding", vectorIndex)

	err = cli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("my_json_collection", schema).
		WithIndexOptions(indexOpt1, indexOpt2, indexOpt))
	if err != nil {
		// handler err
	}
}

func ExampleClient_CreateCollection_dynamicSchema() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// collectionName := `my_dynamic_collection`

	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	err = cli.CreateCollection(ctx,
		milvusclient.SimpleCreateCollectionOptions("my_dynamic_collection", 5).
			WithDynamicSchema(true))
	if err != nil {
		// handle error
	}
}

func ExampleClient_ListCollections() {
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

	collectionNames, err := cli.ListCollections(ctx, milvusclient.NewListCollectionOption())
	if err != nil {
		// handle error
	}

	fmt.Println(collectionNames)
}

func ExampleClient_DescribeCollection() {
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

	collection, err := cli.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption("quick_setup"))
	if err != nil {
		// handle error
	}

	fmt.Println(collection)
}

func ExampleClient_RenameCollection() {
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

	err = cli.RenameCollection(ctx, milvusclient.NewRenameCollectionOption("my_collection", "my_new_collection"))
	if err != nil {
		// handle error
	}
}

func ExampleClient_AlterCollectionProperties_setTTL() {
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

	err = cli.AlterCollectionProperties(ctx, milvusclient.NewAlterCollectionPropertiesOption("my_collection").WithProperty(common.CollectionTTLConfigKey, 60))
	if err != nil {
		// handle error
	}
}

func ExampleClient_LoadCollection() {
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

	loadTask, err := cli.LoadCollection(ctx, milvusclient.NewLoadCollectionOption("customized_setup_1"))
	if err != nil {
		// handle error
	}

	// sync wait collection to be loaded
	err = loadTask.Await(ctx)
	if err != nil {
		// handle error
	}
}

func ExampleClient_ReleaseCollection() {
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

	err = cli.ReleaseCollection(ctx, milvusclient.NewReleaseCollectionOption("custom_quick_setup"))
	if err != nil {
		// handle error
	}
}

func ExampleClient_DropCollection() {
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

	err = cli.DropCollection(ctx, milvusclient.NewDropCollectionOption("customized_setup_2"))
	if err != nil {
		// handle err
	}
}
