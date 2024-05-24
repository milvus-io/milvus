package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"time"

	milvusclient "github.com/milvus-io/milvus/client/v2"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
)

var cmd = flag.String("cmd", helloMilvusCmd, "command to run")

const (
	helloMilvusCmd = `hello_milvus`
	partitionsCmd  = `partitions`
	indexCmd       = `indexes`
	countCmd       = `count`

	milvusAddr     = `localhost:19530`
	nEntities, dim = 3000, 128
	collectionName = "hello_milvus"

	msgFmt                         = "==== %s ====\n"
	idCol, randomCol, embeddingCol = "ID", "random", "embeddings"
	topK                           = 3
)

func main() {
	flag.Parse()

	switch *cmd {
	case helloMilvusCmd:
		HelloMilvus()
	case partitionsCmd:
		Partitions()
	case indexCmd:
		Indexes()
	case countCmd:
		Count()
	}
}

func Count() {
	ctx := context.Background()

	collectionName := "hello_count_inverted"

	c, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: "127.0.0.1:19530",
	})
	if err != nil {
		log.Fatal("failed to connect to milvus, err: ", err.Error())
	}

	schema := entity.NewSchema().WithName(collectionName).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsAutoID(true).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(128))

	err = c.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema))
	if err != nil {
		log.Fatal("failed to connect to milvus, err: ", err.Error())
	}

	indexTask, err := c.CreateIndex(ctx, milvusclient.NewCreateIndexOption(collectionName, "id", index.NewGenericIndex("inverted", map[string]string{})))
	if err != nil {
		log.Fatal("failed to connect to milvus, err: ", err.Error())
	}

	indexTask.Await(ctx)

	indexTask, err = c.CreateIndex(ctx, milvusclient.NewCreateIndexOption(collectionName, "vector", index.NewHNSWIndex(entity.L2, 16, 32)))
	if err != nil {
		log.Fatal("failed to connect to milvus, err: ", err.Error())
	}

	indexTask.Await(ctx)

	loadTask, err := c.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(collectionName))
	if err != nil {
		log.Fatal("faied to load collection, err: ", err.Error())
	}
	loadTask.Await(ctx)

	for i := 0; i < 100; i++ {
		// randomData := make([]int64, 0, nEntities)
		vectorData := make([][]float32, 0, nEntities)
		// generate data
		for i := 0; i < nEntities; i++ {
			// randomData = append(randomData, rand.Int63n(1000))
			vec := make([]float32, 0, dim)
			for j := 0; j < dim; j++ {
				vec = append(vec, rand.Float32())
			}
			vectorData = append(vectorData, vec)
		}

		_, err = c.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName).WithFloatVectorColumn("vector", dim, vectorData))
		if err != nil {
			log.Fatal("failed to insert data")
		}

		log.Println("start flush collection")
		flushTask, err := c.Flush(ctx, milvusclient.NewFlushOption(collectionName))
		if err != nil {
			log.Fatal("failed to flush", err.Error())
		}
		start := time.Now()
		err = flushTask.Await(ctx)
		if err != nil {
			log.Fatal("failed to flush", err.Error())
		}
		log.Println("flush done, elapsed", time.Since(start))

		result, err := c.Query(ctx, milvusclient.NewQueryOption(collectionName).
			WithOutputFields([]string{"count(*)"}).
			WithConsistencyLevel(entity.ClStrong))
		if err != nil {
			log.Fatal("failed to connect to milvus, err: ", err.Error())
		}
		for _, rs := range result.Fields {
			log.Println(rs)
		}
		result, err = c.Query(ctx, milvusclient.NewQueryOption(collectionName).
			WithOutputFields([]string{"count(*)"}).
			WithFilter("id > 0").
			WithConsistencyLevel(entity.ClStrong))
		if err != nil {
			log.Fatal("failed to connect to milvus, err: ", err.Error())
		}
		for _, rs := range result.Fields {
			log.Println(rs)
		}
	}

	// err = c.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	// if err != nil {
	// 	log.Fatal("=== Failed to drop collection", err.Error())
	// }
}

func HelloMilvus() {
	ctx := context.Background()

	log.Printf(msgFmt, "start connecting to Milvus")
	c, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		log.Fatal("failed to connect to milvus, err: ", err.Error())
	}
	defer c.Close(ctx)

	if has, err := c.HasCollection(ctx, milvusclient.NewHasCollectionOption(collectionName)); err != nil {
		log.Fatal("failed to check collection exists or not", err.Error())
	} else if has {
		c.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	}

	err = c.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions(collectionName, dim).WithVarcharPK(true, 128))
	if err != nil {
		log.Fatal("failed to create collection", err.Error())
	}

	collections, err := c.ListCollections(ctx, milvusclient.NewListCollectionOption())
	if err != nil {
		log.Fatal("failed to list collections,", err.Error())
	}

	for _, collectionName := range collections {
		collection, err := c.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(collectionName))
		if err != nil {
			log.Fatal(err.Error())
		}
		log.Println(collection.Name)
		for _, field := range collection.Schema.Fields {
			log.Println("=== Field: ", field.Name, field.DataType, field.AutoID)
		}
	}

	// randomData := make([]int64, 0, nEntities)
	vectorData := make([][]float32, 0, nEntities)
	// generate data
	for i := 0; i < nEntities; i++ {
		// randomData = append(randomData, rand.Int63n(1000))
		vec := make([]float32, 0, dim)
		for j := 0; j < dim; j++ {
			vec = append(vec, rand.Float32())
		}
		vectorData = append(vectorData, vec)
	}

	_, err = c.Insert(ctx, milvusclient.NewColumnBasedInsertOption(collectionName).WithFloatVectorColumn("vector", dim, vectorData))
	if err != nil {
		log.Fatal("failed to insert data")
	}

	log.Println("start flush collection")
	flushTask, err := c.Flush(ctx, milvusclient.NewFlushOption(collectionName))
	if err != nil {
		log.Fatal("failed to flush", err.Error())
	}
	start := time.Now()
	err = flushTask.Await(ctx)
	if err != nil {
		log.Fatal("failed to flush", err.Error())
	}
	log.Println("flush done, elapsed", time.Since(start))

	vec2search := []entity.Vector{
		entity.FloatVector(vectorData[len(vectorData)-2]),
		entity.FloatVector(vectorData[len(vectorData)-1]),
	}

	resultSets, err := c.Search(ctx, milvusclient.NewSearchOption(collectionName, 3, vec2search).WithConsistencyLevel(entity.ClEventually))
	if err != nil {
		log.Fatal("failed to search collection", err.Error())
	}
	for _, resultSet := range resultSets {
		for i := 0; i < resultSet.ResultCount; i++ {
			log.Print(resultSet.IDs.Get(i))
		}
		log.Println()
	}

	err = c.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	if err != nil {
		log.Fatal("=== Failed to drop collection", err.Error())
	}
}

func Partitions() {
	ctx := context.Background()

	log.Printf(msgFmt, "start connecting to Milvus")
	c, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		log.Fatal("failed to connect to milvus, err: ", err.Error())
	}
	defer c.Close(ctx)

	has, err := c.HasCollection(ctx, milvusclient.NewHasCollectionOption(collectionName))
	if err != nil {
		log.Fatal(err)
	}
	if has {
		c.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	}

	err = c.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions(collectionName, dim))
	if err != nil {
		log.Fatal("failed to create collection", err.Error())
	}

	partitions, err := c.ListPartitions(ctx, milvusclient.NewListPartitionOption(collectionName))
	if err != nil {
		log.Fatal("failed to create collection", err.Error())
	}

	for _, partitionName := range partitions {
		err := c.DropPartition(ctx, milvusclient.NewDropPartitionOption(collectionName, partitionName))
		if err != nil {
			log.Println(err.Error())
		}
	}

	c.CreatePartition(ctx, milvusclient.NewCreatePartitionOption(collectionName, "new_partition"))
	partitions, err = c.ListPartitions(ctx, milvusclient.NewListPartitionOption(collectionName))
	if err != nil {
		log.Fatal("failed to create collection", err.Error())
	}
	log.Println(partitions)

	err = c.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	if err != nil {
		log.Fatal("=== Failed to drop collection", err.Error())
	}
}

func Indexes() {
	ctx := context.Background()

	log.Printf(msgFmt, "start connecting to Milvus")
	c, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		log.Fatal("failed to connect to milvus, err: ", err.Error())
	}
	defer c.Close(ctx)

	has, err := c.HasCollection(ctx, milvusclient.NewHasCollectionOption(collectionName))
	if err != nil {
		log.Fatal(err)
	}
	if has {
		c.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	}

	err = c.CreateCollection(ctx, milvusclient.SimpleCreateCollectionOptions(collectionName, dim))
	if err != nil {
		log.Fatal("failed to create collection", err.Error())
	}

	index := index.NewHNSWIndex(entity.COSINE, 16, 64)

	createIdxOpt := milvusclient.NewCreateIndexOption(collectionName, "vector", index)
	task, err := c.CreateIndex(ctx, createIdxOpt)
	if err != nil {
		log.Fatal("failed to create index", err.Error())
	}
	task.Await(ctx)

	indexes, err := c.ListIndexes(ctx, milvusclient.NewListIndexOption(collectionName))
	if err != nil {
		log.Fatal("failed to list indexes", err.Error())
	}
	for _, indexName := range indexes {
		log.Println(indexName)
	}
}
