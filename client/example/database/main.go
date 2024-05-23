package main

import (
	"context"
	"log"

	milvusclient "github.com/milvus-io/milvus/client/v2"
	"github.com/milvus-io/milvus/client/v2/entity"
)

const (
	milvusAddr     = `localhost:19530`
	nEntities, dim = 3000, 128
	collectionName = "hello_milvus"

	msgFmt                         = "==== %s ====\n"
	idCol, randomCol, embeddingCol = "ID", "random", "embeddings"
	topK                           = 3
)

func main() {
	ctx := context.Background()

	log.Printf(msgFmt, "start connecting to Milvus")
	c, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		log.Fatal("failed to connect to milvus, err: ", err.Error())
	}
	defer c.Close(ctx)

	dbNames, err := c.ListDatabase(ctx, milvusclient.NewListDatabaseOption())
	if err != nil {
		log.Fatal("failed to list databases", err.Error())
	}
	log.Println("=== Databases: ", dbNames)

	schema := entity.NewSchema().WithName("hello_milvus").
		WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("Vector").WithDataType(entity.FieldTypeFloatVector).WithDim(128))

	if err := c.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("hello_milvus", schema)); err != nil {
		log.Fatal("failed to create collection:", err.Error())
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

	c.CreateDatabase(ctx, milvusclient.NewCreateDatabaseOption("test"))
	c.UsingDatabase(ctx, milvusclient.NewUsingDatabaseOption("test"))

	schema = entity.NewSchema().WithName("hello_milvus").
		WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("Vector").WithDataType(entity.FieldTypeFloatVector).WithDim(128))

	if err := c.CreateCollection(ctx, milvusclient.NewCreateCollectionOption("hello_milvus", schema)); err != nil {
		log.Fatal("failed to create collection:", err.Error())
	}

	collections, err = c.ListCollections(ctx, milvusclient.NewListCollectionOption())
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
}
