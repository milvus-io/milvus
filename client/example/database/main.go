package main

import (
	"context"
	"log"

	milvusclient "github.com/milvus-io/milvus/client/v2"
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
}
