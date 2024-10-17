package main

import (
	"context"
	"log"
	"math/rand"

	"github.com/samber/lo"

	milvusclient "github.com/milvus-io/milvus/client/v2"
	"github.com/milvus-io/milvus/client/v2/row"
)

type Data struct {
	ID     int64     `milvus:"name:id;primary_key;auto_id"`
	Vector []float32 `milvus:"name:vector;dim:128"`
}

const (
	milvusAddr     = `localhost:19530`
	nEntities, dim = 10, 128
	collectionName = "hello_row_base"

	msgFmt                         = "==== %s ====\n"
	idCol, randomCol, embeddingCol = "id", "random", "vector"
	topK                           = 3
)

func main() {
	schema, err := row.ParseSchema(&Data{})
	if err != nil {
		log.Fatal("failed to parse schema from struct", err.Error())
	}

	for _, field := range schema.Fields {
		log.Printf("Field name: %s, FieldType %s, IsPrimaryKey: %t", field.Name, field.DataType, field.PrimaryKey)
	}
	schema.WithName(collectionName)

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
		log.Printf("collection %s alread exists, dropping it now\n", collectionName)
		c.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
	}

	err = c.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(collectionName, schema))
	if err != nil {
		log.Fatal("failed to create collection", err.Error())
	}

	var rows []*Data
	for i := 0; i < nEntities; i++ {
		vec := make([]float32, 0, dim)
		for j := 0; j < dim; j++ {
			vec = append(vec, rand.Float32())
		}
		rows = append(rows, &Data{
			Vector: vec,
		})
	}

	insertResult, err := c.Insert(ctx, milvusclient.NewRowBasedInsertOption(collectionName, lo.Map(rows, func(data *Data, _ int) any {
		return data
	})...))
	if err != nil {
		log.Fatal("failed to insert data: ", err.Error())
	}
	log.Println(insertResult.IDs)
	for _, row := range rows {
		// id shall be written back
		log.Println(row.ID)
	}

	c.DropCollection(ctx, milvusclient.NewDropCollectionOption(collectionName))
}
