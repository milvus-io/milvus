package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	_ "github.com/milvus-io/milvus/pkg/util/compressor"
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
	Run("gzip")
}

func Run(compressionName string) {
	ctx := context.Background()

	log.Printf(msgFmt, "start connecting to Milvus")
	c, err := client.NewClient(ctx, client.Config{
		Address: milvusAddr,
		DialOptions: []grpc.DialOption{grpc.WithDefaultCallOptions(
			grpc.UseCompressor(compressionName),
		)},
	})
	if err != nil {
		log.Fatal("failed to connect to milvus, err: ", err.Error())
	}
	defer c.Close()

	// delete collection if exists
	has, err := c.HasCollection(ctx, collectionName)
	if err != nil {
		log.Fatalf("failed to check collection exists, err: %v", err)
	}
	if has {
		c.DropCollection(ctx, collectionName)
	}

	// create collection
	log.Printf(msgFmt, fmt.Sprintf("create collection, `%s`", collectionName))
	schema := entity.NewSchema().WithName(collectionName).WithDescription("hello_milvus is the simplest demo to introduce the APIs").
		WithField(entity.NewField().WithName(idCol).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(false)).
		WithField(entity.NewField().WithName(randomCol).WithDataType(entity.FieldTypeDouble)).
		WithField(entity.NewField().WithName(embeddingCol).WithDataType(entity.FieldTypeFloatVector).WithDim(dim))

	if err := c.CreateCollection(ctx, schema, entity.DefaultShardNumber); err != nil { // use default shard number
		log.Fatalf("create collection failed, err: %v", err)
	}

	// insert data
	log.Printf(msgFmt, "start inserting random entities")
	idList, randomList := make([]int64, 0, nEntities), make([]float64, 0, nEntities)
	embeddingList := make([][]float32, 0, nEntities)

	rand.Seed(time.Now().UnixNano())

	// generate data
	for i := 0; i < nEntities; i++ {
		idList = append(idList, int64(i))
	}
	for i := 0; i < nEntities; i++ {
		randomList = append(randomList, rand.Float64())
	}
	for i := 0; i < nEntities; i++ {
		vec := make([]float32, 0, dim)
		for j := 0; j < dim; j++ {
			vec = append(vec, rand.Float32())
		}
		embeddingList = append(embeddingList, vec)
	}
	idColData := entity.NewColumnInt64(idCol, idList)
	randomColData := entity.NewColumnDouble(randomCol, randomList)
	embeddingColData := entity.NewColumnFloatVector(embeddingCol, dim, embeddingList)

	if _, err := c.Insert(ctx, collectionName, "", idColData, randomColData, embeddingColData); err != nil {
		log.Fatalf("failed to insert random data into `hello_milvus, err: %v", err)
	}

	if err := c.Flush(ctx, collectionName, false); err != nil {
		log.Fatalf("failed to flush data, err: %v", err)
	}

	// build index
	log.Printf(msgFmt, "start creating index IVF_FLAT")
	idx, err := entity.NewIndexIvfFlat(entity.L2, 128)
	if err != nil {
		log.Fatalf("failed to create ivf flat index, err: %v", err)
	}
	if err := c.CreateIndex(ctx, collectionName, embeddingCol, idx, false); err != nil {
		log.Fatalf("failed to create index, err: %v", err)
	}

	log.Printf(msgFmt, "start loading collection")
	err = c.LoadCollection(ctx, collectionName, false)
	if err != nil {
		log.Fatalf("failed to load collection, err: %v", err)
	}

	log.Printf(msgFmt, "start searcching based on vector similarity")
	vec2search := []entity.Vector{
		entity.FloatVector(embeddingList[len(embeddingList)-2]),
		entity.FloatVector(embeddingList[len(embeddingList)-1]),
	}
	begin := time.Now()
	sp, _ := entity.NewIndexIvfFlatSearchParam(16)
	sRet, err := c.Search(ctx, collectionName, nil, "", []string{randomCol}, vec2search,
		embeddingCol, entity.L2, topK, sp)
	end := time.Now()
	if err != nil {
		log.Fatalf("failed to search collection, err: %v", err)
	}

	log.Println("results:")
	for _, res := range sRet {
		printResult(&res)
	}
	log.Printf("\tsearch latency: %dms\n", end.Sub(begin)/time.Millisecond)

	// hybrid search
	log.Printf(msgFmt, "start hybrid searching with `random > 0.5`")
	begin = time.Now()
	sRet2, err := c.Search(ctx, collectionName, nil, "random > 0.5",
		[]string{randomCol}, vec2search, embeddingCol, entity.L2, topK, sp)
	end = time.Now()
	if err != nil {
		log.Fatalf("failed to search collection, err: %v", err)
	}
	log.Println("results:")
	for _, res := range sRet2 {
		printResult(&res)
	}
	log.Printf("\tsearch latency: %dms\n", end.Sub(begin)/time.Millisecond)

	// delete data
	log.Printf(msgFmt, "start deleting with expr ``")
	pks := entity.NewColumnInt64(idCol, []int64{0, 1})
	sRet3, err := c.QueryByPks(ctx, collectionName, nil, pks, []string{randomCol})
	if err != nil {
		log.Fatalf("failed to query result, err: %v", err)
	}
	log.Println("results:")
	idlist := make([]int64, 0)
	randList := make([]float64, 0)

	for _, col := range sRet3 {
		if col.Name() == idCol {
			idColumn := col.(*entity.ColumnInt64)
			for i := 0; i < col.Len(); i++ {
				val, err := idColumn.ValueByIdx(i)
				if err != nil {
					log.Fatal(err)
				}
				idlist = append(idlist, val)
			}
		} else {
			randColumn := col.(*entity.ColumnDouble)
			for i := 0; i < col.Len(); i++ {
				val, err := randColumn.ValueByIdx(i)
				if err != nil {
					log.Fatal(err)
				}
				randList = append(randList, val)
			}
		}
	}
	log.Printf("\tids: %#v, randoms: %#v\n", idlist, randList)

	if err := c.DeleteByPks(ctx, collectionName, "", pks); err != nil {
		log.Fatalf("failed to delete by pks, err: %v", err)
	}
	_, err = c.QueryByPks(ctx, collectionName, nil, pks, []string{randomCol}, client.WithSearchQueryConsistencyLevel(entity.ClStrong))
	if err != nil {
		log.Printf("failed to query result, err: %v", err)
	}

	// drop collection
	log.Printf(msgFmt, "drop collection `hello_milvus`")
	if err := c.DropCollection(ctx, collectionName); err != nil {
		log.Fatalf("failed to drop collection, err: %v", err)
	}
}

func printResult(sRet *client.SearchResult) {
	randoms := make([]float64, 0, sRet.ResultCount)
	scores := make([]float32, 0, sRet.ResultCount)

	var randCol *entity.ColumnDouble
	for _, field := range sRet.Fields {
		if field.Name() == randomCol {
			c, ok := field.(*entity.ColumnDouble)
			if ok {
				randCol = c
			}
		}
	}
	for i := 0; i < sRet.ResultCount; i++ {
		val, err := randCol.ValueByIdx(i)
		if err != nil {
			log.Fatal(err)
		}
		randoms = append(randoms, val)
		scores = append(scores, sRet.Scores[i])
	}
	log.Printf("\trandoms: %v, scores: %v\n", randoms, scores)
}
