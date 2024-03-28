package main

import (
	"bytes"
	"context"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/tests/go_client/compressor/deflate"
	"github.com/milvus-io/milvus/tests/go_client/compressor/invalid"
	"github.com/milvus-io/milvus/tests/go_client/compressor/klauspost"
	"github.com/milvus-io/milvus/tests/go_client/compressor/lz4"
	"github.com/milvus-io/milvus/tests/go_client/compressor/snappy"
	"github.com/milvus-io/milvus/tests/go_client/compressor/zstd"
)

var host string

type statshandler struct {
	mu     sync.Mutex
	gotRPC []*stats.OutPayload
}

func (h *statshandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return ctx
}

func (h *statshandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

func (h *statshandler) HandleConn(ctx context.Context, s stats.ConnStats) {
}

func (h *statshandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	h.mu.Lock()
	defer h.mu.Unlock()
	switch s.(type) {
	case *stats.OutPayload:
		h.gotRPC = append(h.gotRPC, s.(*stats.OutPayload))
	}
}

func init() {
	host = os.Getenv("MILVUS_SERVICE_ADDRESS")
	if host == "" {
		host = "localhost:19530"
	}
	ctx := context.Background()
	c, err := client.NewClient(ctx, client.Config{
		Address:     host,
		DialOptions: []grpc.DialOption{grpc.WithDefaultCallOptions()},
	})
	if err != nil {
		log.Fatal("failed to connect to milvus, err: ", err.Error())
	}
	defer c.Close()

	// delete collection if exists
	has, err := c.HasCollection(ctx, defaultCollectionName)
	if err != nil {
		log.Fatalf("failed to check collection exists, err: %v", err)
	}
	if has {
		c.DropCollection(ctx, defaultCollectionName)
	}

	// create collection
	schema := entity.NewSchema().WithName(defaultCollectionName).
		WithField(entity.NewField().WithName(primaryCol).WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)).
		WithField(entity.NewField().WithName(contentCol).WithDataType(entity.FieldTypeVarChar).WithMaxLength(65535)).
		WithField(entity.NewField().WithName(wordCountCol).WithDataType(entity.FieldTypeInt64)).
		WithField(entity.NewField().WithName(vectorCol).WithDataType(entity.FieldTypeFloatVector).WithDim(dim))

	if err := c.CreateCollection(ctx, schema, entity.DefaultShardNumber); err != nil { // use default shard number
		log.Fatalf("create collection failed, err: %v", err)
	}
}

type Client struct {
	client.Client
	CompressionName string
	Handler         *statshandler
}

func NewClient(ctx context.Context, compressionName string) *Client {
	hanlder := &statshandler{}
	c, err := client.NewClient(ctx, client.Config{
		Address:     host,
		DialOptions: []grpc.DialOption{grpc.WithDefaultCallOptions(grpc.UseCompressor(compressionName)), grpc.WithStatsHandler(hanlder)},
	})
	if err != nil {
		log.Fatal("failed to connect to milvus, err: ", err.Error())
		return nil
	}
	return &Client{c, compressionName, hanlder}
}

func TestCompression(t *testing.T) {
	ctx := context.Background()
	for _, compressionName := range []string{"", deflate.Name, lz4.Name, snappy.Name, zstd.Name, gzip.Name} {
		client := NewClient(ctx, compressionName)
		assert.NotNil(t, client)
		defer client.Close()
		_, err := client.ListCollections(ctx)
		assert.NoError(t, err)
	}
	client := NewClient(ctx, invalid.Name)
	assert.NotNil(t, client)
	defer client.Close()
	_, err := client.ListCollections(ctx)
	assert.Equal(t, true, strings.HasSuffix(err.Error(),
		status.Errorf(codes.Unimplemented, "grpc: Decompressor is not installed for grpc-encoding \"%s\"", invalid.Name).Error()))
}

const (
	defaultCollectionName = "book"

	primaryCol, contentCol, wordCountCol, vectorCol = "book_id", "word", "word_count", "book_intro"
	charset                                         = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

func randomString(n int) string {
	sb := strings.Builder{}
	sb.Grow(n)
	for i := 0; i < n; i++ {
		sb.WriteByte(charset[rand.Intn(len(charset))])
	}
	return sb.String()
}

func benchmarkRun(b *testing.B, compressionName string, rowsNum int, contentLength int) {
	ctx := context.Background()
	client := NewClient(ctx, compressionName)
	if client == nil {
		return
	}
	defer func() {
		client.Close()
		payloadSum := 0
		for _, payload := range client.Handler.gotRPC {
			payloadSum += payload.CompressedLength
		}
		log.Printf("compressionName: %s-%d-%d, request: %d, average length: %d", client.CompressionName, rowsNum, contentLength, len(client.Handler.gotRPC), payloadSum/len(client.Handler.gotRPC))
	}()
	// insert data
	contentList := make([]string, 0, rowsNum)
	wordCountList := make([]int64, 0, rowsNum)
	vectorList := make([][]float32, 0, rowsNum)

	rand.Seed(time.Now().UnixNano())

	// generate data
	for i := 0; i < rowsNum; i++ {
		contentList = append(contentList, randomString(contentLength))
		wordCountList = append(wordCountList, int64(i))
		vec := make([]float32, 0, dim)
		for j := 0; j < dim; j++ {
			vec = append(vec, rand.Float32())
		}
		vectorList = append(vectorList, vec)
	}
	contentColData := entity.NewColumnVarChar(contentCol, contentList)
	randomColData := entity.NewColumnInt64(wordCountCol, wordCountList)
	embeddingColData := entity.NewColumnFloatVector(vectorCol, dim, vectorList)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if _, err := client.Insert(ctx, defaultCollectionName, "", contentColData, randomColData, embeddingColData); err != nil {
			log.Fatalf("failed to insert random data into `hello_milvus, err: %v", err)
		}
	}
}

func Benchmark128MDeflate(b *testing.B) {
	benchmarkRun(b, deflate.Name, 128, 1024)
}
func Benchmark128MLz4(b *testing.B) {
	benchmarkRun(b, lz4.Name, 128, 1024)
}
func Benchmark128MSnappy(b *testing.B) {
	benchmarkRun(b, snappy.Name, 128, 1024)
}
func Benchmark128MZstd(b *testing.B) {
	benchmarkRun(b, zstd.Name, 128, 1024)
}

func Benchmark128MGzip(b *testing.B) {
	benchmarkRun(b, gzip.Name, 128, 1024)
}

func BenchmarkGzip256M(b *testing.B) {
	benchmarkRun(b, gzip.Name, 256, 1024)
}

func BenchmarkGzip512M(b *testing.B) {
	benchmarkRun(b, gzip.Name, 512, 1024)
}

func BenchmarkGzip1024M(b *testing.B) {
	benchmarkRun(b, gzip.Name, 1024, 1024)
}

func BenchmarkGzip128M4(b *testing.B) {
	benchmarkRun(b, gzip.Name, 128, 4096)
}

func BenchmarkGzip256M4(b *testing.B) {
	benchmarkRun(b, gzip.Name, 256, 4096)
}

func BenchmarkGzip512M4(b *testing.B) {
	benchmarkRun(b, gzip.Name, 512, 4096)
}

func BenchmarkGzip1024M4(b *testing.B) {
	benchmarkRun(b, gzip.Name, 1024, 4096)
}

func BenchmarkGzip128M16(b *testing.B) {
	benchmarkRun(b, gzip.Name, 128, 16384)
}

func BenchmarkGzip256M16(b *testing.B) {
	benchmarkRun(b, gzip.Name, 256, 16384)
}

func BenchmarkGzip512M16(b *testing.B) {
	benchmarkRun(b, gzip.Name, 512, 16384)
}

func BenchmarkGzip1024M16(b *testing.B) {
	benchmarkRun(b, gzip.Name, 1024, 16384)
}

func BenchmarkGzip128M32(b *testing.B) {
	benchmarkRun(b, gzip.Name, 128, 32768)
}

func BenchmarkGzip256M32(b *testing.B) {
	benchmarkRun(b, gzip.Name, 256, 32768)
}

func BenchmarkGzip512M32(b *testing.B) {
	benchmarkRun(b, gzip.Name, 512, 32768)
}

func BenchmarkGzip1024M32(b *testing.B) {
	benchmarkRun(b, gzip.Name, 1024, 32768)
}

func BenchmarkGzip128M63(b *testing.B) {
	benchmarkRun(b, gzip.Name, 128, 63488)
}

func BenchmarkGzip256M63(b *testing.B) {
	benchmarkRun(b, gzip.Name, 256, 63488)
}

func BenchmarkGzip512M63(b *testing.B) {
	benchmarkRun(b, gzip.Name, 512, 63488)
}

func BenchmarkGzip1024M63(b *testing.B) {
	benchmarkRun(b, gzip.Name, 1024, 63488)
}

func Benchmark128MNone(b *testing.B) {
	benchmarkRun(b, "", 128, 1024)
}

func BenchmarkNone256M(b *testing.B) {
	benchmarkRun(b, "", 256, 1024)
}

func BenchmarkNone512M(b *testing.B) {
	benchmarkRun(b, "", 512, 1024)
}

func BenchmarkNone1024M(b *testing.B) {
	benchmarkRun(b, "", 1024, 1024)
}

func BenchmarkNone128M4(b *testing.B) {
	benchmarkRun(b, "", 128, 4096)
}

func BenchmarkNone256M4(b *testing.B) {
	benchmarkRun(b, "", 256, 4096)
}

func BenchmarkNone512M4(b *testing.B) {
	benchmarkRun(b, "", 512, 4096)
}

func BenchmarkNone1024M4(b *testing.B) {
	benchmarkRun(b, "", 1024, 4096)
}
func BenchmarkNone128M16(b *testing.B) {
	benchmarkRun(b, "", 128, 16384)
}

func BenchmarkNone256M16(b *testing.B) {
	benchmarkRun(b, "", 256, 16384)
}

func BenchmarkNone512M16(b *testing.B) {
	benchmarkRun(b, "", 512, 16384)
}

func BenchmarkNone1024M16(b *testing.B) {
	benchmarkRun(b, "", 1024, 16384)
}

func BenchmarkNone128M32(b *testing.B) {
	benchmarkRun(b, "", 128, 32768)
}

func BenchmarkNone256M32(b *testing.B) {
	benchmarkRun(b, "", 256, 32768)
}

func BenchmarkNone512M32(b *testing.B) {
	benchmarkRun(b, "", 512, 32768)
}

func BenchmarkNone1024M32(b *testing.B) {
	benchmarkRun(b, "", 1024, 32768)
}

func BenchmarkNone128M63(b *testing.B) {
	benchmarkRun(b, "", 128, 63488)
}

func BenchmarkNone256M63(b *testing.B) {
	benchmarkRun(b, "", 256, 63488)
}

func BenchmarkNone512M63(b *testing.B) {
	benchmarkRun(b, "", 512, 63488)
}

func BenchmarkNone1024M63(b *testing.B) {
	benchmarkRun(b, "", 1024, 63488)
}

const data = "// Licensed to the LF AI & Data foundation under one\n// or more contributor license agreements. See the NOTICE file\n// distributed with this work for additional information\n// regarding copyright ownership. The ASF licenses this file\n// to you under the Apache License, Version 2.0 (the\n// \"License\"); you may not use this file except in compliance\n// with the License. You may obtain a copy of the License at\n//\n//\thttp://www.apache.org/licenses/LICENSE-2.0\n//\n// Unless required by applicable law or agreed to in writing, software\n// distributed under the License is distributed on an \"AS IS\" BASIS,\n// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n// See the License for the specific language governing permissions and\n// limitations under the License."

func runGrpcCompressionPerf(b *testing.B, compressionName string) {
	compressor := encoding.GetCompressor(compressionName)

	// Reset the timer to exclude setup time from the measurements
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		writer, _ := compressor.Compress(&buf)
		writer.Write([]byte(data))
		writer.Close()

		compressedData := buf.Bytes()
		reader, _ := compressor.Decompress(bytes.NewReader(compressedData))
		var result bytes.Buffer
		result.ReadFrom(reader)
	}
}

func BenchmarkCompressionKlauspost(b *testing.B) {
	runGrpcCompressionPerf(b, klauspost.Name)
}
func BenchmarkCompressionDeflate(b *testing.B) {
	runGrpcCompressionPerf(b, deflate.Name)
}
func BenchmarkCompressionLz4(b *testing.B) {
	runGrpcCompressionPerf(b, lz4.Name)
}
func BenchmarkCompressionSnappy(b *testing.B) {
	runGrpcCompressionPerf(b, snappy.Name)
}
func BenchmarkCompressionZstd(b *testing.B) {
	runGrpcCompressionPerf(b, zstd.Name)
}
func BenchmarkCompressionGzip(b *testing.B) {
	runGrpcCompressionPerf(b, gzip.Name)
}
