// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package grpcclient

import (
	"bytes"
	"context"
	"log"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/examples/helloworld/helloworld"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util/compressor/deflate"
	"github.com/milvus-io/milvus/pkg/util/compressor/lz4"
	"github.com/milvus-io/milvus/pkg/util/compressor/snappy"
	"github.com/milvus-io/milvus/pkg/util/compressor/zstd"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var compressorTypes = []string{zstd.Name, gzip.Name, snappy.Name, lz4.Name, deflate.Name}

func TestGrpcEncoder(t *testing.T) {
	data := "hello zstd algorithm!"

	for _, name := range compressorTypes {
		compressor := encoding.GetCompressor(name)
		var buf bytes.Buffer
		writer, err := compressor.Compress(&buf)
		assert.NoError(t, err)
		written, err := writer.Write([]byte(data))
		assert.NoError(t, err)
		assert.Equal(t, written, len(data))
		err = writer.Close()
		assert.NoError(t, err)

		reader, err := compressor.Decompress(bytes.NewReader(buf.Bytes()))
		assert.NoError(t, err)
		result := make([]byte, len(data))
		reader.Read(result)
		assert.Equal(t, data, string(result))
	}
}

func TestGrpcCompression(t *testing.T) {
	// ClusterInjectionUnaryClientInterceptor/ClusterInjectionStreamClientInterceptor need read `msgChannel.chanNamePrefix.cluster`
	paramtable.Init()
	lis, err := net.Listen("tcp", "localhost:")
	address := lis.Addr()
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	helloworld.RegisterGreeterServer(s, &compressionServer{})
	reflection.Register(s)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	defer s.Stop()

	for _, compressorType := range compressorTypes {
		_, err := sayHelloWithCompression(address.String(), compressorType)
		assert.NoError(t, err)
	}

	invalidCompression := "invalid"
	_, err = sayHelloWithCompression(address.String(), invalidCompression)
	assert.Equal(t, true, strings.HasSuffix(err.Error(),
		status.Errorf(codes.Internal, "grpc: Compressor is not installed for requested grpc-encoding \"%s\"", invalidCompression).Error()))
}

func sayHelloWithCompression(serverAddress string, compressionType string) (any, error) {
	clientBase := ClientBase[grpcClientInterface]{
		ClientMaxRecvSize:      1 * 1024 * 1024,
		ClientMaxSendSize:      1 * 1024 * 1024,
		DialTimeout:            60 * time.Second,
		KeepAliveTime:          60 * time.Second,
		KeepAliveTimeout:       60 * time.Second,
		RetryServiceNameConfig: "helloworld.Greeter",
		MaxAttempts:            1,
		InitialBackoff:         10.0,
		MaxBackoff:             60.0,
		CompressionEnabled:     true,
		CompressionType:        compressionType,
	}
	clientBase.SetGetAddrFunc(func() (string, error) {
		return serverAddress, nil
	})
	clientBase.SetNewGrpcClientFunc(func(cc *grpc.ClientConn) grpcClientInterface {
		return &compressionClient{helloworld.NewGreeterClient(cc)}
	})
	defer clientBase.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return clientBase.Call(ctx, func(client grpcClientInterface) (any, error) {
		return client.(*compressionClient).SayHello(ctx, &helloworld.HelloRequest{})
	})
}

type grpcClientInterface interface {
	GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error)
}

type compressionClient struct {
	helloworld.GreeterClient
}

func (c *compressionClient) GetComponentStates(ctx context.Context, in *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{}, nil
}

type compressionServer struct {
	helloworld.UnimplementedGreeterServer
}

func (compressionServer) SayHello(ctx context.Context, in *helloworld.HelloRequest) (*helloworld.HelloReply, error) {
	return &helloworld.HelloReply{Message: "Hello " + in.GetName()}, nil
}
