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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func run() error {
	endpoint := flag.String("endpoint", "", "StreamingNode RPC endpoint")
	pchannel := flag.String("pchannel", "", "physical channel")
	term := flag.Int64("term", 0, "current owner term")
	channel := flag.String("vchannel", "", "logical channel")
	after := flag.Uint64("after", 0, "exclusive lower bound")
	through := flag.Uint64("through", 0, "inclusive upper bound")
	flag.Parse()
	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = contextutil.WithCreateTransformStream(ctx, &streamingpb.CreateTransformStreamRequest{Pchannel: &streamingpb.PChannelInfo{Name: *pchannel, Term: *term}})
	stream, err := streamingpb.NewStreamingNodeHandlerServiceClient(conn).SubscribeTransform(ctx)
	if err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetCreateStream() == nil {
		return merr.WrapErrDataIntegrityMsg("missing stream creation acknowledgement")
	}
	err = stream.Send(&streamingpb.TransformRequest{Request: &streamingpb.TransformRequest_Create{Create: &streamingpb.CreateTransformSubscriptionRequest{SubscriptionId: 1, Vchannel: *channel, StartAfterTimeTick: *after, EndTimeTick: *through}}})
	if err != nil {
		return err
	}
	for {
		response, err := stream.Recv()
		if err != nil {
			return err
		}
		data, err := protojson.Marshal(response)
		if err != nil {
			return err
		}
		fmt.Println(string(data))
		if response.GetSyncUp() != nil || response.GetSubscriptionError() != nil {
			return nil
		}
	}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
