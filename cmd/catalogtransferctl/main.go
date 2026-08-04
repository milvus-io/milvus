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

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func main() {
	address := flag.String("address", "127.0.0.1:19540", "Catalog Service gRPC address")
	transferID := flag.String("transfer-id", "", "transfer id")
	transferEpoch := flag.Int64("transfer-epoch", time.Now().UnixNano(), "transfer fencing epoch")
	sourceNamespace := flag.String("source-namespace", "", "source namespace")
	targetNamespace := flag.String("target-namespace", "", "target namespace")
	dbName := flag.String("db", "default", "database name")
	collectionName := flag.String("collection", "", "collection name")
	commitTs := flag.Uint64("commit-ts", uint64(time.Now().UnixNano()), "catalog commit timestamp")
	cacheExpireTs := flag.Uint64("cache-expire-ts", uint64(time.Now().UnixNano()), "proxy cache expiration timestamp")
	drainTimeoutMs := flag.Int64("drain-timeout-ms", 30000, "source RootCoord drain timeout in milliseconds")
	getOnly := flag.Bool("get", false, "get transfer state instead of starting transfer")
	flag.Parse()

	if *transferID == "" {
		fmt.Fprintln(os.Stderr, "--transfer-id is required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn, err := grpc.DialContext(ctx, *address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect catalog service failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := catalogpb.NewCatalogServiceClient(conn)
	if *getOnly {
		resp, err := client.GetCollectionTransfer(ctx, &catalogpb.GetCollectionTransferRequest{TransferId: *transferID})
		if err := merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
			fmt.Fprintf(os.Stderr, "get transfer failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("transfer_id=%s state=%s collection_id=%d last_error=%q\n",
			resp.GetTransferId(), resp.GetState().String(), resp.GetCollectionId(), resp.GetLastError())
		return
	}

	resp, err := client.StartCollectionTransfer(ctx, &catalogpb.StartCollectionTransferRequest{
		TransferId:      *transferID,
		TransferEpoch:   *transferEpoch,
		SourceNamespace: *sourceNamespace,
		TargetNamespace: *targetNamespace,
		DbName:          *dbName,
		CollectionName:  *collectionName,
		CommitTs:        *commitTs,
		CacheExpireTs:   *cacheExpireTs,
		DrainTimeoutMs:  *drainTimeoutMs,
	})
	if err := merr.CheckRPCCall(resp.GetStatus(), err); err != nil {
		fmt.Fprintf(os.Stderr, "start transfer failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("transfer_id=%s state=%s collection_id=%d\n",
		resp.GetTransferId(), resp.GetState().String(), resp.GetCollectionId())
}
