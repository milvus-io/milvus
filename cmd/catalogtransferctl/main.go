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
	transferEpoch := flag.Int64("transfer-epoch", defaultTransferEpoch(), "transfer fencing epoch")
	sourceNamespace := flag.String("source-namespace", "", "source namespace")
	targetNamespace := flag.String("target-namespace", "", "target namespace")
	dbName := flag.String("db", "default", "database name")
	collectionName := flag.String("collection", "", "collection name")
	commitTs := flag.Uint64("commit-ts", defaultTransferCommitTs(), "catalog commit Milvus hybrid timestamp")
	cacheExpireTs := flag.Uint64("cache-expire-ts", defaultTransferCacheExpireTs(), "proxy cache expiration Milvus hybrid timestamp; 0 reuses commit timestamp")
	drainTimeoutMs := flag.Int64("drain-timeout-ms", 30000, "source RootCoord drain timeout in milliseconds")
	getOnly := flag.Bool("get", false, "get transfer state instead of starting transfer")
	confirm := flag.Bool("confirm", false, "confirm starting a destructive collection transfer")
	flag.Parse()

	opts := transferOptions{
		TransferID:      *transferID,
		TransferEpoch:   *transferEpoch,
		SourceNamespace: *sourceNamespace,
		TargetNamespace: *targetNamespace,
		CollectionName:  *collectionName,
		CommitTs:        *commitTs,
		CacheExpireTs:   *cacheExpireTs,
		GetOnly:         *getOnly,
		ConfirmStart:    *confirm,
	}
	if err := validateTransferOptions(opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if !*getOnly && *cacheExpireTs == 0 {
		*cacheExpireTs = *commitTs
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

func defaultTransferEpoch() int64 {
	return 0
}

func defaultTransferCommitTs() uint64 {
	return 0
}

func defaultTransferCacheExpireTs() uint64 {
	return 0
}

type transferOptions struct {
	TransferID      string
	TransferEpoch   int64
	SourceNamespace string
	TargetNamespace string
	CollectionName  string
	CommitTs        uint64
	CacheExpireTs   uint64
	GetOnly         bool
	ConfirmStart    bool
}

func validateTransferOptions(opts transferOptions) error {
	if opts.TransferID == "" {
		return fmt.Errorf("--transfer-id is required")
	}
	if opts.GetOnly {
		return nil
	}
	if !opts.ConfirmStart {
		return fmt.Errorf("--confirm is required to start collection transfer")
	}
	if opts.TransferEpoch <= 0 {
		return fmt.Errorf("--transfer-epoch is required")
	}
	if opts.SourceNamespace == "" {
		return fmt.Errorf("--source-namespace is required")
	}
	if opts.TargetNamespace == "" {
		return fmt.Errorf("--target-namespace is required")
	}
	if opts.CollectionName == "" {
		return fmt.Errorf("--collection is required")
	}
	if opts.CommitTs == 0 {
		return fmt.Errorf("--commit-ts is required and must be a Milvus hybrid timestamp")
	}
	return nil
}
