// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	catalogrootcoord "github.com/milvus-io/milvus/internal/metastore/catalogservice/rootcoord"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/internal/rootcoord/catalogmigration"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func main() {
	rootCoordAddress := flag.String("rootcoord-address", "", "RootCoord gRPC address for online cutover without restarting RootCoord")
	sourceEtcd := flag.String("source-etcd", "127.0.0.1:2379", "source etcd endpoint for the legacy RootCoord catalog")
	sourceRootPath := flag.String("source-root-path", "", "source etcd meta root path, for example by-dev/milvus1/meta")
	catalogAddress := flag.String("catalog-address", "127.0.0.1:19540", "Catalog Service gRPC address")
	targetNamespace := flag.String("target-namespace", "", "target Catalog Service namespace")
	ts := flag.Uint64("ts", defaultCutoverTimestamp(), "target catalog commit timestamp; 0 lets RootCoord allocate a Milvus hybrid timestamp for online cutover")
	drainTimeoutMs := flag.Int64("drain-timeout-ms", 30000, "online cutover metadata write drain timeout in milliseconds")
	cacheExpireTs := flag.Uint64("cache-expire-ts", 0, "proxy metadata cache expiration timestamp for online cutover")
	flag.Parse()

	opts := cutoverOptions{
		RootCoordAddress: *rootCoordAddress,
		SourceRootPath:   *sourceRootPath,
		TargetNamespace:  *targetNamespace,
		Ts:               *ts,
	}
	if err := validateCutoverOptions(opts); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if *rootCoordAddress != "" {
		conn, err := grpc.DialContext(ctx, *rootCoordAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintf(os.Stderr, "connect rootcoord failed: %v\n", err)
			os.Exit(1)
		}
		defer conn.Close()
		client := rootcoordpb.NewRootCoordClient(conn)
		resp, err := client.RootCoordCatalogCutover(ctx, &rootcoordpb.RootCoordCatalogCutoverRequest{
			CatalogServiceAddress:   *catalogAddress,
			CatalogServiceNamespace: *targetNamespace,
			CutoverTs:               *ts,
			DrainTimeoutMs:          *drainTimeoutMs,
			CacheExpireTs:           *cacheExpireTs,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "online cutover rpc failed: %v\n", err)
			os.Exit(1)
		}
		if err := merr.Error(resp.GetStatus()); err != nil {
			fmt.Fprintf(os.Stderr, "online cutover failed: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("online cutover namespace=%s databases=%d collections=%d aliases=%d file_resources=%d\n",
			*targetNamespace, resp.GetDatabases(), resp.GetCollections(), resp.GetAliases(), resp.GetFileResources())
		return
	}

	if *sourceRootPath == "" {
		fmt.Fprintln(os.Stderr, "--source-root-path is required")
		os.Exit(2)
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{*sourceEtcd},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect source etcd failed: %v\n", err)
		os.Exit(1)
	}
	defer etcdClient.Close()
	source := kvrootcoord.NewCatalog(etcdkv.NewEtcdKV(etcdClient, *sourceRootPath))

	conn, err := grpc.DialContext(ctx, *catalogAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect catalog service failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	target := catalogrootcoord.NewCatalog(catalogpb.NewRootCatalogServiceClient(conn), *targetNamespace)

	result, err := catalogmigration.Snapshot(ctx, source, target, typeutil.Timestamp(*ts))
	if err != nil {
		fmt.Fprintf(os.Stderr, "cutover migration failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("cutover namespace=%s databases=%d collections=%d aliases=%d file_resources=%d\n",
		*targetNamespace, result.Databases, result.Collections, result.Aliases, result.FileResources)
}

func defaultCutoverTimestamp() uint64 {
	return 0
}

type cutoverOptions struct {
	RootCoordAddress string
	SourceRootPath   string
	TargetNamespace  string
	Ts               uint64
}

func validateCutoverOptions(opts cutoverOptions) error {
	if opts.TargetNamespace == "" {
		return fmt.Errorf("--target-namespace is required")
	}
	if opts.RootCoordAddress != "" {
		return nil
	}
	if opts.SourceRootPath == "" {
		return fmt.Errorf("--source-root-path is required")
	}
	if opts.Ts == 0 {
		return fmt.Errorf("--ts is required for offline cutover and must be a Milvus hybrid timestamp")
	}
	return nil
}
