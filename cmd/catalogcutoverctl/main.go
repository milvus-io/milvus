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
	"github.com/milvus-io/milvus/internal/metastore"
	catalogrootcoord "github.com/milvus-io/milvus/internal/metastore/catalogservice/rootcoord"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type migrationResult struct {
	Databases     int
	Collections   int
	Aliases       int
	FileResources int
}

func main() {
	rootCoordAddress := flag.String("rootcoord-address", "", "RootCoord gRPC address for online cutover without restarting RootCoord")
	sourceEtcd := flag.String("source-etcd", "127.0.0.1:2379", "source etcd endpoint for the legacy RootCoord catalog")
	sourceRootPath := flag.String("source-root-path", "", "source etcd meta root path, for example by-dev/milvus1/meta")
	catalogAddress := flag.String("catalog-address", "127.0.0.1:19540", "Catalog Service gRPC address")
	targetNamespace := flag.String("target-namespace", "", "target Catalog Service namespace")
	ts := flag.Uint64("ts", uint64(time.Now().UnixNano()), "target catalog commit timestamp")
	drainTimeoutMs := flag.Int64("drain-timeout-ms", 30000, "online cutover metadata write drain timeout in milliseconds")
	cacheExpireTs := flag.Uint64("cache-expire-ts", 0, "proxy metadata cache expiration timestamp for online cutover")
	flag.Parse()

	if *targetNamespace == "" {
		fmt.Fprintln(os.Stderr, "--target-namespace is required")
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

	result, err := migrateRootCoordCatalogSnapshot(ctx, source, target, typeutil.Timestamp(*ts))
	if err != nil {
		fmt.Fprintf(os.Stderr, "cutover migration failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("cutover namespace=%s databases=%d collections=%d aliases=%d file_resources=%d\n",
		*targetNamespace, result.Databases, result.Collections, result.Aliases, result.FileResources)
}

func migrateRootCoordCatalogSnapshot(ctx context.Context, source metastore.RootCoordCatalog, target metastore.RootCoordCatalog, ts typeutil.Timestamp) (migrationResult, error) {
	var result migrationResult

	dbs, err := source.ListDatabases(ctx, typeutil.MaxTimestamp)
	if err != nil {
		return result, err
	}
	for _, db := range dbs {
		if err := target.CreateDatabase(ctx, db, ts); err != nil {
			return result, err
		}
		result.Databases++

		collections, err := source.ListCollections(ctx, db.ID, typeutil.MaxTimestamp)
		if err != nil {
			return result, err
		}
		for _, coll := range collections {
			if err := target.CreateCollection(ctx, coll, ts); err != nil {
				return result, err
			}
			result.Collections++
		}

		aliases, err := source.ListAliases(ctx, db.ID, typeutil.MaxTimestamp)
		if err != nil {
			return result, err
		}
		for _, alias := range aliases {
			if err := target.CreateAlias(ctx, alias, ts); err != nil {
				return result, err
			}
			result.Aliases++
		}
	}

	legacyCollections, err := source.ListCollections(ctx, 0, typeutil.MaxTimestamp)
	if err != nil {
		return result, err
	}
	for _, coll := range legacyCollections {
		if err := target.CreateCollection(ctx, coll, ts); err != nil {
			return result, err
		}
		result.Collections++
	}
	legacyAliases, err := source.ListAliases(ctx, 0, typeutil.MaxTimestamp)
	if err != nil {
		return result, err
	}
	for _, alias := range legacyAliases {
		if err := target.CreateAlias(ctx, alias, ts); err != nil {
			return result, err
		}
		result.Aliases++
	}

	resources, version, err := source.ListFileResource(ctx)
	if err != nil {
		return result, err
	}
	for _, resource := range resources {
		if err := target.SaveFileResource(ctx, resource, version); err != nil {
			return result, err
		}
		result.FileResources++
	}
	return result, nil
}
