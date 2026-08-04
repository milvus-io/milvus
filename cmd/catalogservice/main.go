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
	"log"
	"net"
	"path"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus/internal/catalogservice"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	tikvkv "github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/internal/metastore"
	kvrootcoord "github.com/milvus-io/milvus/internal/metastore/kv/rootcoord"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	tikvutil "github.com/milvus-io/milvus/pkg/v3/util/tikv"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:19540", "Catalog Service gRPC listen address")
	allowInsecureRemoteListen := flag.Bool("allow-insecure-remote-listen", false, "allow plaintext Catalog Service gRPC to listen on non-loopback addresses")
	metastoreType := flag.String("metastore", defaultCatalogServiceMetastoreType(), "metadata backend: etcd or tikv")
	etcdEndpoints := flag.String("etcd", "127.0.0.1:2379", "comma-separated etcd endpoints")
	tikvPD := flag.String("tikv-pd", "127.0.0.1:2389", "comma-separated TiKV PD endpoints")
	rootPrefix := flag.String("root-prefix", "by-dev/catalog", "backend root prefix; namespace metadata is under <root-prefix>/<namespace>")
	namespaceMetaSubPath := flag.String("namespace-meta-subpath", "meta", "Milvus metastore metadata subpath under each namespace root, matching tikv.metaSubPath/etcd.metaSubPath")
	jobPrefix := flag.String("job-prefix", "by-dev/catalog-transfer-jobs", "backend prefix for durable transfer jobs")
	rootcoordRoutes := flag.String("rootcoord-routes", "", "comma-separated namespace=rootcoord-address entries, for example milvus1=127.0.0.1:53100,milvus2=127.0.0.1:53200")
	flag.Parse()

	paramtable.Init()

	if err := validateCatalogServiceListenAddress(*listen, *allowInsecureRemoteListen); err != nil {
		log.Fatalf("invalid catalog service listen address: %v", err)
	}
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen catalog service failed, listen=%s: %v", *listen, err)
	}
	defer lis.Close()

	backend, closeBackend, err := newBackend(*metastoreType, *etcdEndpoints, *tikvPD, *rootPrefix)
	if err != nil {
		log.Fatalf("init catalog backend failed: %v", err)
	}
	defer closeBackend()

	catalogResolver := newNamespaceCatalogResolver(backend, *namespaceMetaSubPath)
	rootResolver, closeRoots, err := newStaticRootCoordResolver(*rootcoordRoutes)
	if err != nil {
		log.Fatalf("init rootcoord routes failed: %v", err)
	}
	defer closeRoots()

	jobKV := backend.kv(*jobPrefix)
	manager := catalogservice.NewTransferManager(
		catalogResolver,
		rootResolver,
		catalogservice.NewKVTransferJobStore(jobKV, "jobs"),
	)

	srv := grpc.NewServer()
	catalogpb.RegisterCatalogServiceServer(srv, catalogservice.NewServer(manager))
	catalogpb.RegisterRootCatalogServiceServer(srv, catalogservice.NewRootCatalogServer(catalogResolver))
	log.Printf("catalog service started listen=%s metastore=%s rootPrefix=%s jobPrefix=%s", *listen, *metastoreType, *rootPrefix, *jobPrefix)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve catalog service failed: %v", err)
	}
}

func defaultCatalogServiceMetastoreType() string {
	return util.MetaStoreTypeTiKV
}

func validateCatalogServiceListenAddress(listen string, allowInsecureRemote bool) error {
	if allowInsecureRemote {
		return nil
	}
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return err
	}
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("plaintext listen address %q is not loopback; set --allow-insecure-remote-listen only for isolated test environments", listen)
}

type backendFactory struct {
	rootPrefix string
	kv         func(root string) kv.MetaKv
}

func newBackend(metastoreType string, etcdEndpoints string, tikvPD string, rootPrefix string) (*backendFactory, func(), error) {
	switch metastoreType {
	case util.MetaStoreTypeEtcd:
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   splitCSV(etcdEndpoints),
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			return nil, nil, err
		}
		return &backendFactory{rootPrefix: rootPrefix, kv: func(root string) kv.MetaKv {
			return etcdkv.NewEtcdKV(cli, root)
		}}, func() { _ = cli.Close() }, nil
	case util.MetaStoreTypeTiKV:
		paramtable.Get().TiKVCfg.Endpoints.SwapTempValue(tikvPD)
		cli, err := tikvutil.GetTiKVClient(&paramtable.Get().TiKVCfg)
		if err != nil {
			return nil, nil, err
		}
		return &backendFactory{rootPrefix: rootPrefix, kv: func(root string) kv.MetaKv {
			return tikvkv.NewTiKV(cli, root)
		}}, func() { _ = cli.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("unsupported metastore type %q", metastoreType)
	}
}

type namespaceCatalogResolver struct {
	backend     *backendFactory
	metaSubPath string

	mu       sync.Mutex
	catalogs map[string]metastore.RootCoordCatalog
}

func newNamespaceCatalogResolver(backend *backendFactory, metaSubPath string) *namespaceCatalogResolver {
	return &namespaceCatalogResolver{
		backend:     backend,
		metaSubPath: metaSubPath,
		catalogs:    make(map[string]metastore.RootCoordCatalog),
	}
}

func (r *namespaceCatalogResolver) RootCoordCatalog(namespace string) (metastore.RootCoordCatalog, error) {
	root, err := namespaceMetaRoot(r.backend.rootPrefix, namespace, r.metaSubPath)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if catalog, ok := r.catalogs[namespace]; ok {
		return catalog, nil
	}
	catalog := kvrootcoord.NewCatalog(r.backend.kv(root))
	r.catalogs[namespace] = catalog
	return catalog, nil
}

func namespaceMetaRoot(rootPrefix string, namespace string, metaSubPath string) (string, error) {
	if err := catalogservice.ValidateCatalogPathSegment("namespace", namespace); err != nil {
		return "", err
	}
	rootPrefix = strings.Trim(rootPrefix, "/")
	metaSubPath = strings.Trim(metaSubPath, "/")
	if metaSubPath == "" {
		return path.Join(rootPrefix, namespace), nil
	}
	return path.Join(rootPrefix, namespace, metaSubPath), nil
}

type staticRootCoordResolver struct {
	clients map[string]catalogservice.TransferRootCoord
	conns   []*grpc.ClientConn
}

func newStaticRootCoordResolver(routes string) (*staticRootCoordResolver, func(), error) {
	clients := make(map[string]catalogservice.TransferRootCoord)
	var conns []*grpc.ClientConn
	for _, entry := range splitCSV(routes) {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return nil, nil, fmt.Errorf("invalid rootcoord route %q", entry)
		}
		namespace := strings.TrimSpace(parts[0])
		addr := strings.TrimSpace(parts[1])
		conn, err := grpc.DialContext(context.Background(), addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, nil, err
		}
		conns = append(conns, conn)
		clients[namespace] = catalogservice.NewRootCoordTransferRPCClient(rootcoordpb.NewRootCoordClient(conn))
	}
	resolver := &staticRootCoordResolver{clients: clients, conns: conns}
	return resolver, func() {
		for _, conn := range conns {
			_ = conn.Close()
		}
	}, nil
}

func (r *staticRootCoordResolver) RootCoord(namespace string) (catalogservice.TransferRootCoord, error) {
	client, ok := r.clients[namespace]
	if !ok {
		return nil, fmt.Errorf("rootcoord route for namespace %q is not configured", namespace)
	}
	return client, nil
}

func splitCSV(value string) []string {
	var out []string
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
