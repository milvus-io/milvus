package testcases

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	clientv2 "github.com/milvus-io/milvus/client/v2"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

var addr = flag.String("addr", "localhost:19530", "server host and port")
var defaultCfg clientv2.ClientConfig

// teardown
func teardown() {
	log.Info("Start to tear down all.....")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*common.DefaultTimeout)
	defer cancel()
	mc, err := base.NewMilvusClient(ctx, &defaultCfg)
	if err != nil {
		log.Fatal("teardown failed to connect milvus with error", zap.Error(err))
	}
	defer mc.Close(ctx)

	// clear dbs
	dbs, _ := mc.ListDatabases(ctx, clientv2.NewListDatabaseOption())
	for _, db := range dbs {
		if db != common.DefaultDb {
			_ = mc.UsingDatabase(ctx, clientv2.NewUsingDatabaseOption(db))
			collections, _ := mc.ListCollections(ctx, clientv2.NewListCollectionOption())
			for _, coll := range collections {
				_ = mc.DropCollection(ctx, clientv2.NewDropCollectionOption(coll))
			}
			_ = mc.DropDatabase(ctx, clientv2.NewDropDatabaseOption(db))
		}
	}
}

// create connect
func createDefaultMilvusClient(ctx context.Context, t *testing.T) *base.MilvusClient {
	t.Helper()

	var (
		mc  *base.MilvusClient
		err error
	)
	mc, err = base.NewMilvusClient(ctx, &defaultCfg)
	common.CheckErr(t, err, true)

	t.Cleanup(func() {
		mc.Close(ctx)
	})

	return mc
}

func TestMain(m *testing.M) {
	flag.Parse()
	log.Info("Parser Milvus address", zap.String("address", *addr))
	defaultCfg = clientv2.ClientConfig{Address: *addr}
	code := m.Run()
	if code != 0 {
		log.Error("Tests failed and exited", zap.Int("code", code))
	}
	teardown()
	os.Exit(code)
}
