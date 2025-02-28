package testcases

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	clientv2 "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

var (
	addr       = flag.String("addr", "localhost:19530", "server host and port")
	logLevel   = flag.String("log.level", "info", "log level for test")
	defaultCfg clientv2.ClientConfig
)

// teardown
func teardown() {
	log.Info("Start to tear down all.....")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*common.DefaultTimeout)
	defer cancel()
	mc, err := base.NewMilvusClient(ctx, &defaultCfg)
	if err != nil {
		log.Error("teardown failed to connect milvus with error", zap.Error(err))
	}
	defer mc.Close(ctx)

	// clear dbs
	dbs, _ := mc.ListDatabase(ctx, clientv2.NewListDatabaseOption())
	for _, db := range dbs {
		if db != common.DefaultDb {
			_ = mc.UseDatabase(ctx, clientv2.NewUseDatabaseOption(db))
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

// create connect
func createMilvusClient(ctx context.Context, t *testing.T, cfg *clientv2.ClientConfig) *base.MilvusClient {
	t.Helper()

	var (
		mc  *base.MilvusClient
		err error
	)
	mc, err = base.NewMilvusClient(ctx, cfg)
	common.CheckErr(t, err, true)

	t.Cleanup(func() {
		mc.Close(ctx)
	})

	return mc
}

func parseLogConfig() {
	log.Info("Parser Log Level", zap.String("logLevel", *logLevel))
	switch *logLevel {
	case "debug", "DEBUG", "Debug":
		log.SetLevel(zap.DebugLevel)
	case "info", "INFO", "Info":
		log.SetLevel(zap.InfoLevel)
	case "warn", "WARN", "Warn":
		log.SetLevel(zap.WarnLevel)
	case "error", "ERROR", "Error":
		log.SetLevel(zap.ErrorLevel)
	default:
		log.SetLevel(zap.InfoLevel)
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	parseLogConfig()
	log.Info("Parser Milvus address", zap.String("address", *addr))
	defaultCfg = clientv2.ClientConfig{Address: *addr}
	code := m.Run()
	if code != 0 {
		log.Error("Tests failed and exited", zap.Int("code", code))
	}
	teardown()
	os.Exit(code)
}
