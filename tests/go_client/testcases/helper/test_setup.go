package helper

import (
	"context"
	"flag"
	"testing"
	"time"

	"go.uber.org/zap"

	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

var (
	addr                = flag.String("addr", "localhost:19530", "server host and port")
	logLevel            = flag.String("log.level", "info", "log level for test")
	defaultClientConfig *client.ClientConfig
)

func setDefaultClientConfig(cfg *client.ClientConfig) {
	defaultClientConfig = cfg
}

func GetDefaultClientConfig() *client.ClientConfig {
	return defaultClientConfig
}

func GetAddr() string {
	return *addr
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

func setup() {
	log.Info("Start to setup all......")
	flag.Parse()
	parseLogConfig()
	log.Info("Parser Milvus address", zap.String("address", *addr))

	// set default milvus client config
	setDefaultClientConfig(&client.ClientConfig{Address: *addr, Username: common.RootUser, Password: common.RootPwd})
}

// Teardown teardown
func teardown() {
	log.Info("Start to tear down all.....")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*common.DefaultTimeout)
	defer cancel()
	mc, err := base.NewMilvusClient(ctx, defaultClientConfig)
	if err != nil {
		log.Error("teardown failed to connect milvus with error", zap.Error(err))
	}
	defer mc.Close(ctx)

	// clear dbs
	dbs, _ := mc.ListDatabase(ctx, client.NewListDatabaseOption())
	for _, db := range dbs {
		if db != common.DefaultDb {
			_ = mc.UseDatabase(ctx, client.NewUseDatabaseOption(db))
			collections, _ := mc.ListCollections(ctx, client.NewListCollectionOption())
			for _, coll := range collections {
				_ = mc.DropCollection(ctx, client.NewDropCollectionOption(coll))
			}
			_ = mc.DropDatabase(ctx, client.NewDropDatabaseOption(db))
		}
	}
}

func RunTests(m *testing.M) int {
	setup()
	code := m.Run()
	if code != 0 {
		log.Error("Tests failed and exited", zap.Int("code", code))
	}
	teardown()
	return code
}
