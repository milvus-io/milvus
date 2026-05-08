package helper

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
)

var (
	addr                = flag.String("addr", "http://localhost:19530", "server host and port")
	user                = flag.String("user", "root", "user")
	password            = flag.String("password", "Milvus", "password")
	logLevel            = flag.String("log.level", "info", "log level for test")
	teiEndpoint         = flag.String("tei_endpoint", "http://text-embeddings-service.milvus-ci.svc.cluster.local:80", "TEI service endpoint for text embedding tests")
	teiRerankerEndpoint = flag.String("tei_reranker_uri", "http://text-rerank-service.milvus-ci.svc.cluster.local:80", "TEI reranker service endpoint")
	teiModelDim         = flag.Int("tei_model_dim", 768, "Vector dimension for text embedding model")
	defaultClientConfig *client.ClientConfig
)

func setDefaultClientConfig(cfg *client.ClientConfig) {
	defaultClientConfig = cfg
}

func GetDefaultClientConfig() *client.ClientConfig {
	newCfg := *defaultClientConfig
	dialOptions := newCfg.DialOptions
	newDialOptions := make([]grpc.DialOption, len(dialOptions))
	copy(newDialOptions, dialOptions)
	newCfg.DialOptions = newDialOptions
	return &newCfg
}

func GetAddr() string {
	return *addr
}

func GetUser() string {
	return *user
}

func GetPassword() string {
	return *password
}

func GetTEIEndpoint() string {
	return *teiEndpoint
}

func GetTEIRerankerEndpoint() string {
	return *teiRerankerEndpoint
}

func GetTEIModelDim() int {
	return *teiModelDim
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
	setDefaultClientConfig(&client.ClientConfig{Address: *addr})
}

// Teardown teardown
func teardown() {
	log.Info("Start to tear down all.....")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*common.DefaultTimeout)
	defer cancel()
	mc, err := base.NewMilvusClient(ctx, &client.ClientConfig{Address: GetAddr(), Username: GetUser(), Password: GetPassword()})
	if err != nil {
		log.Error("teardown failed to connect milvus with error", zap.Error(err))
		return
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

// managementBaseURL returns the Milvus management API base URL (port 9091)
// derived from the gRPC addr flag (e.g. http://host:19530 -> http://host:9091).
func managementBaseURL() string {
	u, err := url.Parse(*addr)
	if err != nil {
		return "http://localhost:9091"
	}
	host := u.Hostname()
	if host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("http://%s:9091", host)
}

// AlterServerConfig changes a Milvus server config via the management HTTP API.
// It returns the previous value so the caller can restore it.
// If the management API is unreachable, it returns ("", error).
func AlterServerConfig(key, value string) (string, error) {
	// Get current value first
	prev, _ := GetServerConfig(key)

	body, _ := json.Marshal(map[string]string{"key": key, "value": value})
	resp, err := http.Post(managementBaseURL()+"/management/config/alter",
		"application/json", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("management API unreachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("alter config failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}
	log.Info("AlterServerConfig", zap.String("key", key), zap.String("value", value), zap.String("prev", prev))
	return prev, nil
}

// GetServerConfig reads a config value from the management API.
func GetServerConfig(key string) (string, error) {
	resp, err := http.Get(managementBaseURL() + "/management/config/get?key=" + url.QueryEscape(key))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("get config failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}
	return string(respBody), nil
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
