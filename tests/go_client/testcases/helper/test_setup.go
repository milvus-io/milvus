package helper

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
	mlog.Info(context.TODO(), "Parser Log Level", mlog.String("logLevel", *logLevel))
	switch *logLevel {
	case "debug", "DEBUG", "Debug":
		mlog.SetLevel(mlog.DebugLevel)
	case "info", "INFO", "Info":
		mlog.SetLevel(mlog.InfoLevel)
	case "warn", "WARN", "Warn":
		mlog.SetLevel(mlog.WarnLevel)
	case "error", "ERROR", "Error":
		mlog.SetLevel(mlog.ErrorLevel)
	default:
		mlog.SetLevel(mlog.InfoLevel)
	}
}

func setup() {
	mlog.Info(context.TODO(), "Start to setup all......")
	flag.Parse()
	parseLogConfig()
	mlog.Info(context.TODO(), "Parser Milvus address", mlog.String("address", *addr))

	// set default milvus client config
	setDefaultClientConfig(&client.ClientConfig{Address: *addr})
}

// Teardown teardown
func teardown() {
	mlog.Info(context.TODO(), "Start to tear down all.....")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*common.DefaultTimeout)
	defer cancel()
	mc, err := base.NewMilvusClient(ctx, &client.ClientConfig{Address: GetAddr(), Username: GetUser(), Password: GetPassword()})
	if err != nil {
		mlog.Error(context.TODO(), "teardown failed to connect milvus with error", mlog.Err(err))
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
	host := ""
	rawAddr := strings.TrimSpace(*addr)
	if rawAddr != "" {
		parseAddr := rawAddr
		if !strings.Contains(rawAddr, "://") {
			parseAddr = "http://" + rawAddr
		}
		if u, err := url.Parse(parseAddr); err == nil {
			host = u.Hostname()
		}
	}
	if host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("http://%s", net.JoinHostPort(host, "9091"))
}

// AlterServerConfig changes a Milvus server config via the management HTTP API.
// It returns the previous value so the caller can restore it.
// If the management API is unreachable, it returns ("", error).
func AlterServerConfig(key, value string) (string, error) {
	// Get current value first
	prev, _ := GetServerConfig(key)

	body, _ := json.Marshal(map[string]string{"key": key, "value": value})
	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Post(managementBaseURL()+"/management/config/alter",
		"application/json", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("management API unreachable: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("alter config failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}
	mlog.Info(context.TODO(), "AlterServerConfig", mlog.String("key", key), mlog.String("value", value), mlog.String("prev", prev))
	return prev, nil
}

// GetServerConfig reads a config value from the management API.
func GetServerConfig(key string) (string, error) {
	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Get(managementBaseURL() + "/management/config/get?keys=" + url.QueryEscape(key))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("get config failed (HTTP %d): %s", resp.StatusCode, string(respBody))
	}
	var result struct {
		Configs []struct {
			Key   string `json:"key"`
			Value string `json:"value"`
			Error string `json:"error"`
		} `json:"configs"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("decode config response: %w", err)
	}
	if len(result.Configs) == 0 {
		return "", fmt.Errorf("config %q not found", key)
	}
	if result.Configs[0].Error != "" {
		return "", fmt.Errorf("get config %q failed: %s", key, result.Configs[0].Error)
	}
	return result.Configs[0].Value, nil
}

func RunTests(m *testing.M) int {
	setup()
	code := m.Run()
	if code != 0 {
		mlog.Error(context.TODO(), "Tests failed and exited", mlog.Int("code", code))
	}
	teardown()
	return code
}
