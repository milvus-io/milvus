package helper

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	client "github.com/milvus-io/milvus/client/v3/milvusclient"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func withManagementRoundTripper(t *testing.T, fn roundTripFunc) {
	t.Helper()
	prevTransport := http.DefaultTransport
	http.DefaultTransport = fn
	t.Cleanup(func() {
		http.DefaultTransport = prevTransport
	})
}

func withTestAddr(t *testing.T, value string) {
	t.Helper()
	prevAddr := *addr
	prevURI := *uri
	*addr = value
	*uri = ""
	t.Cleanup(func() {
		*addr = prevAddr
		*uri = prevURI
	})
}

func withTestConnectionFlags(t *testing.T, addrValue, uriValue, userValue, passwordValue, tokenValue string) {
	t.Helper()
	prevAddr, prevURI := *addr, *uri
	prevUser, prevPassword, prevToken := *user, *password, *token
	*addr, *uri = addrValue, uriValue
	*user, *password, *token = userValue, passwordValue, tokenValue
	t.Cleanup(func() {
		*addr, *uri = prevAddr, prevURI
		*user, *password, *token = prevUser, prevPassword, prevToken
	})
}

func withTestDefaultClientConfig(t *testing.T, cfg *client.ClientConfig) {
	t.Helper()
	prevCfg := defaultClientConfig
	setDefaultClientConfig(cfg)
	t.Cleanup(func() {
		setDefaultClientConfig(prevCfg)
	})
}

func managementResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestURIFromTestArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "addr with equals", args: []string{"test", "-addr=http://localhost:19530"}, want: "http://localhost:19530"},
		{name: "addr with separate value", args: []string{"test", "--addr", "http://localhost:19530"}, want: "http://localhost:19530"},
		{name: "uri with equals", args: []string{"test", "--uri=https://cloud.example"}, want: "https://cloud.example"},
		{name: "uri overrides addr", args: []string{"test", "--addr=http://localhost:19530", "--uri", "https://cloud.example"}, want: "https://cloud.example"},
		{name: "uri before addr", args: []string{"test", "--uri=https://cloud.example", "--addr=http://localhost:19530"}, want: "https://cloud.example"},
		{name: "empty uri uses addr", args: []string{"test", "--addr=http://localhost:19530", "--uri="}, want: "http://localhost:19530"},
		{name: "missing", args: []string{"test", "-test.v"}, want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, URIFromTestArgs(test.args))
		})
	}
}

func TestNewDefaultClientConfig(t *testing.T) {
	t.Run("uses uri and token", func(t *testing.T) {
		withTestConnectionFlags(t, "http://localhost:19530", "https://cloud.example", "root", "Milvus", "cloud-token")

		cfg := newDefaultClientConfig()
		require.Equal(t, "https://cloud.example", cfg.Address)
		require.Equal(t, "root", cfg.Username)
		require.Equal(t, "Milvus", cfg.Password)
		require.Equal(t, "cloud-token", cfg.APIKey)
	})

	t.Run("falls back to legacy connection flags", func(t *testing.T) {
		withTestConnectionFlags(t, "http://localhost:19530", "", "legacy-user", "legacy-password", "")

		cfg := newDefaultClientConfig()
		require.Equal(t, "http://localhost:19530", cfg.Address)
		require.Equal(t, "legacy-user", cfg.Username)
		require.Equal(t, "legacy-password", cfg.Password)
		require.Empty(t, cfg.APIKey)
	})
}

func TestInheritDefaultConnectionConfig(t *testing.T) {
	withTestDefaultClientConfig(t, &client.ClientConfig{
		Address:  "https://cloud.example",
		Username: "root",
		Password: "Milvus",
		APIKey:   "cloud-token",
	})

	t.Run("fills empty connection settings", func(t *testing.T) {
		input := &client.ClientConfig{DBName: "books"}
		cfg := inheritDefaultConnectionConfig(input)

		require.Equal(t, "https://cloud.example", cfg.Address)
		require.Equal(t, "root", cfg.Username)
		require.Equal(t, "Milvus", cfg.Password)
		require.Equal(t, "cloud-token", cfg.APIKey)
		require.Empty(t, input.Address)
		require.Empty(t, input.APIKey)
	})

	t.Run("adds token for default credentials", func(t *testing.T) {
		cfg := inheritDefaultConnectionConfig(&client.ClientConfig{
			Address:  "https://cloud.example",
			Username: "root",
			Password: "Milvus",
		})

		require.Equal(t, "cloud-token", cfg.APIKey)
	})

	t.Run("preserves custom user credentials", func(t *testing.T) {
		cfg := inheritDefaultConnectionConfig(&client.ClientConfig{
			Address:  "https://cloud.example",
			Username: "test-user",
			Password: "test-password",
		})

		require.Empty(t, cfg.APIKey)
		require.Equal(t, "test-user", cfg.Username)
		require.Equal(t, "test-password", cfg.Password)
	})

	t.Run("preserves explicit token", func(t *testing.T) {
		cfg := inheritDefaultConnectionConfig(&client.ClientConfig{
			Address: "https://other.example",
			APIKey:  "other-token",
		})

		require.Equal(t, "https://other.example", cfg.Address)
		require.Equal(t, "other-token", cfg.APIKey)
	})
}

func TestManagementBaseURL(t *testing.T) {
	t.Run("uses host from grpc address", func(t *testing.T) {
		withTestAddr(t, "http://milvus.example:19530")

		require.Equal(t, "http://milvus.example:9091", managementBaseURL())
	})

	t.Run("falls back to localhost on invalid address", func(t *testing.T) {
		withTestAddr(t, "http://%zz")

		require.Equal(t, "http://localhost:9091", managementBaseURL())
	})

	t.Run("uses host from address without scheme", func(t *testing.T) {
		withTestAddr(t, "localhost:19530")

		require.Equal(t, "http://localhost:9091", managementBaseURL())
	})

	t.Run("uses ci service host from address without scheme", func(t *testing.T) {
		withTestAddr(t, "gosdk-4823-milvus.jenkins-milvus-ci:19530")

		require.Equal(t, "http://gosdk-4823-milvus.jenkins-milvus-ci:9091", managementBaseURL())
	})
}

func TestGetServerConfig(t *testing.T) {
	configKey := "queryNode.internalCollection.useTakeForOutput"

	t.Run("success", func(t *testing.T) {
		withTestAddr(t, "http://milvus.example:19530")
		withManagementRoundTripper(t, func(req *http.Request) (*http.Response, error) {
			require.Equal(t, http.MethodGet, req.Method)
			require.Equal(t, "milvus.example:9091", req.URL.Host)
			require.Equal(t, "/management/config/get", req.URL.Path)
			require.Equal(t, configKey, req.URL.Query().Get("keys"))
			return managementResponse(http.StatusOK,
				`{"configs":[{"key":"`+configKey+`","value":"true"}]}`), nil
		})

		value, err := GetServerConfig(configKey)
		require.NoError(t, err)
		require.Equal(t, "true", value)
	})

	t.Run("http error", func(t *testing.T) {
		withManagementRoundTripper(t, func(req *http.Request) (*http.Response, error) {
			return managementResponse(http.StatusInternalServerError, "boom"), nil
		})

		_, err := GetServerConfig(configKey)
		require.ErrorContains(t, err, "HTTP 500")
		require.ErrorContains(t, err, "boom")
	})

	t.Run("invalid json", func(t *testing.T) {
		withManagementRoundTripper(t, func(req *http.Request) (*http.Response, error) {
			return managementResponse(http.StatusOK, "{"), nil
		})

		_, err := GetServerConfig(configKey)
		require.Error(t, err)
	})

	t.Run("missing config", func(t *testing.T) {
		withManagementRoundTripper(t, func(req *http.Request) (*http.Response, error) {
			return managementResponse(http.StatusOK, `{"configs":[]}`), nil
		})

		_, err := GetServerConfig(configKey)
		require.ErrorContains(t, err, "not found")
	})

	t.Run("config error", func(t *testing.T) {
		withManagementRoundTripper(t, func(req *http.Request) (*http.Response, error) {
			return managementResponse(http.StatusOK,
				`{"configs":[{"key":"`+configKey+`","error":"unknown key"}]}`), nil
		})

		_, err := GetServerConfig(configKey)
		require.ErrorContains(t, err, "unknown key")
	})

	t.Run("transport error", func(t *testing.T) {
		withManagementRoundTripper(t, func(req *http.Request) (*http.Response, error) {
			return nil, errors.New("dial failed")
		})

		_, err := GetServerConfig(configKey)
		require.ErrorContains(t, err, "dial failed")
	})
}

func TestAlterServerConfig(t *testing.T) {
	configKey := "queryNode.internalCollection.useTakeForOutput"

	t.Run("success returns previous value", func(t *testing.T) {
		var postSeen bool
		withTestAddr(t, "http://milvus.example:19530")
		withManagementRoundTripper(t, func(req *http.Request) (*http.Response, error) {
			switch req.Method {
			case http.MethodGet:
				return managementResponse(http.StatusOK,
					`{"configs":[{"key":"`+configKey+`","value":"false"}]}`), nil
			case http.MethodPost:
				postSeen = true
				require.Equal(t, "milvus.example:9091", req.URL.Host)
				require.Equal(t, "/management/config/alter", req.URL.Path)
				body, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.JSONEq(t,
					`{"key":"`+configKey+`","value":"true"}`,
					string(body))
				return managementResponse(http.StatusOK, "{}"), nil
			default:
				require.FailNow(t, "unexpected method", req.Method)
				return nil, nil
			}
		})

		prev, err := AlterServerConfig(configKey, "true")
		require.NoError(t, err)
		require.Equal(t, "false", prev)
		require.True(t, postSeen)
	})

	t.Run("post http error", func(t *testing.T) {
		withManagementRoundTripper(t, func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodGet {
				return managementResponse(http.StatusOK,
					`{"configs":[{"key":"`+configKey+`","value":"false"}]}`), nil
			}
			return managementResponse(http.StatusServiceUnavailable, "not ready"), nil
		})

		_, err := AlterServerConfig(configKey, "true")
		require.ErrorContains(t, err, "HTTP 503")
		require.ErrorContains(t, err, "not ready")
	})

	t.Run("post transport error", func(t *testing.T) {
		withManagementRoundTripper(t, func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodGet {
				return managementResponse(http.StatusOK,
					`{"configs":[{"key":"`+configKey+`","value":"false"}]}`), nil
			}
			return nil, errors.New("connection refused")
		})

		_, err := AlterServerConfig(configKey, "true")
		require.ErrorContains(t, err, "management API unreachable")
		require.ErrorContains(t, err, "connection refused")
	})
}
