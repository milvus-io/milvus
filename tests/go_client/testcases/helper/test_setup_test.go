package helper

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
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
	*addr = value
	t.Cleanup(func() {
		*addr = prevAddr
	})
}

func managementResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
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
