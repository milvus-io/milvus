package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestDial(t *testing.T) {
	paramtable.Init()

	c, _ := kvfactory.GetEtcdAndPath()
	assert.NotNil(t, c)

	client := NewClient(c)
	assert.NotNil(t, client)
	client.Close()
}

func TestMixCoordClientCredentials(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()

	setParam := func(t *testing.T, item *paramtable.ParamItem, value string) {
		t.Helper()
		old := item.SwapTempValue(value)
		t.Cleanup(func() { item.SwapTempValue(old) })
	}

	t.Run("external TLS mode keeps the client plaintext", func(t *testing.T) {
		setParam(t, &params.RootCoordGrpcClientCfg.TLSMode, "1")
		setParam(t, &params.InternalTLSCfg.InternalTLSEnabled, "false")
		setParam(t, &params.MixCoordCfg.ClientTLSEnabled, "false")
		setParam(t, &params.RootCoordGrpcClientCfg.CaPemPath, "/does/not/exist.pem")

		creds, err := getMixCoordClientCredentials(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "insecure", creds.Info().SecurityProtocol)
	})

	t.Run("directional TLS uses the external CA", func(t *testing.T) {
		setParam(t, &params.InternalTLSCfg.InternalTLSEnabled, "false")
		setParam(t, &params.MixCoordCfg.ClientTLSEnabled, "true")
		setParam(t, &params.RootCoordGrpcClientCfg.CaPemPath, "../../../configs/cert/ca.pem")
		setParam(t, &params.InternalTLSCfg.InternalTLSCaPemPath, "/does/not/exist.pem")

		creds, err := getMixCoordClientCredentials(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, "tls", creds.Info().SecurityProtocol)
	})

	t.Run("directional TLS fails closed for an invalid CA", func(t *testing.T) {
		setParam(t, &params.InternalTLSCfg.InternalTLSEnabled, "false")
		setParam(t, &params.MixCoordCfg.ClientTLSEnabled, "true")
		setParam(t, &params.RootCoordGrpcClientCfg.CaPemPath, "/does/not/exist.pem")

		creds, err := getMixCoordClientCredentials(context.Background())
		assert.Error(t, err)
		assert.Nil(t, creds)
	})
}
