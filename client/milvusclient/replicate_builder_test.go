package milvusclient_test

import (
	"testing"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/stretchr/testify/assert"
)

func TestMilvusClusterBuilder(t *testing.T) {
	t.Run("default_values", func(t *testing.T) {
		cluster := milvusclient.NewMilvusClusterBuilder("test-cluster").Build()
		assert.Equal(t, "test-cluster", cluster.ClusterId)
		assert.Equal(t, "localhost:19530", cluster.ConnectionParam.Uri)
		assert.Equal(t, "", cluster.ConnectionParam.Token)
		assert.Empty(t, cluster.Pchannels)
		assert.Equal(t, "", cluster.ConnectionParam.GetCaPemPath())
		assert.Equal(t, "", cluster.ConnectionParam.GetClientPemPath())
		assert.Equal(t, "", cluster.ConnectionParam.GetClientKeyPath())
	})

	t.Run("with_all_options", func(t *testing.T) {
		cluster := milvusclient.NewMilvusClusterBuilder("my-cluster").
			WithURI("remote:19531").
			WithToken("my-token").
			WithPchannels("ch1", "ch2").
			Build()
		assert.Equal(t, "my-cluster", cluster.ClusterId)
		assert.Equal(t, "remote:19531", cluster.ConnectionParam.Uri)
		assert.Equal(t, "my-token", cluster.ConnectionParam.Token)
		assert.Equal(t, []string{"ch1", "ch2"}, cluster.Pchannels)
	})

	t.Run("chained_pchannels", func(t *testing.T) {
		cluster := milvusclient.NewMilvusClusterBuilder("c1").
			WithPchannels("ch1").
			WithPchannels("ch2", "ch3").
			Build()
		assert.Equal(t, []string{"ch1", "ch2", "ch3"}, cluster.Pchannels)
	})

	t.Run("with_tls_paths", func(t *testing.T) {
		cluster := milvusclient.NewMilvusClusterBuilder("secure-cluster").
			WithURI("https://target:19530").
			WithToken("my-token").
			WithTLS("/certs/ca.pem", "/certs/client.pem", "/certs/client.key").
			WithPchannels("ch1").
			Build()
		assert.Equal(t, "/certs/ca.pem", cluster.ConnectionParam.GetCaPemPath())
		assert.Equal(t, "/certs/client.pem", cluster.ConnectionParam.GetClientPemPath())
		assert.Equal(t, "/certs/client.key", cluster.ConnectionParam.GetClientKeyPath())
	})

	t.Run("with_ca_only", func(t *testing.T) {
		cluster := milvusclient.NewMilvusClusterBuilder("one-way-tls").
			WithTLS("/certs/ca.pem", "", "").
			Build()
		assert.Equal(t, "/certs/ca.pem", cluster.ConnectionParam.GetCaPemPath())
		assert.Equal(t, "", cluster.ConnectionParam.GetClientPemPath())
		assert.Equal(t, "", cluster.ConnectionParam.GetClientKeyPath())
	})
}
