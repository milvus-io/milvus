package datacoord

import (
	"testing"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/stretchr/testify/assert"
	"stathat.com/c/consistent"
)

func TestReload(t *testing.T) {
	t.Run("test reload with data", func(t *testing.T) {
		Params.Init()
		kv := memkv.NewMemoryKV()
		hash := consistent.New()
		cm, err := NewChannelManager(kv, &dummyPosProvider{}, withFactory(NewConsistentHashChannelPolicyFactory(hash)))
		assert.Nil(t, err)
		assert.Nil(t, cm.AddNode(1))
		assert.Nil(t, cm.AddNode(2))
		assert.Nil(t, cm.Watch(&channel{"channel1", 1}))
		assert.Nil(t, cm.Watch(&channel{"channel2", 1}))

		hash2 := consistent.New()
		cm2, err := NewChannelManager(kv, &dummyPosProvider{}, withFactory(NewConsistentHashChannelPolicyFactory(hash2)))
		assert.Nil(t, err)
		assert.Nil(t, cm2.Startup([]int64{1, 2}))
		assert.Nil(t, cm2.AddNode(3))
		assert.True(t, cm2.Match(3, "channel1"))
		assert.True(t, cm2.Match(3, "channel2"))
	})
}
