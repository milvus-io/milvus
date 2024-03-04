package connection

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestConnectionManager(t *testing.T) {
	paramtable.Init()

	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.ConnectionCheckInterval.Key, "2")
	pt.Save(pt.ProxyCfg.ConnectionClientInfoTTL.Key, "1")
	defer pt.Reset(pt.ProxyCfg.ConnectionCheckInterval.Key)
	defer pt.Reset(pt.ProxyCfg.ConnectionClientInfoTTL.Key)
	s := newConnectionManager()
	defer s.Stop()

	s.Register(context.TODO(), 1, &commonpb.ClientInfo{
		Reserved: map[string]string{"for_test": "for_test"},
	})
	assert.Equal(t, 1, len(s.List()))

	// register duplicate.
	s.Register(context.TODO(), 1, &commonpb.ClientInfo{})
	assert.Equal(t, 1, len(s.List()))

	s.Register(context.TODO(), 2, &commonpb.ClientInfo{})
	assert.Equal(t, 2, len(s.List()))

	s.KeepActive(1)
	s.KeepActive(2)

	time.Sleep(time.Millisecond * 5)
	assert.Equal(t, 2, len(s.List()))

	assert.Eventually(t, func() bool {
		return len(s.List()) == 0
	}, time.Second*5, time.Second)
}
