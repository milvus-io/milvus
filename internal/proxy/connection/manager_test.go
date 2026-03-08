package connection

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestConnectionManager(t *testing.T) {
	paramtable.Init()

	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.ConnectionCheckIntervalSeconds.Key, "2")
	pt.Save(pt.ProxyCfg.ConnectionClientInfoTTLSeconds.Key, "1")
	defer pt.Reset(pt.ProxyCfg.ConnectionCheckIntervalSeconds.Key)
	defer pt.Reset(pt.ProxyCfg.ConnectionClientInfoTTLSeconds.Key)
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

func TestConnectionManager_Purge(t *testing.T) {
	paramtable.Init()

	pt := paramtable.Get()
	pt.Save(pt.ProxyCfg.ConnectionCheckIntervalSeconds.Key, "2")
	pt.Save(pt.ProxyCfg.MaxConnectionNum.Key, "2")
	defer pt.Reset(pt.ProxyCfg.ConnectionCheckIntervalSeconds.Key)
	defer pt.Reset(pt.ProxyCfg.MaxConnectionNum.Key)
	s := newConnectionManager()
	defer s.Stop()

	repeat := 10
	for i := 0; i < repeat; i++ {
		s.Register(context.TODO(), int64(i), &commonpb.ClientInfo{})
	}

	assert.Eventually(t, func() bool {
		return s.clientInfos.Len() <= 2
	}, time.Second*5, time.Second)
}
