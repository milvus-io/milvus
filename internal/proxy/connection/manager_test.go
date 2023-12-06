package connection

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func Test_withDuration(t *testing.T) {
	s := &connectionManager{}
	s.apply(withDuration(defaultConnCheckDuration))
	assert.Equal(t, defaultConnCheckDuration, s.duration)
}

func Test_withTTL(t *testing.T) {
	s := &connectionManager{}
	s.apply(withTTL(defaultTTLForInactiveConn))
	assert.Equal(t, defaultTTLForInactiveConn, s.ttl)
}

func Test_connectionManager_apply(t *testing.T) {
	s := &connectionManager{}
	s.apply(
		withDuration(defaultConnCheckDuration),
		withTTL(defaultTTLForInactiveConn))
	assert.Equal(t, defaultConnCheckDuration, s.duration)
	assert.Equal(t, defaultTTLForInactiveConn, s.ttl)
}

func TestGetConnectionManager(t *testing.T) {
	s := GetManager()
	assert.Equal(t, defaultConnCheckDuration, s.duration)
	assert.Equal(t, defaultTTLForInactiveConn, s.ttl)
}

func TestConnectionManager(t *testing.T) {
	s := newConnectionManager(
		withDuration(time.Millisecond*5),
		withTTL(time.Millisecond*100))

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

	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 0, len(s.List()))

	s.Stop()

	time.Sleep(time.Millisecond * 5)
}
