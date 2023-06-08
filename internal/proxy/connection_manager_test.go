package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

	"github.com/stretchr/testify/assert"
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
	s := GetConnectionManager()
	assert.Equal(t, defaultConnCheckDuration, s.duration)
	assert.Equal(t, defaultTTLForInactiveConn, s.ttl)
}

func TestConnectionManager(t *testing.T) {
	s := newConnectionManager(
		withDuration(time.Millisecond*5),
		withTTL(time.Millisecond*100))

	s.register(context.TODO(), 1, &commonpb.ClientInfo{
		Reserved: map[string]string{"for_test": "for_test"},
	})
	assert.Equal(t, 1, len(s.list()))

	// register duplicate.
	s.register(context.TODO(), 1, &commonpb.ClientInfo{})
	assert.Equal(t, 1, len(s.list()))

	s.register(context.TODO(), 2, &commonpb.ClientInfo{})
	assert.Equal(t, 2, len(s.list()))

	s.keepActive(1)
	s.keepActive(2)

	time.Sleep(time.Millisecond * 5)
	assert.Equal(t, 2, len(s.list()))

	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 0, len(s.list()))

	s.stop()

	time.Sleep(time.Millisecond * 5)
}
