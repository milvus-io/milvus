package discoverer

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/json"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestSessionDiscovererClearsStaleSessionsOnRetry(t *testing.T) {
	etcdClient, _ := kvfactory.GetEtcdAndPath()
	prefix := funcutil.RandomString(10) + "/"
	ctx := context.Background()

	// Put 3 sessions into etcd.
	sessions := map[int64]*sessionutil.SessionRaw{
		1: {ServerID: 1, Address: "127.0.0.1:12345", Version: "0.2.0"},
		2: {ServerID: 2, Address: "127.0.0.1:12346", Version: "0.3.0"},
		3: {ServerID: 3, Address: "127.0.0.1:12347", Version: "0.4.0"},
	}
	for id, s := range sessions {
		val, err := json.Marshal(s)
		assert.NoError(t, err)
		_, err = etcdClient.Put(ctx, fmt.Sprintf("%s%d", prefix, id), string(val))
		assert.NoError(t, err)
	}

	d := NewSessionDiscoverer(etcdClient, OptSDPrefix(prefix), OptSDVersionRange(">=0.1.0"))

	// First initDiscover: should see all 3 sessions.
	err := d.initDiscover(ctx)
	assert.NoError(t, err)
	assert.Len(t, d.peerSessions, 3)

	// Delete session 2 from etcd (simulating a node going down during watch gap).
	_, err = etcdClient.Delete(ctx, fmt.Sprintf("%s%d", prefix, 2))
	assert.NoError(t, err)

	// Second initDiscover (retry after watch break): stale session 2 must be gone.
	err = d.initDiscover(ctx)
	assert.NoError(t, err)
	assert.Len(t, d.peerSessions, 2)
	for _, s := range d.peerSessions {
		assert.NotEqual(t, int64(2), s.ServerID, "stale session should have been cleared")
	}
}

func TestSessionDiscoverer(t *testing.T) {
	etcdClient, _ := kvfactory.GetEtcdAndPath()
	targetVersion := "0.1.0"
	prefix := funcutil.RandomString(10) + "/"
	d := NewSessionDiscoverer(etcdClient, OptSDPrefix(prefix), OptSDVersionRange(">="+targetVersion))

	expected := []map[int64]*sessionutil.SessionRaw{
		{},
		{
			1: {ServerID: 1, Address: "127.0.0.1:12345", Version: "0.2.0"},
		},
		{
			1: {ServerID: 1, Address: "127.0.0.1:12345", Version: "0.2.0"},
			2: {ServerID: 2, Address: "127.0.0.1:12346", Version: "0.4.0"},
		},
		{
			1: {ServerID: 1, Address: "127.0.0.1:12345", Version: "0.2.0"},
			2: {ServerID: 2, Address: "127.0.0.1:12346", Version: "0.4.0"},
			3: {ServerID: 3, Address: "127.0.0.1:12347", Version: "0.3.0"},
		},
		{
			1: {ServerID: 1, Address: "127.0.0.1:12345", Version: "0.2.0"},
			2: {ServerID: 2, Address: "127.0.0.1:12346", Version: "0.4.0"},
			3: {ServerID: 3, Address: "127.0.0.1:12347", Version: "0.3.0", Stopping: true},
		},
		{
			1: {ServerID: 1, Address: "127.0.0.1:12345", Version: "0.2.0"},
			2: {ServerID: 2, Address: "127.0.0.1:12346", Version: "0.4.0"},
			3: {ServerID: 3, Address: "127.0.0.1:12347", Version: "0.3.0"},
			4: {ServerID: 4, Address: "127.0.0.1:12348", Version: "0.0.1"}, // version filtering
		},
	}

	idx := 0
	var lastVersion typeutil.Version = typeutil.VersionInt64(-1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := d.Discover(ctx, func(state VersionedState) error {
		sessions := state.Sessions()

		expectedSessions := make(map[int64]*sessionutil.SessionRaw, len(expected[idx]))
		for k, v := range expected[idx] {
			if semver.MustParse(v.Version).GT(semver.MustParse(targetVersion)) {
				expectedSessions[k] = v
			}
		}
		assert.Equal(t, expectedSessions, sessions)
		assert.True(t, state.Version.GT(lastVersion))

		lastVersion = state.Version
		if idx < len(expected)-1 {
			ops := make([]clientv3.Op, 0, len(expected[idx+1]))
			for k, v := range expected[idx+1] {
				sessionStr, err := json.Marshal(v)
				assert.NoError(t, err)
				ops = append(ops, clientv3.OpPut(fmt.Sprintf("%s%d", prefix, k), string(sessionStr)))
			}

			resp, err := etcdClient.Txn(ctx).Then(
				ops...,
			).Commit()
			assert.NoError(t, err)
			assert.NotNil(t, resp)
			idx++
			return nil
		}
		return io.EOF
	})
	assert.ErrorIs(t, err, io.EOF)

	// Do a init discover here.
	d = NewSessionDiscoverer(etcdClient, OptSDPrefix(prefix), OptSDVersionRange(">="+targetVersion))
	err = d.Discover(ctx, func(state VersionedState) error {
		// balance attributes
		sessions := state.Sessions()
		expectedSessions := make(map[int64]*sessionutil.SessionRaw, len(expected[idx]))
		for k, v := range expected[idx] {
			if semver.MustParse(v.Version).GT(semver.MustParse(targetVersion)) {
				expectedSessions[k] = v
			}
		}
		assert.Equal(t, expectedSessions, sessions)

		// resolver attributes
		for _, addr := range state.State.Addresses {
			serverID := attributes.GetServerID(addr.Attributes)
			assert.NotNil(t, serverID)
		}
		return io.EOF
	})
	assert.ErrorIs(t, err, io.EOF)

	d = NewSessionDiscoverer(etcdClient, OptSDPrefix(prefix), OptSDVersionRange(">="+targetVersion), OptSDForcePort(12345))
	err = d.Discover(ctx, func(state VersionedState) error {
		// balance attributes
		expectedSessions := make(map[int64]*sessionutil.SessionRaw, len(expected[idx]))
		for k, v := range expected[idx] {
			if semver.MustParse(v.Version).GT(semver.MustParse(targetVersion)) {
				expectedSessions[k] = v
			}
		}
		assert.NotZero(t, len(expectedSessions))

		// resolver attributes
		for _, addr := range state.State.Addresses {
			serverID := attributes.GetServerID(addr.Attributes)
			assert.NotNil(t, serverID)
			_, port, err := net.SplitHostPort(addr.Addr)
			assert.NoError(t, err)
			assert.Equal(t, "12345", port)
		}
		return io.EOF
	})
	assert.ErrorIs(t, err, io.EOF)
}
