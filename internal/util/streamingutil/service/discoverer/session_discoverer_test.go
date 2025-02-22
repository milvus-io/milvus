package discoverer

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/blang/semver/v4"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestSessionDiscoverer(t *testing.T) {
	err := etcd.InitEtcdServer(true, "", t.TempDir(), "stdout", "info")
	assert.NoError(t, err)
	defer etcd.StopEtcdServer()

	etcdClient, err := etcd.GetEmbedEtcdClient()
	assert.NoError(t, err)
	targetVersion := "0.1.0"
	d := NewSessionDiscoverer(etcdClient, "session/", false, targetVersion)

	s := d.NewVersionedState()
	assert.True(t, s.Version.EQ(typeutil.VersionInt64(-1)))

	expected := []map[int64]*sessionutil.SessionRaw{
		{},
		{
			1: {ServerID: 1, Version: "0.2.0"},
		},
		{
			1: {ServerID: 1, Version: "0.2.0"},
			2: {ServerID: 2, Version: "0.4.0"},
		},
		{
			1: {ServerID: 1, Version: "0.2.0"},
			2: {ServerID: 2, Version: "0.4.0"},
			3: {ServerID: 3, Version: "0.3.0"},
		},
		{
			1: {ServerID: 1, Version: "0.2.0"},
			2: {ServerID: 2, Version: "0.4.0"},
			3: {ServerID: 3, Version: "0.3.0", Stopping: true},
		},
		{
			1: {ServerID: 1, Version: "0.2.0"},
			2: {ServerID: 2, Version: "0.4.0"},
			3: {ServerID: 3, Version: "0.3.0"},
			4: {ServerID: 4, Version: "0.0.1"}, // version filtering
		},
	}

	idx := 0
	var lastVersion typeutil.Version = typeutil.VersionInt64(-1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = d.Discover(ctx, func(state VersionedState) error {
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
				ops = append(ops, clientv3.OpPut(fmt.Sprintf("session/%d", k), string(sessionStr)))
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
	d = NewSessionDiscoverer(etcdClient, "session/", false, targetVersion)
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
}
