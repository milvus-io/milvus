package milvus

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestSessionCleanupLegacyCoordinatorOwnership(t *testing.T) {
	cli := newSessionCleanupEtcd(t)
	cases := []struct {
		name     string
		sessions map[string]string
		want     []string
	}{
		{
			name: "all owned",
			sessions: map[string]string{
				"mixcoord-7": sessionCleanupJSON(7), "mixcoord": sessionCleanupJSON(7),
				"rootcoord": sessionCleanupJSON(7), "datacoord": sessionCleanupJSON(7), "querycoord": sessionCleanupJSON(7),
			},
			want: []string{"mixcoord-7", "mixcoord", "rootcoord", "datacoord", "querycoord"},
		},
		{
			name: "active key missing",
			sessions: map[string]string{
				"mixcoord-7": sessionCleanupJSON(7),
				"rootcoord":  sessionCleanupJSON(7), "datacoord": sessionCleanupJSON(7), "querycoord": sessionCleanupJSON(7),
			},
			want: []string{"mixcoord-7", "rootcoord", "datacoord", "querycoord"},
		},
		{
			name: "active key belongs to successor",
			sessions: map[string]string{
				"mixcoord-7": sessionCleanupJSON(7), "mixcoord": sessionCleanupJSON(9),
				"rootcoord": sessionCleanupJSON(7), "datacoord": sessionCleanupJSON(7), "querycoord": sessionCleanupJSON(7),
			},
			want: []string{"mixcoord-7", "rootcoord", "datacoord", "querycoord"},
		},
		{
			name: "active key corrupt",
			sessions: map[string]string{
				"mixcoord-7": sessionCleanupJSON(7), "mixcoord": "invalid JSON",
				"rootcoord": sessionCleanupJSON(7), "datacoord": sessionCleanupJSON(7), "querycoord": sessionCleanupJSON(7),
			},
			want: []string{"mixcoord-7", "rootcoord", "datacoord", "querycoord"},
		},
		{
			name: "skip foreign corrupt and missing legacy keys",
			sessions: map[string]string{
				"mixcoord-7": sessionCleanupJSON(7), "mixcoord": sessionCleanupJSON(7),
				"rootcoord": sessionCleanupJSON(9), "datacoord": "invalid JSON",
			},
			want: []string{"mixcoord-7", "mixcoord"},
		},
		{
			name: "skip legacy keys without valid owner",
			sessions: map[string]string{
				"mixcoord-7": sessionCleanupJSON(7), "mixcoord": sessionCleanupJSON(7),
				"rootcoord": "null", "datacoord": `{}`, "querycoord": `{"ServerID":"7"}`,
			},
			want: []string{"mixcoord-7", "mixcoord"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			prefix := path.Join("legacy-cleanup", t.Name())
			for suffix, value := range tc.sessions {
				_, err := cli.Put(ctx, path.Join(prefix, suffix), value)
				require.NoError(t, err)
			}
			targets, err := collectSessionCleanupTargets(ctx, cli, prefix, []string{"mixcoord-7"})
			require.NoError(t, err)
			wantKeys := make([]string, 0, len(tc.want))
			selected := make(map[string]bool, len(tc.want))
			for _, suffix := range tc.want {
				key := path.Join(prefix, suffix)
				wantKeys = append(wantKeys, key)
				selected[key] = true
			}
			gotKeys := make([]string, 0, len(targets))
			for _, target := range targets {
				gotKeys = append(gotKeys, target.key)
				resp, err := cli.Get(ctx, target.key)
				require.NoError(t, err)
				require.Len(t, resp.Kvs, 1)
				require.Equal(t, resp.Kvs[0].ModRevision, target.modRevision)
			}
			require.ElementsMatch(t, wantKeys, gotKeys)
			require.NoError(t, deleteSessionCleanupTargets(ctx, cli, targets))
			for suffix, value := range tc.sessions {
				key := path.Join(prefix, suffix)
				resp, err := cli.Get(ctx, key)
				require.NoError(t, err)
				if selected[key] {
					require.Empty(t, resp.Kvs, key)
				} else {
					require.Len(t, resp.Kvs, 1, key)
					require.Equal(t, value, string(resp.Kvs[0].Value), key)
				}
			}
		})
	}
}

func TestSessionCleanupLegacyPreservesChangedRevision(t *testing.T) {
	cli := newSessionCleanupEtcd(t)
	for _, role := range []string{"rootcoord", "datacoord", "querycoord"} {
		for _, change := range []string{"new owner", "same owner rewrite", "delete and recreate"} {
			t.Run(role+"/"+change, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				prefix := path.Join("legacy-cleanup", t.Name())
				suffixes := []string{"mixcoord-7", "mixcoord", "rootcoord", "datacoord", "querycoord"}
				for _, suffix := range suffixes {
					_, err := cli.Put(ctx, path.Join(prefix, suffix), sessionCleanupJSON(7))
					require.NoError(t, err)
				}
				targets, err := collectSessionCleanupTargets(ctx, cli, prefix, []string{"mixcoord-7"})
				require.NoError(t, err)
				require.Len(t, targets, len(suffixes))
				key := path.Join(prefix, role)
				value := sessionCleanupJSON(7)
				switch change {
				case "new owner":
					value = sessionCleanupJSON(9)
				case "delete and recreate":
					_, err = cli.Delete(ctx, key)
					require.NoError(t, err)
				}
				put, err := cli.Put(ctx, key, value)
				require.NoError(t, err)
				require.NoError(t, deleteSessionCleanupTargets(ctx, cli, targets))
				for _, suffix := range suffixes {
					resp, err := cli.Get(ctx, path.Join(prefix, suffix))
					require.NoError(t, err)
					if suffix == role {
						require.Len(t, resp.Kvs, 1)
						require.Equal(t, value, string(resp.Kvs[0].Value))
						require.Equal(t, put.Header.Revision, resp.Kvs[0].ModRevision)
					} else {
						require.Empty(t, resp.Kvs, suffix)
					}
				}
			})
		}
	}
}

func TestSessionCleanupLegacyLeaseRevocationAndTakeover(t *testing.T) {
	cli := newSessionCleanupEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := path.Join("legacy-cleanup", t.Name())
	roles := []string{"mixcoord", "rootcoord", "datacoord", "querycoord"}
	oldLease, err := cli.Grant(ctx, 30)
	require.NoError(t, err)
	for _, suffix := range append([]string{"mixcoord-7"}, roles...) {
		_, err := cli.Put(ctx, path.Join(prefix, suffix), sessionCleanupJSON(7), clientv3.WithLease(oldLease.ID))
		require.NoError(t, err)
	}
	targets, err := collectSessionCleanupTargets(ctx, cli, prefix, []string{"mixcoord-7"})
	require.NoError(t, err)
	require.Len(t, targets, 5)

	// The old supervisor pauses after reading ownership. Its lease is revoked,
	// and a standby acquires every coordinator key before cleanup resumes.
	_, err = cli.Revoke(ctx, oldLease.ID)
	require.NoError(t, err)
	newLease, err := cli.Grant(ctx, 30)
	require.NoError(t, err)
	comparisons := make([]clientv3.Cmp, 0, len(roles))
	puts := make([]clientv3.Op, 0, len(roles))
	for _, role := range roles {
		key := path.Join(prefix, role)
		comparisons = append(comparisons, clientv3.Compare(clientv3.Version(key), "=", 0))
		puts = append(puts, clientv3.OpPut(key, sessionCleanupJSON(9), clientv3.WithLease(newLease.ID)))
	}
	takeover, err := cli.Txn(ctx).If(comparisons...).Then(puts...).Commit()
	require.NoError(t, err)
	require.True(t, takeover.Succeeded)
	require.NoError(t, deleteSessionCleanupTargets(ctx, cli, targets))
	for _, role := range roles {
		resp, err := cli.Get(ctx, path.Join(prefix, role))
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1, role)
		require.Equal(t, sessionCleanupJSON(9), string(resp.Kvs[0].Value), role)
		require.Equal(t, int64(newLease.ID), resp.Kvs[0].Lease, role)
	}

	// Cleanup must not open a path for a third coordinator to activate.
	third, err := cli.Txn(ctx).If(comparisons...).Then(clientv3.OpPut(path.Join(prefix, "mixcoord"), sessionCleanupJSON(11))).Commit()
	require.NoError(t, err)
	require.False(t, third.Succeeded)
}

type legacyCleanupReadErrorKV struct {
	clientv3.KV
	key string
	err error
}

func (kv legacyCleanupReadErrorKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	if key == kv.key {
		return nil, kv.err
	}
	return kv.KV.Get(ctx, key, opts...)
}

func TestSessionCleanupLegacyReadErrorFailsClosed(t *testing.T) {
	cli := newSessionCleanupEtcd(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	prefix := path.Join("legacy-cleanup", t.Name())
	for _, suffix := range []string{"mixcoord-7", "mixcoord", "rootcoord", "datacoord", "querycoord"} {
		_, err := cli.Put(ctx, path.Join(prefix, suffix), sessionCleanupJSON(7))
		require.NoError(t, err)
	}
	injected := errors.New("legacy ownership read failed")
	targets, err := collectSessionCleanupTargets(ctx, legacyCleanupReadErrorKV{KV: cli, key: path.Join(prefix, "datacoord"), err: injected}, prefix, []string{"mixcoord-7"})
	require.ErrorIs(t, err, injected)
	require.Nil(t, targets)
	for _, suffix := range []string{"mixcoord-7", "mixcoord", "rootcoord", "datacoord", "querycoord"} {
		resp, err := cli.Get(ctx, path.Join(prefix, suffix))
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1, suffix)
		require.Equal(t, sessionCleanupJSON(7), string(resp.Kvs[0].Value))
	}
}
