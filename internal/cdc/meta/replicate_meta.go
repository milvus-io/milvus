package meta

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

const requestTimeout = 30 * time.Second

type ReplicateChannel struct {
	Key         string
	Value       *streamingpb.ReplicatePChannelMeta
	ModRevision int64
}

type ReplicateChannels struct {
	Channels []*ReplicateChannel
	Revision int64
}

// ListReplicatePChannels lists the replicate pchannels metadata from metastore.
func ListReplicatePChannels(ctx context.Context, etcdCli *clientv3.Client, prefix string) (*ReplicateChannels, error) {
	resp, err := listByPrefix(ctx, etcdCli, prefix)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, resp.Count)
	values := make([]string, 0, resp.Count)
	revisions := make([]int64, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, string(kv.Value))
		revisions = append(revisions, kv.ModRevision)
	}
	channels := make([]*ReplicateChannel, 0, len(values))
	for k, value := range values {
		meta := &streamingpb.ReplicatePChannelMeta{}
		err = proto.Unmarshal([]byte(value), meta)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal replicate pchannel meta %s failed", keys[k])
		}
		channel := &ReplicateChannel{
			Key:         keys[k],
			Value:       meta,
			ModRevision: revisions[k],
		}
		channels = append(channels, channel)
	}
	return &ReplicateChannels{
		Channels: channels,
		Revision: resp.Header.Revision,
	}, nil
}

// RemoveReplicatePChannelWithRevision removes the replicate pchannel from metastore.
// Remove the task of CDC replication task of current cluster, should be called when a CDC replication task is finished.
// Because CDC removes the replicate pchannel meta asynchronously, there may be ordering conflicts
// during primary-secondary switches. For example:
// 1. Init: A -> B, Save key
// 2. Switch: B -> A, Delete key asynchronously
// 3. Switch again: A -> B, Save key
// Expected meta op order: [Save, Delete, Save]
// Actual meta op may be: [Save, Save, Delete]
// To avoid ordering conflicts, we need to pass the key’s revision
// and only remove the KV when the revision matches.
func RemoveReplicatePChannelWithRevision(ctx context.Context, etcdCli *clientv3.Client, key string, revision int64) (bool, error) {
	result, err := removeWithCmps(ctx, etcdCli, key, clientv3.Compare(clientv3.ModRevision(key), "=", revision))
	return result, err
}

func MustParseReplicateChannelFromEvent(e *clientv3.Event) *streamingpb.ReplicatePChannelMeta {
	meta := &streamingpb.ReplicatePChannelMeta{}
	err := proto.Unmarshal(e.Kv.Value, meta)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal replicate pchannel meta: %v", err))
	}
	return meta
}

// listByPrefix gets the key-value pairs with the given prefix.
func listByPrefix(ctx context.Context, etcdCli *clientv3.Client, prefix string) (*clientv3.GetResponse, error) {
	ctx1, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}
	return etcdCli.Get(ctx1, prefix, opts...)
}

// removeWithCmps removes the key with given cmps.
func removeWithCmps(ctx context.Context, etcdCli *clientv3.Client, key string, cmps ...clientv3.Cmp) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	delOp := clientv3.OpDelete(key)
	txn := etcdCli.Txn(ctx).If(cmps...).Then(delOp)
	resp, err := txn.Commit()
	if err != nil {
		return false, err
	}
	return resp.Succeeded, nil
}
