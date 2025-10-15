package controllerimpl

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

func ListReplicatePChannels(ctx context.Context, etcdCli *clientv3.Client, prefix string) ([]*streamingpb.ReplicatePChannelMeta, int64, error) {
	resp, err := get(ctx, etcdCli, prefix)
	if err != nil {
		return nil, 0, err
	}
	keys := make([]string, 0, resp.Count)
	values := make([]string, 0, resp.Count)
	for _, kv := range resp.Kvs {
		keys = append(keys, string(kv.Key))
		values = append(values, string(kv.Value))
	}
	channels := make([]*streamingpb.ReplicatePChannelMeta, 0, len(values))
	for k, value := range values {
		info := &streamingpb.ReplicatePChannelMeta{}
		err = proto.Unmarshal([]byte(value), info)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "unmarshal replicate pchannel meta %s failed", keys[k])
		}
		channels = append(channels, info)
	}
	return channels, resp.Header.Revision, nil
}

func MustParseReplicateChannelFromEvent(e *clientv3.Event) *streamingpb.ReplicatePChannelMeta {
	meta := &streamingpb.ReplicatePChannelMeta{}
	err := proto.Unmarshal(e.Kv.Value, meta)
	if err != nil {
		panic(fmt.Sprintf("failed to unmarshal replicate pchannel meta: %v", err))
	}
	return meta
}

func get(ctx context.Context, etcdCli *clientv3.Client, prefix string) (*clientv3.GetResponse, error) {
	ctx1, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}
	return etcdCli.Get(ctx1, prefix, opts...)
}
