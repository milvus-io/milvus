package streamingcoord

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/kv"
)

// NewCataLog creates a new catalog instance
func NewCataLog(metaKV kv.MetaKv) metastore.StreamingCoordCataLog {
	return &catalog{
		metaKV: metaKV,
	}
}

// catalog is a kv based catalog.
type catalog struct {
	metaKV kv.MetaKv
}

// ListPChannels returns all pchannels
func (c *catalog) ListPChannel(ctx context.Context) ([]*streamingpb.PChannelMeta, error) {
	keys, values, err := c.metaKV.LoadWithPrefix(PChannelMeta)
	if err != nil {
		return nil, err
	}

	infos := make([]*streamingpb.PChannelMeta, 0, len(values))
	for k, value := range values {
		info := &streamingpb.PChannelMeta{}
		err = proto.Unmarshal([]byte(value), info)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal pchannel %s failed", keys[k])
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// SavePChannels saves a pchannel
func (c *catalog) SavePChannels(ctx context.Context, infos []*streamingpb.PChannelMeta) error {
	kvs := make(map[string]string, len(infos))
	for _, info := range infos {
		key := buildPChannelInfoPath(info.GetChannel().GetName())
		v, err := proto.Marshal(info)
		if err != nil {
			return errors.Wrapf(err, "marshal pchannel %s failed", info.GetChannel().GetName())
		}
		kvs[key] = string(v)
	}
	return c.metaKV.MultiSave(kvs)
}

// buildPChannelInfoPath builds the path for pchannel info.
func buildPChannelInfoPath(name string) string {
	return PChannelMeta + "/" + name
}
