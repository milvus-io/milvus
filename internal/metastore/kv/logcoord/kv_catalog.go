package logcoord

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"
)

// NewCataLog creates a new catalog instance
func NewCataLog(metaKV kv.MetaKv) metastore.LogCoordCataLog {
	return &catalog{
		metaKV: metaKV,
	}
}

// catalog is a kv based catalog.
type catalog struct {
	metaKV kv.MetaKv
	prefix string
}

// ListPChannels returns all pchannels
func (c *catalog) ListPChannel(ctx context.Context) (map[string]*logpb.PChannelInfo, error) {
	_, values, err := c.metaKV.LoadWithPrefix(PChannelInfo)
	if err != nil {
		return nil, err
	}

	infos := make(map[string]*logpb.PChannelInfo)
	for _, value := range values {
		info := &logpb.PChannelInfo{}
		err = proto.Unmarshal([]byte(value), info)
		if err != nil {
			log.Error("unmarshal info failed when ListPChannelInfo", zap.Error(err))
			return nil, err
		}
		infos[info.GetName()] = info
	}
	return infos, nil
}

// SavePChannel saves a pchannel
func (c *catalog) SavePChannel(ctx context.Context, info *logpb.PChannelInfo) error {
	k := buildPChannelInfoPath(info.GetName())
	v, err := proto.Marshal(info)
	if err != nil {
		return err
	}
	return c.metaKV.Save(k, string(v))
}

// DropPChannel drops a pchannel
func (c *catalog) DropPChannel(ctx context.Context, name string) error {
	k := buildPChannelInfoPath(name)
	return c.metaKV.Remove(k)
}

// buildPChannelInfoPath builds the path for pchannel info.
func buildPChannelInfoPath(name string) string {
	return PChannelInfo + "/" + name
}
