package queryview

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// LoadInfoCatalog retains immutable loading requirements referenced by views.
// Historical snapshot GC is intentionally deferred.
type LoadInfoCatalog interface {
	GetLoadInfo(context.Context, int64, uint64) (*querypb.GetQueryViewLoadInfoResponse, error)
	SaveLoadInfo(context.Context, *querypb.GetQueryViewLoadInfoResponse) error
}

type loadInfoCatalog struct{ storage kv.MetaKv }

func NewLoadInfoCatalog(storage kv.MetaKv) LoadInfoCatalog {
	return &loadInfoCatalog{storage: kv.NewReliableWriteMetaKv(storage)}
}

func loadInfoKey(collectionID int64, version uint64) string {
	return fmt.Sprintf("coord/qv-load-info/%d/%d", collectionID, version)
}

func (c *loadInfoCatalog) GetLoadInfo(ctx context.Context, collectionID int64, version uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
	value, err := c.storage.Load(ctx, loadInfoKey(collectionID, version))
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	info := new(querypb.GetQueryViewLoadInfoResponse)
	if err := proto.Unmarshal([]byte(value), info); err != nil {
		return nil, merr.Wrap(err, "decode query view load snapshot")
	}
	if info.GetCollectionID() != collectionID || info.GetVersion() != version {
		return nil, merr.WrapErrDataIntegrityMsg("query view load snapshot identity does not match its catalog key")
	}
	return info, nil
}

func (c *loadInfoCatalog) SaveLoadInfo(ctx context.Context, info *querypb.GetQueryViewLoadInfoResponse) error {
	value, err := proto.Marshal(info)
	if err != nil {
		return merr.Wrap(err, "encode query view load snapshot")
	}
	return c.storage.Save(ctx, loadInfoKey(info.GetCollectionID(), info.GetVersion()), string(value))
}
