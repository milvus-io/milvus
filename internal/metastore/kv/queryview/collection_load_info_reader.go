package queryview

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const VersionedLoadInfoPrefix = "coord/load_info"

// GetCollectionLoadInfo reads the exact requirements published by the QueryView
// producer. Publication, reference tracking and GC belong to that producer.
func GetCollectionLoadInfo(ctx context.Context, storage kv.MetaKv, collectionID int64, version uint64) (*viewpb.CollectionLoadConfig, error) {
	key := fmt.Sprintf("%s/%d/%d", VersionedLoadInfoPrefix, collectionID, version)
	value, err := storage.Load(ctx, key)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	info := new(viewpb.CollectionLoadConfig)
	if err := proto.Unmarshal([]byte(value), info); err != nil {
		return nil, merr.Wrap(err, "decode collection load requirements")
	}
	if info.GetCollectionID() != collectionID || info.GetVersion() != version || version == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("collection load info identity does not match its catalog key")
	}
	return info, nil
}
