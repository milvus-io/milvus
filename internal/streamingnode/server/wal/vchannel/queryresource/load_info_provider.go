package queryresource

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type futureLoadInfoProvider struct {
	client *syncutil.Future[types.MixCoordClient]
}

func NewFutureLoadInfoProvider(client *syncutil.Future[types.MixCoordClient]) LoadInfoProvider {
	if client == nil {
		return nil
	}
	return &futureLoadInfoProvider{client: client}
}

func (p *futureLoadInfoProvider) QueryViewLoadInfo(ctx context.Context, collectionID int64, version uint64) (QueryViewLoadInfo, error) {
	client, err := p.client.GetWithContext(ctx)
	if err != nil {
		return QueryViewLoadInfo{}, err
	}
	resp, err := client.GetQueryViewLoadInfo(ctx, &querypb.GetQueryViewLoadInfoRequest{
		CollectionID: collectionID,
		Version:      version,
	})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return QueryViewLoadInfo{}, err
	}
	if resp.GetCollectionID() != collectionID {
		return QueryViewLoadInfo{}, errors.Errorf("query view load info collection mismatch: expected %d, got %d", collectionID, resp.GetCollectionID())
	}
	return QueryViewLoadInfo{
		PartitionIDs: append([]int64(nil), resp.GetPartitionIDs()...),
		LoadFields:   nonNilLoadFields(resp.GetLoadFields()),
		IndexInfos:   nonNilIndexInfos(resp.GetIndexInfoList()),
	}, nil
}

func nonNilLoadFields(fields []*messagespb.LoadFieldConfig) []*messagespb.LoadFieldConfig {
	nonNil := make([]*messagespb.LoadFieldConfig, 0, len(fields))
	for _, field := range fields {
		if field != nil {
			nonNil = append(nonNil, field)
		}
	}
	return nonNil
}

func nonNilIndexInfos(infos []*indexpb.IndexInfo) []*indexpb.IndexInfo {
	nonNil := make([]*indexpb.IndexInfo, 0, len(infos))
	for _, info := range infos {
		if info != nil {
			nonNil = append(nonNil, info)
		}
	}
	return nonNil
}
