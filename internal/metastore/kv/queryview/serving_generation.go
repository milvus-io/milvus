package queryview

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ServingGenerationCatalog persists cluster-local read publication independently
// of RootCoord's replicated routing and of individual QueryView cleanup.
type ServingGenerationCatalog interface {
	ListServingGenerations(context.Context) ([]*viewpb.ServingGeneration, error)
	SaveServingGeneration(context.Context, *viewpb.ServingGeneration) error
	RemoveServingGeneration(context.Context, int64) error
}

func (c *queryViewCatalog) servingPrefix() string {
	return strings.TrimSuffix(c.prefix, queryViewKeyPrefix) + "qv-serving/"
}

func (c *queryViewCatalog) ListServingGenerations(ctx context.Context) ([]*viewpb.ServingGeneration, error) {
	_, values, err := c.metaKV.LoadWithPrefix(ctx, c.servingPrefix())
	if err != nil {
		return nil, err
	}
	result := make([]*viewpb.ServingGeneration, 0, len(values))
	for _, value := range values {
		generation := &viewpb.ServingGeneration{}
		if err := proto.Unmarshal([]byte(value), generation); err != nil {
			return nil, merr.Wrap(err, "decode serving generation")
		}
		result = append(result, generation)
	}
	return result, nil
}

func (c *queryViewCatalog) SaveServingGeneration(ctx context.Context, generation *viewpb.ServingGeneration) error {
	encoded, err := proto.Marshal(generation)
	if err != nil {
		return merr.Wrap(err, "encode serving generation")
	}
	return c.metaKV.Save(ctx, c.servingPrefix()+strconv.FormatInt(generation.GetCollectionId(), 10), string(encoded))
}

func (c *queryViewCatalog) RemoveServingGeneration(ctx context.Context, collectionID int64) error {
	return c.metaKV.Remove(ctx, c.servingPrefix()+strconv.FormatInt(collectionID, 10))
}
