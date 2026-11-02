//go:build test && dynamic

package metacache

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// MockPublishedSchemaForTest supplies immutable schema bodies for metadata-only
// tests whose DescribeCollection fixture also describes the published schema.
// It leaves collection resolution, invalidation and authorization intact. Tests
// of read/write version selection must mock the streaming schema client instead.
type fixtureSchemaKey struct {
	id      int64
	version int32
}

var fixtureSchemas sync.Map

var (
	fixtureMu     sync.Mutex
	fixtureOwners = make(map[testing.TB]bool)
	fixturePatch  *mockey.Mocker
)

func prepareFixtureSchemas(t testing.TB) {
	t.Helper()
	fixtureMu.Lock()
	defer fixtureMu.Unlock()
	if fixtureOwners[t] {
		fixturePatch.Patch() // PatchConvey may have ended the previous scope.
		return
	}
	if len(fixtureOwners) == 0 {
		fixtureSchemas.Clear()
		fixturePatch = mockey.Mock(resolveSchema).To(func(ctx context.Context, info *CollectionInfo, write bool) (*SchemaInfo, error) {
			schema, ok := fixtureSchemas.Load(info)
			if !ok {
				schema, ok = fixtureSchemas.Load(fixtureSchemaKey{info.CollID, info.ReadSchemaVersion})
			}
			if !ok || schema == nil {
				return nil, merr.WrapErrServiceInternalMsg("test schema not seeded for collection %d", info.CollID)
			}
			return withCollectionMetadata(schema.(*SchemaInfo), info), nil
		}).Build()
	}
	fixturePatch.Patch()
	fixtureOwners[t] = true
	t.Cleanup(func() {
		fixtureMu.Lock()
		defer fixtureMu.Unlock()
		delete(fixtureOwners, t)
		if len(fixtureOwners) == 0 {
			fixturePatch.UnPatch()
			fixtureSchemas.Clear()
		}
	})
}

func MockPublishedSchemaForTest(t testing.TB) {
	prepareFixtureSchemas(t)
	t.Helper()
	patch := mockey.Mock((*MetaCache).describeCollection).To(func(m *MetaCache, ctx context.Context, database, name string, id int64) (*milvuspb.DescribeCollectionResponse, error) {
		response, err := m.mixCoord.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
			Base:   commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_DescribeCollection)),
			DbName: database, CollectionName: name, CollectionID: id,
		})
		if err := merr.CheckRPCCall(response, err); err != nil {
			return nil, err
		}
		response = proto.Clone(response).(*milvuspb.DescribeCollectionResponse)
		if response.Schema == nil {
			return response, nil
		}
		response.Schema.Fields = slices.DeleteFunc(response.Schema.Fields, func(field *schemapb.FieldSchema) bool {
			return common.IsSystemField(field.GetFieldID())
		})
		err = m.SeedSchemaForTest(t, response.GetCollectionID(), response.GetSchema())
		if err != nil {
			return nil, err
		}
		return response, nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

// SeedSchemaForTest avoids runtime patching in tests which exercise concurrent
// metadata fills for multiple collections.
func (m *MetaCache) SeedSchemaForTest(t testing.TB, id int64, schema *schemapb.CollectionSchema) error {
	prepareFixtureSchemas(t)
	body, err := NewSchemaInfo(proto.Clone(schema).(*schemapb.CollectionSchema))
	if err != nil {
		return err
	}
	fixtureSchemas.Store(fixtureSchemaKey{id, schema.GetVersion()}, body)
	return nil
}
