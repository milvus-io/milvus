//go:build test && dynamic

package metacache

import (
	"context"
	"slices"
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
func MockPublishedSchemaForTest(t testing.TB) {
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
		response.Schema.Fields = slices.DeleteFunc(response.Schema.Fields, func(field *schemapb.FieldSchema) bool {
			return common.IsSystemField(field.GetFieldID())
		})
		err = m.SeedSchemaForTest(response.GetCollectionID(), response.GetSchema())
		if err != nil {
			return nil, err
		}
		return response, nil
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

// SeedSchemaForTest avoids runtime patching in tests which exercise concurrent
// metadata fills for multiple collections.
func (m *MetaCache) SeedSchemaForTest(id int64, schema *schemapb.CollectionSchema) error {
	body, err := NewSchemaInfo(proto.Clone(schema).(*schemapb.CollectionSchema))
	if err != nil {
		return err
	}
	m.schemaBodies.LoadOrStore(schemaIdentity{id, schema.GetVersion()}, body)
	return nil
}
