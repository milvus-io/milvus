//go:build test && dynamic

package metacache

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// Existing Proxy fixtures model a steady-state schema. Dedicated version and
// refresh tests mock SchemaClient instead of these fixture adapters.
var (
	fixtureMu         sync.Mutex
	fixtureOwners     = make(map[testing.TB]bool)
	fixtureWritePatch *mockey.Mocker
)

func MockWriteSchemaForTest(t testing.TB) {
	t.Helper()
	fixtureMu.Lock()
	defer fixtureMu.Unlock()
	if fixtureOwners[t] {
		fixtureWritePatch.Patch()
		return
	}
	if len(fixtureOwners) == 0 {
		fixtureWritePatch = mockey.Mock((*CollectionInfo).GetWriteSchema).To(func(info *CollectionInfo, _ context.Context) (*SchemaInfo, error) {
			return info.Schema, nil
		}).Build()
	}
	fixtureWritePatch.Patch()
	fixtureOwners[t] = true
	t.Cleanup(func() {
		fixtureMu.Lock()
		defer fixtureMu.Unlock()
		delete(fixtureOwners, t)
		if len(fixtureOwners) == 0 {
			fixtureWritePatch.UnPatch()
		}
	})
}

func MockPublishedSchemaForTest(t testing.TB) {
	t.Helper()
	patch := mockey.Mock(loadCollectionSchema).To(func(_ context.Context, response *milvuspb.DescribeCollectionResponse) (*SchemaInfo, error) {
		return NewSchemaInfo(response.Schema)
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
}
