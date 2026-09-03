package checkers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// drainingDrainer answers the one question the index checker asks. The other
// methods are never reached from here; they panic so a seam wired to the wrong
// method fails loudly.
type drainingDrainer struct{ draining map[int64]bool }

func (d drainingDrainer) CollectionDraining(_ context.Context, collectionID int64) bool {
	return d.draining[collectionID]
}

func (drainingDrainer) AllowVectorIndexDropWhileLoaded(context.Context, int64, string) bool {
	panic("the index checker must not consult the drop admission check")
}

func (drainingDrainer) BeginDropIndex(context.Context, *indexpb.DropIndexRequest) bool {
	panic("the index checker must not classify drops")
}

func (drainingDrainer) AfterDropIndex(context.Context, *indexpb.DropIndexRequest) {
	panic("the index checker must not report drops")
}

func (drainingDrainer) AbortDropIndex(context.Context, *indexpb.DropIndexRequest) {
	panic("the index checker must not abort drops")
}

func (drainingDrainer) AfterCreateIndex(context.Context, *indexpb.CreateIndexRequest) {
	panic("the index checker must not report creates")
}

type checkerDrainProvider struct{ drainer extension.IndexDrainer }

func (checkerDrainProvider) Name() string                       { return "test" }
func (checkerDrainProvider) Requires() []extension.CapabilityID { return nil }
func (p checkerDrainProvider) Capabilities() extension.Capabilities {
	return extension.Capabilities{IndexDrain: p.drainer}
}

func installCheckerDrainer(t *testing.T, drainer extension.IndexDrainer) {
	t.Helper()
	extension.ResetForTest()
	t.Cleanup(extension.ResetForTest)
	require.NoError(t, extension.SetProvider(checkerDrainProvider{drainer: drainer}))
}

// A collection mid-drain is left entirely alone: no index listing, no segment
// updates. Its segments are serving in-flight queries on an index that is
// already deleted in metadata, and a segment update issued now would reopen
// the segment against the current index set - without that index. The mock
// broker proves the skip: no expectations are registered, so any call to it
// fails the test.
func (suite *IndexCheckerSuite) TestDrainingCollectionIsLeftAlone() {
	checker := suite.checker
	ctx := context.Background()

	coll := utils.CreateTestCollection(1, 1)
	coll.FieldIndexID = map[int64]int64{101: 1000}
	coll.Schema = &schemapb.CollectionSchema{
		Name: "test_drain_skip",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, DataType: schemapb.DataType_FloatVector, Name: "vec"},
		},
	}
	checker.meta.PutCollection(ctx, coll)
	checker.meta.Put(ctx, utils.CreateTestReplica(200, 1, []int64{1, 2}))
	suite.nodeMgr.Add(session.NewNodeInfo(session.ImmutableNodeInfo{
		NodeID:   1,
		Address:  "localhost",
		Hostname: "localhost",
	}))
	checker.meta.HandleNodeUp(ctx, 1)
	checker.dist.SegmentDistManager.Update(1, utils.CreateTestSegment(1, 1, 2, 1, 1, "test-insert-channel"))

	installCheckerDrainer(suite.T(), drainingDrainer{draining: map[int64]bool{1: true}})

	tasks := checker.Check(context.Background())
	suite.Len(tasks, 0, "a draining collection must produce no segment updates")
}
