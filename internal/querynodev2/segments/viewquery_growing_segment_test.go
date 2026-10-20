//go:build test && dynamic

package segments

import (
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestViewQueryGrowingSegmentBatchPkExistIsConservative(t *testing.T) {
	segment := NewGrowingSegmentForViewQuery(ViewQueryGrowingSegmentInfo{CollectionID: 10}, nil)

	hits := segment.BatchPkExist(storage.NewBatchLocationsCache([]storage.PrimaryKey{
		storage.NewInt64PrimaryKey(1),
		storage.NewInt64PrimaryKey(2),
	}))

	assert.Equal(t, []bool{true, true}, hits)
}

func (suite *RetrieveSuite) TestViewQueryGrowingArrowMatchesProto() {
	params := paramtable.Get()
	key := params.CommonCfg.InterfaceZeroCopyEnabled.Key
	previous := params.CommonCfg.InterfaceZeroCopyEnabled.GetValue()
	defer params.Save(key, previous)

	outputs := []int64{
		mustFindFieldByType(suite.T(), suite.schema, schemapb.DataType_Int64).GetFieldID(),
		mustFindFieldByType(suite.T(), suite.schema, schemapb.DataType_Float).GetFieldID(),
		mustFindFieldByType(suite.T(), suite.schema, schemapb.DataType_FloatVector).GetFieldID(),
		common.TimeStampField,
	}
	expr, err := buildArrowTestRetrievePlanExpr(suite.schema, outputs)
	suite.Require().NoError(err)
	planNode := &planpb.PlanNode{}
	suite.Require().NoError(proto.Unmarshal(expr, planNode))
	plan, err := buildArrowTestRetrievePlan(suite.collection.GetCCollection(), outputs)
	suite.Require().NoError(err)
	defer plan.Delete()
	selected := []Segment{NewGrowingSegmentForViewQuery(ViewQueryGrowingSegmentInfo{
		CollectionID: suite.collectionID,
		PartitionID:  suite.partitionID,
	}, suite.growing.(*LocalSegment).csegment)}
	req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{
		CollectionID: suite.collectionID, OutputFieldsId: outputs,
		SerializedExprPlan: expr, Limit: typeutil.Unlimited,
	}, Scope: querypb.DataScope_Streaming}

	retrieve := func(useArrow bool) *segcorepb.RetrieveResults {
		suite.Require().NoError(params.Save(key, strconv.FormatBool(useArrow)))
		results, err := RetrieveSelectedSegments(suite.ctx, plan, req, selected)
		suite.Require().NoError(err)
		defer ReleaseRecords(results)
		suite.Require().Len(results, 1)
		var records []arrow.Record
		if useArrow {
			suite.Require().NotNil(results[0].Record)
			suite.Positive(results[0].Record.NumCols())
			records = []arrow.Record{results[0].Record}
		} else {
			suite.Nil(results[0].Record)
		}
		result, selection, err := RunQNQueryPipeline(suite.ctx, req, suite.schema, planNode,
			[]*segcorepb.RetrieveResults{results[0].Result}, records, selected, suite.manager, plan)
		suite.Require().NoError(err)
		suite.Require().NoError(queryutil.MaterializeArrowSelection(result, selection, suite.schema))
		return result
	}

	expected := retrieve(false)
	actual := retrieve(true)
	suite.Equal([]int64{1, 2, 3}, actual.GetIds().GetIntId().GetData())
	suite.True(proto.Equal(expected, actual), "Arrow and protobuf responses must match after records are released")
}
