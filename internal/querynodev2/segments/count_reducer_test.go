package segments

/*import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/segcorepb"
)

type InternalCntReducerSuite struct {
	suite.Suite
	r *cntReducer
}

func (suite *InternalCntReducerSuite) SetupTest() {
	suite.r = &cntReducer{}
}

func (suite *InternalCntReducerSuite) TearDownTest() {}

func TestInternalCntReducerSuite(t *testing.T) {
	suite.Run(t, new(InternalCntReducerSuite))
}

func (suite *InternalCntReducerSuite) TestInvalid() {
	results := []*internalpb.RetrieveResults{
		{
			FieldsData: []*schemapb.FieldData{nil, nil},
		},
	}

	_, err := suite.r.Reduce(context.TODO(), results)
	suite.Error(err)
}

func (suite *InternalCntReducerSuite) TestNormalCase() {
	results := []*internalpb.RetrieveResults{
		funcutil.WrapCntToInternalResult(1),
		funcutil.WrapCntToInternalResult(2),
		funcutil.WrapCntToInternalResult(3),
		funcutil.WrapCntToInternalResult(4),
	}

	res, err := suite.r.Reduce(context.TODO(), results)
	suite.NoError(err)

	total, err := funcutil.CntOfInternalResult(res)
	suite.NoError(err)
	suite.Equal(int64(1+2+3+4), total)
}

type SegCoreCntReducerSuite struct {
	suite.Suite
	r *cntReducerSegCore
}

func (suite *SegCoreCntReducerSuite) SetupTest() {
	suite.r = &cntReducerSegCore{}
}

func (suite *SegCoreCntReducerSuite) TearDownTest() {}

func TestSegCoreCntReducerSuite(t *testing.T) {
	suite.Run(t, new(SegCoreCntReducerSuite))
}

func (suite *SegCoreCntReducerSuite) TestInvalid() {
	results := []*segcorepb.RetrieveResults{
		{
			FieldsData: []*schemapb.FieldData{nil, nil},
		},
	}

	_, err := suite.r.Reduce(context.TODO(), results, nil, nil)
	suite.Error(err)
}

func (suite *SegCoreCntReducerSuite) TestNormalCase() {
	results := []*segcorepb.RetrieveResults{
		funcutil.WrapCntToSegCoreResult(1),
		funcutil.WrapCntToSegCoreResult(2),
		funcutil.WrapCntToSegCoreResult(3),
		funcutil.WrapCntToSegCoreResult(4),
	}

	res, err := suite.r.Reduce(context.TODO(), results, nil, nil)
	suite.NoError(err)

	total, err := funcutil.CntOfSegCoreResult(res)
	suite.NoError(err)
	suite.Equal(int64(1+2+3+4), total)
}*/
