package segments

/*import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
)

type ReducerFactorySuite struct {
	suite.Suite
	ir internalReducer
	sr segCoreReducer
	ok bool
}

func (suite *ReducerFactorySuite) SetupTest() {}

func (suite *ReducerFactorySuite) TearDownTest() {}

func TestReducerFactorySuite(t *testing.T) {
	suite.Run(t, new(ReducerFactorySuite))
}

func (suite *ReducerFactorySuite) TestCreateInternalReducer() {
	req := &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			IsCount: false,
		},
	}

	suite.ir = CreateInternalReducer(req, nil)
	_, suite.ok = suite.ir.(*defaultLimitReducer)
	suite.True(suite.ok)

	req.Req.IsCount = true

	suite.ir = CreateInternalReducer(req, nil)
	_, suite.ok = suite.ir.(*cntReducer)
	suite.True(suite.ok)
}

func (suite *ReducerFactorySuite) TestCreateSegCoreReducer() {
	req := &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			IsCount: false,
		},
	}

	suite.sr = CreateSegCoreReducer(req, nil, nil)
	_, suite.ok = suite.sr.(*defaultLimitReducerSegcore)
	suite.True(suite.ok)

	req.Req.IsCount = true
	suite.sr = CreateSegCoreReducer(req, nil, nil)
	_, suite.ok = suite.sr.(*cntReducerSegCore)
	suite.True(suite.ok)
}*/
