package milvusclient

import (
	"context"
	"math/rand"
	"net"
	"strings"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

const (
	bufSize = 1024 * 1024
)

type MockSuiteBase struct {
	suite.Suite

	lis *bufconn.Listener
	svr *grpc.Server

	client        *Client
	connectMocker *mockey.Mocker
}

func (s *MockSuiteBase) SetupSuite() {
	s.lis = bufconn.Listen(bufSize)
	s.svr = grpc.NewServer()

	milvuspb.RegisterMilvusServiceServer(s.svr, &milvuspb.UnimplementedMilvusServiceServer{})

	go func() {
		s.T().Log("start mock server")
		if err := s.svr.Serve(s.lis); err != nil {
			s.Fail("failed to start mock server", err.Error())
		}
	}()

	// Setup Connect mock for client initialization
	s.connectMocker = mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).Connect).Return(&milvuspb.ConnectResponse{
		Status:     &commonpb.Status{},
		Identifier: 1,
	}, nil).Build()
}

func (s *MockSuiteBase) TearDownSuite() {
	if s.connectMocker != nil {
		s.connectMocker.UnPatch()
		s.connectMocker = nil
	}
	s.svr.Stop()
	s.lis.Close()
}

func (s *MockSuiteBase) mockDialer(context.Context, string) (net.Conn, error) {
	return s.lis.Dial()
}

func (s *MockSuiteBase) SetupTest() {
	c, err := New(context.Background(), &ClientConfig{
		Address: "bufnet",
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(s.mockDialer),
		},
	})
	s.Require().NoError(err)

	s.client = c
}

func (s *MockSuiteBase) TearDownTest() {
	if s.client != nil {
		s.client.Close(context.Background())
		s.client = nil
	}
	// Reapply Connect mock after UnPatchAll() in subtests
	s.reapplyConnectMock()
}

// reapplyConnectMock re-establishes the Connect mock after mockey.UnPatchAll() is called in subtests.
// This ensures the Connect mock is available for the next test's SetupTest().
func (s *MockSuiteBase) reapplyConnectMock() {
	if s.connectMocker != nil {
		s.connectMocker.UnPatch()
	}
	s.connectMocker = mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).Connect).Return(&milvuspb.ConnectResponse{
		Status:     &commonpb.Status{},
		Identifier: 1,
	}, nil).Build()
}

func (s *MockSuiteBase) setupCache(collName string, schema *entity.Schema) {
	s.client.collCache.collections.Insert(collName, &entity.Collection{
		Name:   collName,
		Schema: schema,
	})
}

func (s *MockSuiteBase) getInt64FieldData(name string, data []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: data,
					},
				},
			},
		},
	}
}

func (s *MockSuiteBase) getVarcharFieldData(name string, data []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: data,
					},
				},
			},
		},
	}
}

func (s *MockSuiteBase) getJSONBytesFieldData(name string, data [][]byte, isDynamic bool) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_JSON,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{
					JsonData: &schemapb.JSONArray{
						Data: data,
					},
				},
			},
		},
		IsDynamic: isDynamic,
	}
}

func (s *MockSuiteBase) getGeometryWktFieldData(name string, data []string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Geometry,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_GeometryWktData{
					GeometryWktData: &schemapb.GeometryWktArray{
						Data: data,
					},
				},
			},
		},
	}
}

func (s *MockSuiteBase) getFloatVectorFieldData(name string, dim int64, data []float32) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: name,
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: data,
					},
				},
			},
		},
	}
}

func (s *MockSuiteBase) getSuccessStatus() *commonpb.Status {
	return s.getStatus(commonpb.ErrorCode_Success, "")
}

func (s *MockSuiteBase) getStatus(code commonpb.ErrorCode, reason string) *commonpb.Status {
	return &commonpb.Status{
		ErrorCode: code,
		Reason:    reason,
	}
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func (s *MockSuiteBase) randString(l int) string {
	builder := strings.Builder{}
	for i := 0; i < l; i++ {
		builder.WriteRune(letters[rand.Intn(len(letters))])
	}
	return builder.String()
}
