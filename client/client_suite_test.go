package client

import (
	"context"
	"math/rand"
	"net"
	"strings"

	mock "github.com/stretchr/testify/mock"
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

	lis  *bufconn.Listener
	svr  *grpc.Server
	mock *MilvusServiceServer

	client *Client
}

func (s *MockSuiteBase) SetupSuite() {
	s.lis = bufconn.Listen(bufSize)
	s.svr = grpc.NewServer()

	s.mock = &MilvusServiceServer{}

	milvuspb.RegisterMilvusServiceServer(s.svr, s.mock)

	go func() {
		s.T().Log("start mock server")
		if err := s.svr.Serve(s.lis); err != nil {
			s.Fail("failed to start mock server", err.Error())
		}
	}()
	s.setupConnect()
}

func (s *MockSuiteBase) TearDownSuite() {
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
	s.setupConnect()

	s.client = c
}

func (s *MockSuiteBase) TearDownTest() {
	s.client.Close(context.Background())
	s.client = nil
}

func (s *MockSuiteBase) resetMock() {
	// MetaCache.reset()
	if s.mock != nil {
		s.mock.Calls = nil
		s.mock.ExpectedCalls = nil
		s.setupConnect()
	}
}

func (s *MockSuiteBase) setupConnect() {
	s.mock.EXPECT().Connect(mock.Anything, mock.AnythingOfType("*milvuspb.ConnectRequest")).
		Return(&milvuspb.ConnectResponse{
			Status:     &commonpb.Status{},
			Identifier: 1,
		}, nil).Maybe()
}

func (s *MockSuiteBase) setupCache(collName string, schema *entity.Schema) {
	s.client.collCache.collections.Insert(collName, &entity.Collection{
		Name:   collName,
		Schema: schema,
	})
}

func (s *MockSuiteBase) setupHasCollection(collNames ...string) {
	s.mock.EXPECT().HasCollection(mock.Anything, mock.AnythingOfType("*milvuspb.HasCollectionRequest")).
		Call.Return(func(ctx context.Context, req *milvuspb.HasCollectionRequest) *milvuspb.BoolResponse {
		resp := &milvuspb.BoolResponse{Status: &commonpb.Status{}}
		for _, collName := range collNames {
			if req.GetCollectionName() == collName {
				resp.Value = true
				break
			}
		}
		return resp
	}, nil)
}

func (s *MockSuiteBase) setupHasCollectionError(errorCode commonpb.ErrorCode, err error) {
	s.mock.EXPECT().HasCollection(mock.Anything, mock.AnythingOfType("*milvuspb.HasCollectionRequest")).
		Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{ErrorCode: errorCode},
		}, err)
}

func (s *MockSuiteBase) setupHasPartition(collName string, partNames ...string) {
	s.mock.EXPECT().HasPartition(mock.Anything, mock.AnythingOfType("*milvuspb.HasPartitionRequest")).
		Call.Return(func(ctx context.Context, req *milvuspb.HasPartitionRequest) *milvuspb.BoolResponse {
		resp := &milvuspb.BoolResponse{Status: &commonpb.Status{}}
		if req.GetCollectionName() == collName {
			for _, partName := range partNames {
				if req.GetPartitionName() == partName {
					resp.Value = true
					break
				}
			}
		}
		return resp
	}, nil)
}

func (s *MockSuiteBase) setupHasPartitionError(errorCode commonpb.ErrorCode, err error) {
	s.mock.EXPECT().HasPartition(mock.Anything, mock.AnythingOfType("*milvuspb.HasPartitionRequest")).
		Return(&milvuspb.BoolResponse{
			Status: &commonpb.Status{ErrorCode: errorCode},
		}, err)
}

func (s *MockSuiteBase) setupDescribeCollection(_ string, schema *entity.Schema) {
	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.AnythingOfType("*milvuspb.DescribeCollectionRequest")).
		Call.Return(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) *milvuspb.DescribeCollectionResponse {
		return &milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Schema: schema.ProtoMessage(),
		}
	}, nil)
}

func (s *MockSuiteBase) setupDescribeCollectionError(errorCode commonpb.ErrorCode, err error) {
	s.mock.EXPECT().DescribeCollection(mock.Anything, mock.AnythingOfType("*milvuspb.DescribeCollectionRequest")).
		Return(&milvuspb.DescribeCollectionResponse{
			Status: &commonpb.Status{ErrorCode: errorCode},
		}, err)
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
