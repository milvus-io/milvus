package milvusclient

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type ReadSuite struct {
	MockSuiteBase

	schema    *entity.Schema
	schemaDyn *entity.Schema
}

func (s *ReadSuite) SetupSuite() {
	s.MockSuiteBase.SetupSuite()
	s.schema = entity.NewSchema().
		WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("Vector").WithDataType(entity.FieldTypeFloatVector).WithDim(128))

	s.schemaDyn = entity.NewSchema().WithDynamicFieldEnabled(true).
		WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(128))
}

func (s *ReadSuite) TestSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))
		s.setupCache(collectionName, s.schema)
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			s.Equal(collectionName, sr.GetCollectionName())
			s.ElementsMatch([]string{partitionName}, sr.GetPartitionNames())

			return &milvuspb.SearchResults{
				Status: merr.Success(),
				Results: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       10,
					FieldsData: []*schemapb.FieldData{
						s.getInt64FieldData("ID", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
					},
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
							},
						},
					},
					Scores: make([]float32, 10),
					Topks:  []int64{10},
				},
			}, nil
		}).Once()

		ap := index.NewCustomAnnParam()
		ap.WithExtraParam("custom_level", 1)
		_, err := s.client.Search(ctx, NewSearchOption(collectionName, 10, []entity.Vector{
			entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
				return rand.Float32()
			})),
		}).WithPartitions(partitionName).
			WithFilter("id > {tmpl_id}").
			WithTemplateParam("tmpl_id", 100).
			WithGroupByField("group_by").
			WithSearchParam("ignore_growing", "true").
			WithAnnParam(ap),
		)
		s.NoError(err)
	})

	s.Run("dynamic_schema", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))
		s.setupCache(collectionName, s.schemaDyn)
		s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			return &milvuspb.SearchResults{
				Status: merr.Success(),
				Results: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       2,
					FieldsData: []*schemapb.FieldData{
						s.getInt64FieldData("ID", []int64{1, 2}),
						s.getJSONBytesFieldData("$meta", [][]byte{
							[]byte(`{"A": 123, "B": "456"}`),
							[]byte(`{"B": "abc", "A": 456}`),
						}, true),
					},
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{1, 2},
							},
						},
					},
					Scores: make([]float32, 2),
					Topks:  []int64{2},
				},
			}, nil
		}).Once()

		_, err := s.client.Search(ctx, NewSearchOption(collectionName, 10, []entity.Vector{
			entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
				return rand.Float32()
			})),
		}).WithPartitions(partitionName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collectionName, s.schemaDyn)

		_, err := s.client.Search(ctx, NewSearchOption(collectionName, 10, []entity.Vector{nonSupportData{}}))
		s.Error(err)

		s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			return nil, merr.WrapErrServiceInternal("mocked")
		}).Once()

		_, err = s.client.Search(ctx, NewSearchOption(collectionName, 10, []entity.Vector{
			entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
				return rand.Float32()
			})),
		}))
		s.Error(err)
	})
}

func (s *ReadSuite) TestHybridSearch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))
		s.setupCache(collectionName, s.schema)

		s.mock.EXPECT().HybridSearch(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, hsr *milvuspb.HybridSearchRequest) (*milvuspb.SearchResults, error) {
			s.Equal(collectionName, hsr.GetCollectionName())
			s.ElementsMatch([]string{partitionName}, hsr.GetPartitionNames())
			s.ElementsMatch([]string{"*"}, hsr.GetOutputFields())
			return &milvuspb.SearchResults{
				Status: merr.Success(),
				Results: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       2,
					FieldsData: []*schemapb.FieldData{
						s.getInt64FieldData("ID", []int64{1, 2}),
						s.getJSONBytesFieldData("$meta", [][]byte{
							[]byte(`{"A": 123, "B": "456"}`),
							[]byte(`{"B": "abc", "A": 456}`),
						}, true),
					},
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{1, 2},
							},
						},
					},
					Scores: make([]float32, 2),
					Topks:  []int64{2},
				},
			}, nil
		}).Once()

		_, err := s.client.HybridSearch(ctx, NewHybridSearchOption(collectionName, 5, NewAnnRequest("vector", 10, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
			return rand.Float32()
		}))).WithFilter("ID > 100"), NewAnnRequest("vector", 10, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
			return rand.Float32()
		})))).WithConsistencyLevel(entity.ClStrong).WithPartitons(partitionName).WithReranker(NewRRFReranker()).WithOutputFields("*"))
		s.NoError(err)
	})

	s.Run("failure", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collectionName, s.schemaDyn)

		_, err := s.client.HybridSearch(ctx, NewHybridSearchOption(collectionName, 5, NewAnnRequest("vector", 10, nonSupportData{})))
		s.Error(err)

		s.mock.EXPECT().HybridSearch(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, hsr *milvuspb.HybridSearchRequest) (*milvuspb.SearchResults, error) {
			return nil, merr.WrapErrServiceInternal("mocked")
		}).Once()

		_, err = s.client.HybridSearch(ctx, NewHybridSearchOption(collectionName, 5, NewAnnRequest("vector", 10, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
			return rand.Float32()
		}))).WithFilter("ID > 100"), NewAnnRequest("vector", 10, entity.FloatVector(lo.RepeatBy(128, func(_ int) float32 {
			return rand.Float32()
		})))))
		s.Error(err)
	})
}

func (s *ReadSuite) TestQuery() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))
		s.setupCache(collectionName, s.schema)

		s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
			s.Equal(collectionName, qr.GetCollectionName())

			return &milvuspb.QueryResults{}, nil
		}).Once()

		rs, err := s.client.Query(ctx, NewQueryOption(collectionName).WithPartitions(partitionName))
		s.NoError(err)
		s.NotNil(rs.sch)
	})

	s.Run("bad_request", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collectionName, s.schema)

		_, err := s.client.Query(ctx, NewQueryOption(collectionName).WithFilter("id > {tmpl_id}").WithTemplateParam("tmpl_id", struct{}{}))
		s.Error(err)
	})
}

func TestRead(t *testing.T) {
	suite.Run(t, new(ReadSuite))
}
