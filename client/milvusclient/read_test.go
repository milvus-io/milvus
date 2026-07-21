package milvusclient

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
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
					Scores:  make([]float32, 10),
					Topks:   []int64{10},
					Recalls: []float32{0.9},
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

	s.Run("bad_result", func() {
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
					Topks:  []int64{},
				},
			}, nil
		}).Once()

		ap := index.NewCustomAnnParam()
		ap.WithExtraParam("custom_level", 1)
		rss, err := s.client.Search(ctx, NewSearchOption(collectionName, 10, []entity.Vector{
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
		s.Len(rss, 1)
		rs := rss[0]
		s.Error(rs.Err)
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

func (s *ReadSuite) TestParseEmptyNullableStructArrayFromWire() {
	schema := entity.NewSchema().WithField(entity.NewField().
		WithName("clips").
		WithDataType(entity.FieldTypeArray).
		WithElementType(entity.FieldTypeStruct).
		WithNullable(true))
	source := column.NewColumnStructArray("clips", []column.Column{
		column.NewColumnInt64Array("label", nil),
		column.NewColumnFloatVectorArray("embedding", 2, nil),
	})
	source.SetNullable(true)

	wire, err := proto.Marshal(source.FieldData())
	s.Require().NoError(err)
	decoded := &schemapb.FieldData{}
	s.Require().NoError(proto.Unmarshal(wire, decoded))
	for _, subField := range decoded.GetStructArrays().GetFields() {
		s.Nil(subField.ValidData)
	}

	columns, err := s.client.parseSearchResult(schema, []string{"clips"}, []*schemapb.FieldData{decoded}, 0, 0, 0)
	s.Require().NoError(err)
	s.Require().Len(columns, 1)
	s.True(columns[0].Nullable())
	s.Zero(columns[0].Len())
	s.Require().NoError(columns[0].ValidateNullable())
}

func (s *ReadSuite) TestParseEmptyNullableStructArrayWithoutFieldData() {
	schema := entity.NewSchema().WithField(entity.NewField().
		WithName("clips").
		WithDataType(entity.FieldTypeArray).
		WithElementType(entity.FieldTypeStruct).
		WithNullable(true).
		WithStructSchema(entity.NewStructSchema().
			WithField(entity.NewField().WithName("label").WithDataType(entity.FieldTypeInt64)).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(2))))

	columns, err := s.client.parseSearchResult(schema, []string{"clips"}, nil, 0, 0, 0)
	s.Require().NoError(err)
	s.Require().Len(columns, 1)
	s.True(columns[0].Nullable())
	s.Zero(columns[0].Len())
	s.Require().NoError(columns[0].ValidateNullable())
}

// TestSearch_TextMatch tests the text match search functionality.
// It tests the minimum_should_match parameter in the expression.
func (s *ReadSuite) TestSearch_TextMatch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("min_should_match_in_expr", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		s.setupCache(collectionName, s.schema)

		s.mock.EXPECT().Search(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, sr *milvuspb.SearchRequest) (*milvuspb.SearchResults, error) {
			// ensure the expression contains minimum_should_match and both fields
			s.Contains(sr.GetDsl(), "minimum_should_match=2")
			s.Contains(sr.GetDsl(), "text_match(")
			return &milvuspb.SearchResults{
				Status: merr.Success(),
				Results: &schemapb.SearchResultData{
					NumQueries: 1,
					TopK:       1,
					FieldsData: []*schemapb.FieldData{
						s.getInt64FieldData("ID", []int64{1}),
					},
					Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
					Scores: []float32{0.1},
					Topks:  []int64{1},
				},
			}, nil
		}).Once()

		q := "artificial intelligence"
		expr := "text_match(title, \"" + q + "\", minimum_should_match=2) OR text_match(document_text, \"" + q + "\", minimum_should_match=2)"
		vectors := []entity.Vector{entity.Text(q)}
		_, err := s.client.Search(ctx, NewSearchOption(collectionName, 5, vectors).
			WithANNSField("text_sparse_vector").
			WithFilter(expr).
			WithOutputFields("ID"))
		s.NoError(err)
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

	s.Run("empty nullable struct array", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		structSchema := entity.NewStructSchema().
			WithField(entity.NewField().WithName("label").WithDataType(entity.FieldTypeInt64)).
			WithField(entity.NewField().WithName("embedding").WithDataType(entity.FieldTypeFloatVector).WithDim(2))
		schema := entity.NewSchema().
			WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
			WithField(entity.NewField().
				WithName("clips").
				WithDataType(entity.FieldTypeArray).
				WithElementType(entity.FieldTypeStruct).
				WithNullable(true).
				WithStructSchema(structSchema))
		s.setupCache(collectionName, schema)

		s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
			s.Equal([]string{"clips"}, qr.GetOutputFields())
			return &milvuspb.QueryResults{}, nil
		}).Once()

		rs, err := s.client.Query(ctx, NewQueryOption(collectionName).
			WithFilter("ID < 0").
			WithOutputFields("clips"))
		s.Require().NoError(err)
		clips := rs.GetColumn("clips")
		s.Require().NotNil(clips)
		s.True(clips.Nullable())
		s.Zero(clips.Len())
		s.Require().NoError(clips.ValidateNullable())
	})

	s.Run("empty dynamic field", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		schema := entity.NewSchema().WithDynamicFieldEnabled(true).
			WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true))
		s.setupCache(collectionName, schema)

		s.mock.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, qr *milvuspb.QueryRequest) (*milvuspb.QueryResults, error) {
			s.Equal([]string{"dynamic_key"}, qr.GetOutputFields())
			return &milvuspb.QueryResults{}, nil
		}).Once()

		rs, err := s.client.Query(ctx, NewQueryOption(collectionName).
			WithFilter("ID < 0").
			WithOutputFields("dynamic_key"))
		s.Require().NoError(err)
		dynamic := rs.GetColumn("dynamic_key")
		s.Require().NotNil(dynamic)
		s.Zero(dynamic.Len())
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
