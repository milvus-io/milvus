// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func genResultHandlerStage(ctx context.Context, t *testing.T) *resultHandlerStage {
	s, err := genSimpleStreaming(ctx)
	assert.NoError(t, err)
	h, err := genSimpleHistorical(ctx)
	assert.NoError(t, err)

	inputChan := make(chan queryResult, queryBufferSize)
	stream, err := genQueryMsgStream(ctx)
	assert.NoError(t, err)

	queryResChannel := genQueryResultChannel()

	stream.AsProducer([]string{queryResChannel})
	stream.Start()

	resStage := newResultHandlerStage(ctx,
		defaultCollectionID,
		s,
		h,
		inputChan,
		stream)
	return resStage
}

func TestResultHandlerStage_TestSearch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resStage := genResultHandlerStage(ctx, t)
	go resStage.start()

	resMsg, err := genSimpleSearchResult()
	assert.NoError(t, err)
	go func() {
		resStage.input <- resMsg
	}()

	resStream, err := initConsumer(ctx, resStage.queryResultStream.GetProduceChannels()[0])
	assert.NoError(t, err)
	defer resStream.Close()

	res, err := consumeSimpleSearchResult(resStream)
	assert.NoError(t, err)
	assert.Equal(t, defaultTopK, res.TopK)
	assert.Equal(t, 0, len(res.ChannelIDsSearched))
	assert.Equal(t, 1, len(res.SealedSegmentIDsSearched))
	assert.Equal(t, defaultSegmentID, res.SealedSegmentIDsSearched[0])
	assert.Equal(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
}

func TestResultHandlerStage_TestRetrieve(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resStage := genResultHandlerStage(ctx, t)
	go resStage.start()

	resMsg, err := genSimpleRetrieveResult()
	assert.NoError(t, err)
	go func() {
		resStage.input <- resMsg
	}()

	resStream, err := initConsumer(ctx, resStage.queryResultStream.GetProduceChannels()[0])
	assert.NoError(t, err)
	defer resStream.Close()

	res, err := consumeSimpleRetrieveResult(resStream)
	assert.NoError(t, err)
	assert.NotNil(t, res.Ids)
	assert.Equal(t, commonpb.ErrorCode_Success, res.Status.ErrorCode)
}

func TestResultHandlerStage_TestReduceSearchInvalid(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resStage := genResultHandlerStage(ctx, t)

	t.Run("test null search results", func(t *testing.T) {
		resMsg, err := genSimpleSearchResult()
		assert.NoError(t, err)
		resMsg.searchResults = make([]*SearchResult, 0)

		resStage.results[resMsg.ID()] = make([]queryResult, 0)
		resStage.results[resMsg.ID()] = append(resStage.results[resMsg.ID()], resMsg)

		resStage.reduceSearch(resMsg.ID())
	})

	t.Run("test search results with error", func(t *testing.T) {
		resMsg, err := genSimpleSearchResult()
		assert.NoError(t, err)
		resMsg.err = errors.New("test error")

		resStage.results[resMsg.ID()] = make([]queryResult, 0)
		resStage.results[resMsg.ID()] = append(resStage.results[resMsg.ID()], resMsg)

		resStage.reduceSearch(resMsg.ID())
	})

	t.Run("test invalid search result type", func(t *testing.T) {
		resMsg, err := genSimpleRetrieveResult()
		assert.NoError(t, err)

		resStage.results[resMsg.ID()] = make([]queryResult, 0)
		resStage.results[resMsg.ID()] = append(resStage.results[resMsg.ID()], resMsg)

		resStage.reduceSearch(resMsg.ID())
	})
}

func TestResultHandlerStage_TranslateHits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resStage := genResultHandlerStage(ctx, t)

	fieldID := FieldID(0)
	fieldIDs := []FieldID{fieldID}

	genRawHits := func(dataType schemapb.DataType) [][]byte {
		// ids
		ids := make([]int64, 0)
		for i := 0; i < defaultMsgLength; i++ {
			ids = append(ids, int64(i))
		}

		// raw data
		rawData := make([][]byte, 0)
		switch dataType {
		case schemapb.DataType_Bool:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, true)
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Int8:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, int8(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Int16:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, int16(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Int32:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, int32(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Int64:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, int64(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Float:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, float32(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		case schemapb.DataType_Double:
			var buf bytes.Buffer
			for i := 0; i < defaultMsgLength; i++ {
				err := binary.Write(&buf, binary.LittleEndian, float64(i))
				assert.NoError(t, err)
			}
			rawData = append(rawData, buf.Bytes())
		}
		hit := &milvuspb.Hits{
			IDs:     ids,
			RowData: rawData,
		}
		hits := []*milvuspb.Hits{hit}
		rawHits := make([][]byte, 0)
		for _, h := range hits {
			rawHit, err := proto.Marshal(h)
			assert.NoError(t, err)
			rawHits = append(rawHits, rawHit)
		}
		return rawHits
	}

	genSchema := func(dataType schemapb.DataType) *typeutil.SchemaHelper {
		schema := &schemapb.CollectionSchema{
			Name:   defaultCollectionName,
			AutoID: true,
			Fields: []*schemapb.FieldSchema{
				genConstantField(constFieldParam{
					id:       fieldID,
					dataType: dataType,
				}),
			},
		}
		schemaHelper, err := typeutil.CreateSchemaHelper(schema)
		assert.NoError(t, err)
		return schemaHelper
	}

	t.Run("test bool field", func(t *testing.T) {
		dataType := schemapb.DataType_Bool
		_, err := resStage.translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test int8 field", func(t *testing.T) {
		dataType := schemapb.DataType_Int8
		_, err := resStage.translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test int16 field", func(t *testing.T) {
		dataType := schemapb.DataType_Int16
		_, err := resStage.translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test int32 field", func(t *testing.T) {
		dataType := schemapb.DataType_Int32
		_, err := resStage.translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test int64 field", func(t *testing.T) {
		dataType := schemapb.DataType_Int64
		_, err := resStage.translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test float field", func(t *testing.T) {
		dataType := schemapb.DataType_Float
		_, err := resStage.translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test double field", func(t *testing.T) {
		dataType := schemapb.DataType_Double
		_, err := resStage.translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.NoError(t, err)
	})

	t.Run("test field with error type", func(t *testing.T) {
		dataType := schemapb.DataType_FloatVector
		_, err := resStage.translateHits(genSchema(dataType), fieldIDs, genRawHits(dataType))
		assert.Error(t, err)
	})
}
