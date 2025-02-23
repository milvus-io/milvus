// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pipeline

import (
	"math/rand"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
)

const defaultDim = 128

func buildDeleteMsg(collectionID int64, partitionID int64, channel string, rowSum int) *msgstream.DeleteMsg {
	deleteMsg := emptyDeleteMsg(collectionID, partitionID, channel)
	for i := 1; i <= rowSum; i++ {
		deleteMsg.Timestamps = append(deleteMsg.Timestamps, 0)
		deleteMsg.HashValues = append(deleteMsg.HashValues, 0)
		deleteMsg.NumRows++
	}
	deleteMsg.PrimaryKeys = genDefaultDeletePK(rowSum)
	return deleteMsg
}

func buildInsertMsg(collectionID int64, partitionID int64, segmentID int64, channel string, rowSum int) *msgstream.InsertMsg {
	insertMsg := emptyInsertMsg(collectionID, partitionID, segmentID, channel)
	for i := 1; i <= rowSum; i++ {
		insertMsg.HashValues = append(insertMsg.HashValues, 0)
		insertMsg.Timestamps = append(insertMsg.Timestamps, 0)
		insertMsg.RowIDs = append(insertMsg.RowIDs, rand.Int63n(100))
		insertMsg.NumRows++
	}
	insertMsg.FieldsData = genDefaultFiledData(rowSum)
	return insertMsg
}

func emptyDeleteMsg(collectionID int64, partitionID int64, channel string) *msgstream.DeleteMsg {
	deleteReq := &msgpb.DeleteRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Delete),
			commonpbutil.WithTimeStamp(0),
		),
		CollectionID: collectionID,
		PartitionID:  partitionID,
		ShardName:    channel,
	}

	return &msgstream.DeleteMsg{
		BaseMsg:       msgstream.BaseMsg{},
		DeleteRequest: deleteReq,
	}
}

func emptyInsertMsg(collectionID int64, partitionID int64, segmentID int64, channel string) *msgstream.InsertMsg {
	insertReq := &msgpb.InsertRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Insert),
			commonpbutil.WithTimeStamp(0),
		),
		CollectionID: collectionID,
		PartitionID:  partitionID,
		SegmentID:    segmentID,
		ShardName:    channel,
		Version:      msgpb.InsertDataVersion_ColumnBased,
	}
	insertMsg := &msgstream.InsertMsg{
		BaseMsg:       msgstream.BaseMsg{},
		InsertRequest: insertReq,
	}

	return insertMsg
}

// gen IDs with random pks for DeleteMsg
func genDefaultDeletePK(rowSum int) *schemapb.IDs {
	pkDatas := []int64{}

	for i := 1; i <= rowSum; i++ {
		pkDatas = append(pkDatas, int64(i))
	}

	return &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: pkDatas,
			},
		},
	}
}

// gen IDs with specified pk
func genDeletePK(pks ...int64) *schemapb.IDs {
	pkDatas := make([]int64, 0, len(pks))
	pkDatas = append(pkDatas, pks...)

	return &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: pkDatas,
			},
		},
	}
}

func genDefaultFiledData(numRows int) []*schemapb.FieldData {
	pkDatas := []int64{}
	vectorDatas := []byte{}

	for i := 1; i <= numRows; i++ {
		pkDatas = append(pkDatas, int64(i))
		vectorDatas = append(vectorDatas, uint8(i))
	}

	return []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_Int64,
			FieldName: "pk",
			FieldId:   100,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: pkDatas,
						},
					},
				},
			},
		},
		{
			Type:      schemapb.DataType_BinaryVector,
			FieldName: "vector",
			FieldId:   101,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{
					Dim: 8,
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: vectorDatas,
					},
				},
			},
		},
	}
}

func genFiledDataWithSchema(schema *schemapb.CollectionSchema, numRows int) []*schemapb.FieldData {
	fieldsData := make([]*schemapb.FieldData, 0)
	for _, field := range schema.Fields {
		if field.DataType < 100 {
			fieldsData = append(fieldsData, testutils.GenerateScalarFieldDataWithID(field.DataType, field.DataType.String(), field.GetFieldID(), numRows))
		} else {
			fieldsData = append(fieldsData, testutils.GenerateVectorFieldDataWithID(field.DataType, field.DataType.String(), field.GetFieldID(), numRows, defaultDim))
		}
	}
	return fieldsData
}
