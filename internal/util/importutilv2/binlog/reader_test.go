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

package binlog

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/stretchr/testify/mock"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ReaderSuite struct {
	suite.Suite

	// test param
	schema  *schemapb.CollectionSchema
	numRows int

	deleteData []any
	tsField    []uint64
	tsStart    uint64
	tsEnd      uint64
}

func (suite *ReaderSuite) SetupSuite() {

}

func (suite *ReaderSuite) SetupTest() {
	cm := mocks.NewChunkManager(suite.T())
	cm.EXPECT().Read(mock.Anything, path).Return(nil, nil)
	reader, err := NewReader(cm)
}

func (suite *ReaderSuite) run(dt schemapb.DataType) {
	const (
		insertPrefix = "mock-insert-binlog-prefix"
		deltaPrefix  = "mock-delta-binlog-prefix"

		tsStart = 2000
		tsEnd   = 5000
	)
	var (
		insertBinlogs = []string{
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/0/435978159903735801",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/0/435978159903735802",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/0/435978159903735803",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/1/435978159903735811",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/1/435978159903735812",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/1/435978159903735813",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/100/435978159903735821",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/100/435978159903735822",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/100/435978159903735823",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/101/435978159903735831",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/101/435978159903735832",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/101/435978159903735833",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/102/435978159903735841",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/102/435978159903735842",
			"backup/bak1/data/insert_log/435978159196147009/435978159196147010/435978159261483008/102/435978159903735843",
		}
		deltaLogs = []string{
			"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415105",
			"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415106",
			"backup/bak1/data/delta_log/435978159196147009/435978159196147010/435978159261483009/434574382554415107",
		}
	)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
			},
			{
				FieldID:  102,
				Name:     dt.String(),
				DataType: dt,
			},
		},
	}
	cm := mocks.NewChunkManager(suite.T())
	reader, err := NewReader(cm, schema, []string{insertPrefix, deltaPrefix}, tsStart, tsEnd)
	suite.NoError(err)

	cm.EXPECT().ListWithPrefix(mock.Anything, insertPrefix, mock.Anything).Return(insertBinlogs, nil, nil)
	cm.EXPECT().ListWithPrefix(mock.Anything, deltaPrefix, mock.Anything).Return(deltaLogs, nil, nil)
	for _, path := range insertBinlogs {
		cm.EXPECT().Read(mock.Anything, path).Return(nil, nil)
	}
}

func (suite *ReaderSuite) newField(dt schemapb.DataType, fieldID int64, isPK bool) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:      fieldID,
		Name:         dt.String(),
		IsPrimaryKey: isPK,
		DataType:     dt,
	}
}

func (suite *ReaderSuite) TestReadBool() {

}

func TestUtil(t *testing.T) {
	suite.Run(t, new(ReaderSuite))
}
