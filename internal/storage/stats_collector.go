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

package storage

import (
	"strconv"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// StatsCollector collects statistics from records
type StatsCollector interface {
	// Collect collects statistics from a record
	Collect(r Record) error
	// Digest serializes the collected statistics, writes them to storage,
	// and returns the field binlog metadata
	Digest(
		collectionID, partitionID, segmentID UniqueID,
		rootPath string,
		rowNum int64,
		allocator allocator.Interface,
		blobsWriter ChunkedBlobsWriter,
	) (map[FieldID]*datapb.FieldBinlog, error)
}

// PkStatsCollector collects primary key statistics
type PkStatsCollector struct {
	pkstats      *PrimaryKeyStats
	collectionID UniqueID // needed for initializing codecs, TODO: remove this
	schema       *schemapb.CollectionSchema
}

// Collect collects primary key stats from the record
func (c *PkStatsCollector) Collect(r Record) error {
	if c.pkstats == nil {
		return nil
	}

	rows := r.Len()
	for i := 0; i < rows; i++ {
		switch schemapb.DataType(c.pkstats.PkType) {
		case schemapb.DataType_Int64:
			pkArray := r.Column(c.pkstats.FieldID).(*array.Int64)
			pk := &Int64PrimaryKey{
				Value: pkArray.Value(i),
			}
			c.pkstats.Update(pk)
		case schemapb.DataType_VarChar:
			pkArray := r.Column(c.pkstats.FieldID).(*array.String)
			pk := &VarCharPrimaryKey{
				Value: pkArray.Value(i),
			}
			c.pkstats.Update(pk)
		default:
			panic("invalid data type")
		}
	}
	return nil
}

// Digest serializes the collected primary key statistics, writes them to storage,
// and returns the field binlog metadata
func (c *PkStatsCollector) Digest(
	collectionID, partitionID, segmentID UniqueID,
	rootPath string,
	rowNum int64,
	allocator allocator.Interface,
	blobsWriter ChunkedBlobsWriter,
) (map[FieldID]*datapb.FieldBinlog, error) {
	if c.pkstats == nil {
		return nil, nil
	}

	// Serialize PK stats
	codec := NewInsertCodecWithSchema(&etcdpb.CollectionMeta{
		ID:     c.collectionID,
		Schema: c.schema,
	})
	sblob, err := codec.SerializePkStats(c.pkstats, rowNum)
	if err != nil {
		return nil, err
	}

	// Get pk field ID
	pkField, err := typeutil.GetPrimaryFieldSchema(c.schema)
	if err != nil {
		return nil, err
	}

	// Allocate ID for stats blob
	id, err := allocator.AllocOne()
	if err != nil {
		return nil, err
	}

	// Assign proper path to the blob
	fieldID := pkField.GetFieldID()
	sblob.Key = metautil.BuildStatsLogPath(rootPath,
		c.collectionID, partitionID, segmentID, fieldID, id)

	// Write the blob
	if err := blobsWriter([]*Blob{sblob}); err != nil {
		return nil, err
	}

	// Return as map for interface consistency
	return map[FieldID]*datapb.FieldBinlog{
		fieldID: {
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:    int64(len(sblob.GetValue())),
					MemorySize: int64(len(sblob.GetValue())),
					LogPath:    sblob.Key,
					EntriesNum: rowNum,
				},
			},
		},
	}, nil
}

// NewPkStatsCollector creates a new primary key stats collector
func NewPkStatsCollector(
	collectionID UniqueID,
	schema *schemapb.CollectionSchema,
	maxRowNum int64,
) (*PkStatsCollector, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	stats, err := NewPrimaryKeyStats(pkField.GetFieldID(), int64(pkField.GetDataType()), maxRowNum)
	if err != nil {
		return nil, err
	}

	return &PkStatsCollector{
		pkstats:      stats,
		collectionID: collectionID,
		schema:       schema,
	}, nil
}

// Bm25StatsCollector collects BM25 statistics
type Bm25StatsCollector struct {
	bm25Stats map[int64]*BM25Stats
}

// Collect collects BM25 statistics from the record
func (c *Bm25StatsCollector) Collect(r Record) error {
	if len(c.bm25Stats) == 0 {
		return nil
	}

	rows := r.Len()
	for fieldID, stats := range c.bm25Stats {
		field, ok := r.Column(fieldID).(*array.Binary)
		if !ok {
			return errors.New("bm25 field value not found")
		}
		for i := 0; i < rows; i++ {
			stats.AppendBytes(field.Value(i))
		}
	}
	return nil
}

// Digest serializes the collected BM25 statistics, writes them to storage,
// and returns the field binlog metadata
func (c *Bm25StatsCollector) Digest(
	collectionID, partitionID, segmentID UniqueID,
	rootPath string,
	rowNum int64,
	allocator allocator.Interface,
	blobsWriter ChunkedBlobsWriter,
) (map[FieldID]*datapb.FieldBinlog, error) {
	if len(c.bm25Stats) == 0 {
		return nil, nil
	}

	// Serialize BM25 stats into blobs
	blobs := make([]*Blob, 0, len(c.bm25Stats))
	for fid, stats := range c.bm25Stats {
		bytes, err := stats.Serialize()
		if err != nil {
			return nil, err
		}
		blob := &Blob{
			Key:        strconv.FormatInt(fid, 10), // temporary key, will be replaced below
			Value:      bytes,
			RowNum:     stats.NumRow(),
			MemorySize: int64(len(bytes)),
		}
		blobs = append(blobs, blob)
	}

	// Allocate IDs for stats blobs
	id, _, err := allocator.Alloc(uint32(len(blobs)))
	if err != nil {
		return nil, err
	}

	result := make(map[FieldID]*datapb.FieldBinlog)

	// Process each blob and assign proper paths
	for _, blob := range blobs {
		// Parse the field ID from the temporary key
		fieldID, parseErr := strconv.ParseInt(blob.Key, 10, 64)
		if parseErr != nil {
			// This should not happen for BM25 blobs
			continue
		}

		blob.Key = metautil.BuildBm25LogPath(rootPath,
			collectionID, partitionID, segmentID, fieldID, id)

		result[fieldID] = &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:    int64(len(blob.GetValue())),
					MemorySize: int64(len(blob.GetValue())),
					LogPath:    blob.Key,
					EntriesNum: rowNum,
				},
			},
		}
		id++
	}

	// Write all blobs
	if err := blobsWriter(blobs); err != nil {
		return nil, err
	}

	return result, nil
}

// NewBm25StatsCollector creates a new BM25 stats collector
func NewBm25StatsCollector(schema *schemapb.CollectionSchema) *Bm25StatsCollector {
	bm25FieldIDs := lo.FilterMap(schema.GetFunctions(), func(function *schemapb.FunctionSchema, _ int) (int64, bool) {
		if function.GetType() == schemapb.FunctionType_BM25 {
			return function.GetOutputFieldIds()[0], true
		}
		return 0, false
	})
	bm25Stats := make(map[int64]*BM25Stats, len(bm25FieldIDs))
	for _, fid := range bm25FieldIDs {
		bm25Stats[fid] = NewBM25Stats()
	}

	return &Bm25StatsCollector{
		bm25Stats: bm25Stats,
	}
}
