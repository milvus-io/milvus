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
	"fmt"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type IndexFileBinlogCodec struct{}

// NewIndexFileBinlogCodec is constructor for IndexFileBinlogCodec
func NewIndexFileBinlogCodec() *IndexFileBinlogCodec {
	return &IndexFileBinlogCodec{}
}

func (codec *IndexFileBinlogCodec) serializeImpl(
	indexBuildID UniqueID,
	version int64,
	collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	fieldID UniqueID,
	indexName string,
	indexID UniqueID,
	key string,
	value []byte,
	ts Timestamp,
) (*Blob, error) {
	writer := NewIndexFileBinlogWriter(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexName, indexID, key)
	defer writer.Close()

	eventWriter, err := writer.NextIndexFileEventWriter()
	if err != nil {
		return nil, err
	}
	defer eventWriter.Close()

	err = eventWriter.AddOneStringToPayload(typeutil.UnsafeBytes2str(value), true)
	if err != nil {
		return nil, err
	}

	eventWriter.SetEventTimestamp(ts, ts)

	writer.SetEventTimeStamp(ts, ts)

	// https://github.com/milvus-io/milvus/issues/9620
	// len(params) is also not accurate, indexParams is a map
	writer.AddExtra(originalSizeKey, fmt.Sprintf("%v", len(value)))

	err = writer.Finish()
	if err != nil {
		return nil, err
	}
	buffer, err := writer.GetBuffer()
	if err != nil {
		return nil, err
	}

	return &Blob{
		Key: key,
		// Key:   strconv.Itoa(len(datas)),
		Value: buffer,
	}, nil
}

// SerializeIndexParams serilizes index params as blob.
func (codec *IndexFileBinlogCodec) SerializeIndexParams(
	indexBuildID UniqueID,
	version int64,
	collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	fieldID UniqueID,
	indexParams map[string]string,
	indexName string,
	indexID UniqueID,
) (*Blob, error) {
	ts := Timestamp(time.Now().UnixNano())

	// save index params.
	// querycoord will parse index extra info from binlog, better to let this key appear first.
	params, _ := json.Marshal(indexParams)
	indexParamBlob, err := codec.serializeImpl(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexName, indexID, IndexParamsKey, params, ts)
	if err != nil {
		return nil, err
	}
	return indexParamBlob, nil
}

// Serialize serilizes data as blobs.
func (codec *IndexFileBinlogCodec) Serialize(
	indexBuildID UniqueID,
	version int64,
	collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	fieldID UniqueID,
	indexParams map[string]string,
	indexName string,
	indexID UniqueID,
	datas []*Blob,
) ([]*Blob, error) {
	var err error

	var blobs []*Blob

	ts := Timestamp(time.Now().UnixNano())

	// save index params.
	// querycoord will parse index extra info from binlog, better to let this key appear first.
	indexParamBlob, err := codec.SerializeIndexParams(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexParams, indexName, indexID)
	if err != nil {
		return nil, err
	}
	blobs = append(blobs, indexParamBlob)

	for pos := range datas {
		blob, err := codec.serializeImpl(indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexName, indexID, datas[pos].Key, datas[pos].Value, ts)
		if err != nil {
			return nil, err
		}
		blobs = append(blobs, blob)
	}

	return blobs, nil
}

func (codec *IndexFileBinlogCodec) DeserializeImpl(blobs []*Blob) (
	indexBuildID UniqueID,
	version int64,
	collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	fieldID UniqueID,
	indexParams map[string]string,
	indexName string,
	indexID UniqueID,
	datas []*Blob,
	err error,
) {
	if len(blobs) == 0 {
		return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, errors.New("blobs is empty")
	}
	indexParams = make(map[string]string)
	datas = make([]*Blob, 0)

	for _, blob := range blobs {
		binlogReader, err := NewBinlogReader(blob.Value)
		if err != nil {
			log.Warn("failed to read binlog",
				zap.Error(err))
			return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, err
		}
		dataType := binlogReader.PayloadDataType

		//desc, err := binlogReader.readDescriptorEvent()
		//if err != nil {
		//	log.Warn("failed to read descriptor event",
		//		zap.Error(err))
		//	return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, err
		//}
		desc := binlogReader.descriptorEvent
		extraBytes := desc.ExtraBytes
		extra := make(map[string]interface{})
		_ = json.Unmarshal(extraBytes, &extra)

		value, _ := strconv.Atoi(extra["indexBuildID"].(string))
		indexBuildID = UniqueID(value)

		value, _ = strconv.Atoi(extra["version"].(string))
		version = int64(value)

		collectionID = desc.CollectionID
		partitionID = desc.PartitionID
		segmentID = desc.SegmentID
		fieldID = desc.FieldID

		indexName = extra["indexName"].(string)

		value, _ = strconv.Atoi(extra["indexID"].(string))
		indexID = UniqueID(value)

		key := extra["key"].(string)

		for {
			eventReader, err := binlogReader.NextEventReader()
			if err != nil {
				log.Warn("failed to get next event reader",
					zap.Error(err))
				binlogReader.Close()
				return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, err
			}
			if eventReader == nil {
				break
			}
			switch dataType {
			// just for backward compatibility
			case schemapb.DataType_Int8:
				// todo: valid_data may need to check when create index
				content, _, err := eventReader.GetByteFromPayload()
				if err != nil {
					log.Warn("failed to get byte from payload",
						zap.Error(err))
					eventReader.Close()
					binlogReader.Close()
					return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, err
				}

				if key == IndexParamsKey {
					_ = json.Unmarshal(content, &indexParams)
				} else {
					blob := &Blob{Key: key}
					blob.Value = content
					datas = append(datas, blob)
				}

			case schemapb.DataType_String:
				content, _, err := eventReader.GetStringFromPayload()
				if err != nil {
					log.Warn("failed to get string from payload", zap.Error(err))
					eventReader.Close()
					binlogReader.Close()
					return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, err
				}

				// make sure there is one string
				if len(content) != 1 {
					err := fmt.Errorf("failed to parse index event because content length is not one %d", len(content))
					eventReader.Close()
					binlogReader.Close()
					return 0, 0, 0, 0, 0, 0, nil, "", 0, nil, err
				}
				contentByte := typeutil.UnsafeStr2bytes(content[0])
				if key == IndexParamsKey {
					_ = json.Unmarshal(contentByte, &indexParams)
				} else {
					blob := &Blob{Key: key}
					blob.Value = contentByte
					datas = append(datas, blob)
				}
			}
			eventReader.Close()
		}
		binlogReader.Close()
	}

	return indexBuildID, version, collectionID, partitionID, segmentID, fieldID, indexParams, indexName, indexID, datas, nil
}

func (codec *IndexFileBinlogCodec) Deserialize(blobs []*Blob) (
	datas []*Blob,
	indexParams map[string]string,
	indexName string,
	indexID UniqueID,
	err error,
) {
	_, _, _, _, _, _, indexParams, indexName, indexID, datas, err = codec.DeserializeImpl(blobs)
	return datas, indexParams, indexName, indexID, err
}

// IndexCodec can serialize and deserialize index
type IndexCodec struct{}

// NewIndexCodec creates IndexCodec
func NewIndexCodec() *IndexCodec {
	return &IndexCodec{}
}

// Serialize serializes index
func (indexCodec *IndexCodec) Serialize(blobs []*Blob, params map[string]string, indexName string, indexID UniqueID) ([]*Blob, error) {
	paramsBytes, err := json.Marshal(struct {
		Params    map[string]string
		IndexName string
		IndexID   UniqueID
	}{
		Params:    params,
		IndexName: indexName,
		IndexID:   indexID,
	})
	if err != nil {
		return nil, err
	}
	blobs = append(blobs, &Blob{Key: IndexParamsKey, Value: paramsBytes})
	return blobs, nil
}

// Deserialize deserializes index
func (indexCodec *IndexCodec) Deserialize(blobs []*Blob) ([]*Blob, map[string]string, string, UniqueID, error) {
	var file *Blob
	for i := 0; i < len(blobs); i++ {
		if blobs[i].Key != IndexParamsKey {
			continue
		}
		file = blobs[i]
		blobs = append(blobs[:i], blobs[i+1:]...)
		break
	}
	if file == nil {
		return nil, nil, "", InvalidUniqueID, errors.New("can not find params blob")
	}
	info := struct {
		Params    map[string]string
		IndexName string
		IndexID   UniqueID
	}{}
	if err := json.Unmarshal(file.Value, &info); err != nil {
		return nil, nil, "", InvalidUniqueID, fmt.Errorf("json unmarshal error: %s", err.Error())
	}

	return blobs, info.Params, info.IndexName, info.IndexID, nil
}

// NewIndexFileBinlogWriter returns a new IndexFileBinlogWriter with provided parameters
func NewIndexFileBinlogWriter(
	indexBuildID UniqueID,
	version int64,
	collectionID UniqueID,
	partitionID UniqueID,
	segmentID UniqueID,
	fieldID UniqueID,
	indexName string,
	indexID UniqueID,
	key string,
) *IndexFileBinlogWriter {
	descriptorEvent := newDescriptorEvent()
	descriptorEvent.CollectionID = collectionID
	descriptorEvent.PartitionID = partitionID
	descriptorEvent.SegmentID = segmentID
	descriptorEvent.FieldID = fieldID
	descriptorEvent.PayloadDataType = schemapb.DataType_String
	descriptorEvent.AddExtra("indexBuildID", fmt.Sprintf("%d", indexBuildID))
	descriptorEvent.AddExtra("version", fmt.Sprintf("%d", version))
	descriptorEvent.AddExtra("indexName", indexName)
	descriptorEvent.AddExtra("indexID", fmt.Sprintf("%d", indexID))
	descriptorEvent.AddExtra("key", key)
	w := &IndexFileBinlogWriter{
		baseBinlogWriter: baseBinlogWriter{
			descriptorEvent: *descriptorEvent,
			magicNumber:     MagicNumber,
			binlogType:      IndexFileBinlog,
			eventWriters:    make([]EventWriter, 0),
			buffer:          nil,
		},
	}
	return w
}
