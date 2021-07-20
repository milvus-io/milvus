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

package storage

import (
	"encoding/binary"
	"math"

	"github.com/milvus-io/milvus/internal/proto/etcdpb"
)

type VectorChunkManager struct {
	localChunkManager  ChunkManager
	remoteChunkManager ChunkManager

	insertCodec *InsertCodec
}

func NewVectorChunkManager(localChunkManager ChunkManager, remoteChunkManager ChunkManager, schema *etcdpb.CollectionMeta) *VectorChunkManager {
	insertCodec := NewInsertCodec(schema)
	return &VectorChunkManager{
		localChunkManager:  localChunkManager,
		remoteChunkManager: remoteChunkManager,
		insertCodec:        insertCodec,
	}
}

func (vcm *VectorChunkManager) Load(key string) (string, error) {
	if vcm.localChunkManager.Exist(key) {
		return vcm.localChunkManager.Load(key)
	}
	content, err := vcm.remoteChunkManager.ReadAll(key)
	if err != nil {
		return "", err
	}
	blob := &Blob{
		Key:   key,
		Value: content,
	}

	_, _, data, err := vcm.insertCodec.Deserialize([]*Blob{blob})
	if err != nil {
		return "", err
	}

	for _, singleData := range data.Data {
		binaryVector, ok := singleData.(*BinaryVectorFieldData)
		if ok {
			vcm.localChunkManager.Write(key, binaryVector.Data)
		}
		floatVector, ok := singleData.(*FloatVectorFieldData)
		if ok {
			floatData := floatVector.Data
			result := make([]byte, 0)
			for _, singleFloat := range floatData {
				result = append(result, Float32ToByte(singleFloat)...)
			}
			vcm.localChunkManager.Write(key, result)
		}
	}
	vcm.insertCodec.Close()
	return vcm.localChunkManager.Load(key)
}

func (vcm *VectorChunkManager) Write(key string, content []byte) error {
	return vcm.localChunkManager.Write(key, content)
}

func (vcm *VectorChunkManager) Exist(key string) bool {
	return vcm.localChunkManager.Exist(key)
}

func (vcm *VectorChunkManager) ReadAll(key string) ([]byte, error) {
	if vcm.localChunkManager.Exist(key) {
		return vcm.localChunkManager.ReadAll(key)
	}
	_, err := vcm.Load(key)
	if err != nil {
		return nil, err
	}
	return vcm.localChunkManager.ReadAll(key)
}

func (vcm *VectorChunkManager) ReadAt(key string, p []byte, off int64) (n int, err error) {
	return vcm.localChunkManager.ReadAt(key, p, off)
}

func Float32ToByte(float float32) []byte {
	bits := math.Float32bits(float)
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, bits)

	return bytes
}

func ByteToFloat32(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)

	return math.Float32frombits(bits)
}
