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

type VectorFileManager struct {
	localFileManager  FileManager
	remoteFileManager FileManager

	insertCodec *InsertCodec
}

func (vfm *VectorFileManager) GetFile(key string) error {
	if vfm.localFileManager.Exist(key) {
		return nil
	}
	content, err := vfm.remoteFileManager.ReadAll(key)
	if err != nil {
		return err
	}
	blob := &Blob{
		Key:   key,
		Value: content,
	}

	_, _, data, err := vfm.insertCodec.Deserialize([]*Blob{blob})
	if err != nil {
		return err
	}

	for _, singleData := range data.Data {
		binaryVector, ok := singleData.(*BinaryVectorFieldData)
		if ok {
			vfm.localFileManager.PutFile(key, binaryVector.Data)
		}
		floatVector, ok := singleData.(*FloatVectorFieldData)
		if ok {
			floatData := floatVector.Data
			result := make([]byte, len(floatData)*8)
			for _, singleFloat := range floatData {
				result = append(result, Float32ToByte(singleFloat)...)
			}
			vfm.localFileManager.PutFile(key, result)
		}
	}
	return vfm.localFileManager.GetFile(key)
}

func (vfm *VectorFileManager) PutFile(key string, content []byte) error {
	return vfm.localFileManager.PutFile(key, content)
}

func (vfm *VectorFileManager) Exist(key string) bool {
	return vfm.localFileManager.Exist(key)
}

func (vfm *VectorFileManager) ReadAll(key string) ([]byte, error) {
	if vfm.localFileManager.Exist(key) {
		return vfm.localFileManager.ReadAll(key)
	}
	err := vfm.GetFile(key)
	if err != nil {
		return nil, err
	}
	return vfm.localFileManager.ReadAll(key)
}

func (vfm *VectorFileManager) ReadAt(p []byte, off int64) (n int, err error) {
	return vfm.localFileManager.ReadAt(p, off)
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
