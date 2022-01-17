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

package querynode

// indexInfo stores index info, such as name, id, index params and so on
type indexInfo struct {
	indexName   string
	indexID     UniqueID
	buildID     UniqueID
	fieldID     UniqueID
	indexPaths  []string
	indexParams map[string]string
	readyLoad   bool
}

// newIndexInfo returns a new indexInfo
func newIndexInfo() *indexInfo {
	return &indexInfo{
		indexPaths:  make([]string, 0),
		indexParams: make(map[string]string),
	}
}

// setIndexName sets the name of index
func (info *indexInfo) setIndexName(name string) {
	info.indexName = name
}

// setIndexID sets the id of index
func (info *indexInfo) setIndexID(id UniqueID) {
	info.indexID = id
}

// setBuildID sets the build id of index
func (info *indexInfo) setBuildID(id UniqueID) {
	info.buildID = id
}

// setFieldID sets the field id of index
func (info *indexInfo) setFieldID(id UniqueID) {
	info.fieldID = id
}

// setIndexPaths sets the index paths
func (info *indexInfo) setIndexPaths(paths []string) {
	info.indexPaths = paths
}

// setIndexParams sets the params of index, such as indexType, metricType and so on
func (info *indexInfo) setIndexParams(params map[string]string) {
	info.indexParams = params
}

// setReadyLoad the flag to check if the index is ready to load
func (info *indexInfo) setReadyLoad(load bool) {
	info.readyLoad = load
}

// getIndexName returns the name of index
func (info *indexInfo) getIndexName() string {
	return info.indexName
}

// getIndexID returns the index id
func (info *indexInfo) getIndexID() UniqueID {
	return info.indexID
}

// getBuildID returns the build id of index
func (info *indexInfo) getBuildID() UniqueID {
	return info.buildID
}

// getFieldID returns filed id of index
func (info *indexInfo) getFieldID() UniqueID {
	return info.fieldID
}

// getIndexPaths returns indexPaths
func (info *indexInfo) getIndexPaths() []string {
	return info.indexPaths
}

// getIndexParams returns indexParams
func (info *indexInfo) getIndexParams() map[string]string {
	return info.indexParams
}

// getReadyLoad returns if index is ready to load
func (info *indexInfo) getReadyLoad() bool {
	return info.readyLoad
}
