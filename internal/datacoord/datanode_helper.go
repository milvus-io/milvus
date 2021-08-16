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

package datacoord

import (
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

// positionProvider provides vchannel pair related position pairs
type positionProvider interface {
	GetVChanPositions(channel string, collectionID UniqueID, seekFromStartPosition bool) *datapb.VchannelInfo
}

var _ positionProvider = (*dummyPosProvider)(nil)

type dummyPosProvider struct{}

//GetVChanPositions implements positionProvider
func (dp dummyPosProvider) GetVChanPositions(channel string, collectionID UniqueID, seekFromStartPosition bool) *datapb.VchannelInfo {
	return &datapb.VchannelInfo{
		CollectionID: collectionID,
		ChannelName:  channel,
	}
}
