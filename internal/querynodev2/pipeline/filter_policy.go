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

//MsgFilter will return error if Msg was invalid
type InsertMsgFilter = func(n *filterNode, c *Collection, msg *InsertMsg) error

type DeleteMsgFilter = func(n *filterNode, c *Collection, msg *DeleteMsg) error

//Chack msg is aligned --
//len of each kind of infos in InsertMsg should match each other
func InsertNotAligned(n *filterNode, c *Collection, msg *InsertMsg) error {
	err := msg.CheckAligned()
	if err != nil {
		return WrapErrMsgNotAligned(err)
	}
	return nil
}

func InsertEmpty(n *filterNode, c *Collection, msg *InsertMsg) error {
	if len(msg.GetTimestamps()) <= 0 {
		return ErrMsgEmpty
	}
	return nil
}

func InsertOutOfTarget(n *filterNode, c *Collection, msg *InsertMsg) error {
	if msg.GetCollectionID() != c.ID() {
		return WrapErrMsgNotTarget("Collection")
	}

	// all growing will be be in-memory to support dynamic partition load/release
	return nil
}

func InsertExcluded(n *filterNode, c *Collection, msg *InsertMsg) error {
	segInfo, ok := n.excludedSegments.Get(msg.SegmentID)
	if !ok {
		return nil
	}
	if msg.EndTimestamp <= segInfo.GetDmlPosition().GetTimestamp() {
		return WrapErrMsgExcluded(msg.SegmentID)
	}
	return nil
}

func DeleteNotAligned(n *filterNode, c *Collection, msg *DeleteMsg) error {
	err := msg.CheckAligned()
	if err != nil {
		return WrapErrMsgNotAligned(err)
	}
	return nil
}

func DeleteEmpty(n *filterNode, c *Collection, msg *DeleteMsg) error {
	if len(msg.GetTimestamps()) <= 0 {
		return ErrMsgEmpty
	}
	return nil
}

func DeleteOutOfTarget(n *filterNode, c *Collection, msg *DeleteMsg) error {
	if msg.GetCollectionID() != c.ID() {
		return WrapErrMsgNotTarget("Collection")
	}

	// all growing will be be in-memory to support dynamic partition load/release
	return nil
}
