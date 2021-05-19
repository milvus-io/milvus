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

package dataservice

import (
	"errors"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/msgstream"
)

type streamType int

const (
	_ streamType = iota
	streamTypeFlush
	streamTypeStats
)

var (
	errInvalidStreamType = errors.New("invalid stream type")
)

// storeStreamPos store current processed stream pos
func (s *Server) storeStreamPos(st streamType, pos *msgstream.MsgPosition) error {
	key := s.streamTypeSubKey(st)
	if key == "" {
		return errInvalidStreamType
	}
	val := proto.MarshalTextString(pos)
	err := s.kvClient.Save(key, val)
	if err != nil {
		return err
	}
	return nil
}

// loadStreamLastPos load last successful pos with specified stream type
func (s *Server) loadStreamLastPos(st streamType) (pos *msgstream.MsgPosition, err error) {
	key := s.streamTypeSubKey(st)
	if key == "" {
		return nil, errInvalidStreamType
	}
	var val string
	pos = &msgstream.MsgPosition{}
	val, err = s.kvClient.Load(key)
	if err != nil {
		return pos, err
	}
	err = proto.UnmarshalText(val, pos)
	return pos, err
}

// streamTypeSubKey converts stream type to corresponding k-v store key
func (s *Server) streamTypeSubKey(st streamType) string {
	switch st {
	case streamTypeFlush:
		return Params.FlushStreamPosSubPath
	case streamTypeStats:
		return Params.StatsStreamPosSubPath
	default:
		return ""
	}
}
