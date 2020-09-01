// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"encoding/json"
	"path"
	"sync"
	"github.com/gogo/protobuf/proto"
	"github.com/czs007/suvlim/errors"
	"github.com/czs007/suvlim/pkg/metapb"
	"github.com/czs007/suvlim/master/kv"
)

const (
	clusterPath              = "raft"
	configPath               = "config"
)


// Storage wraps all kv operations, keep it stateless.
type Storage struct {
	kv.Base
	mu               sync.Mutex
}

// NewStorage creates Storage instance with Base.
func NewStorage(base kv.Base) *Storage {
	return &Storage{
		Base: base,
	}
}

// LoadMeta loads cluster meta from storage.
func (s *Storage) LoadMeta(meta *metapb.Cluster) (bool, error) {
	return loadProto(s.Base, clusterPath, meta)
}

// SaveMeta save cluster meta to storage.
func (s *Storage) SaveMeta(meta *metapb.Cluster) error {
	return saveProto(s.Base, clusterPath, meta)
}

func (s *Storage) SaveConfig(cfg interface{}) error {
	value, err := json.Marshal(cfg)
	if err != nil {
		return errors.WithStack(err)
	}
	return s.Save(configPath, string(value))
}

// LoadConfig loads config from configPath then unmarshal it to cfg.
func (s *Storage) LoadConfig(cfg interface{}) (bool, error) {
	value, err := s.Load(configPath)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = json.Unmarshal([]byte(value), cfg)
	if err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}

// SaveJSON saves json format data to storage.
func (s *Storage) SaveJSON(prefix, key string, data interface{}) error {
	value, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return s.Save(path.Join(prefix, key), string(value))
}


// Close closes the s.
func (s *Storage) Close() error {
	return nil
}

func loadProto(s kv.Base, key string, msg proto.Message) (bool, error) {
	value, err := s.Load(key)
	if err != nil {
		return false, err
	}
	if value == "" {
		return false, nil
	}
	err = proto.Unmarshal([]byte(value), msg)
	return true, errors.WithStack(err)
}

func saveProto(s kv.Base, key string, msg proto.Message) error {
	value, err := proto.Marshal(msg)
	if err != nil {
		return errors.WithStack(err)
	}
	return s.Save(key, string(value))
}
