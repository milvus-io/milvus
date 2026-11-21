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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storage/deltacodec"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type record struct {
	Key      string          `json:"key"`
	Revision int64           `json:"revision"`
	Data     json.RawMessage `json:"data"`
}

func run() error {
	paramtable.Init()
	p := paramtable.Get()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cli, err := clientv3.New(clientv3.Config{Endpoints: p.EtcdCfg.Endpoints.GetAsStrings(), DialTimeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer cli.Close()
	cfg := compaction.CreateStorageConfig()
	cm, err := storage.NewChunkManagerFactoryWithParam(p).NewPersistentStorageChunkManager(ctx)
	if err != nil {
		return err
	}
	base := p.EtcdCfg.RootPath.GetValue() + "/meta/"
	enc := json.NewEncoder(os.Stdout)
	segments := map[int64]*datapb.SegmentInfo{}
	var revision int64
	for _, prefix := range []string{"coord/dv/", "streamingcoord-meta/pchannel/", "streamingnode-meta/wal/", "datacoord-meta/s/", "datacoord-meta/deltalog/", "datacoord-meta/compaction-task/"} {
		resp, err := cli.Get(ctx, base+prefix, clientv3.WithPrefix(), clientv3.WithRev(revision))
		if err != nil {
			return err
		}
		if revision == 0 {
			revision = resp.Header.Revision
		}
		for _, kv := range resp.Kvs {
			key := strings.TrimPrefix(string(kv.Key), base)
			parts := strings.Split(key, "/")
			var msg proto.Message
			switch prefix {
			case "coord/dv/":
				msg = &viewpb.DataViewOfCollection{}
			case "streamingcoord-meta/pchannel/":
				msg = &streamingpb.PChannelMeta{}
			case "streamingnode-meta/wal/":
				if len(parts) > 4 && parts[3] == "qv" {
					msg = &viewpb.QueryViewOfShard{}
					break
				}
				if len(parts) != 5 || parts[3] != "vchannel" {
					continue
				}
				msg = &streamingpb.VChannelMeta{}
			case "datacoord-meta/s/":
				msg = &datapb.SegmentInfo{}
			case "datacoord-meta/compaction-task/":
				msg = &datapb.CompactionTask{}
			case "datacoord-meta/deltalog/":
				msg = &datapb.FieldBinlog{}
			}
			if err := proto.Unmarshal(kv.Value, msg); err != nil {
				return err
			}
			data, err := protojson.Marshal(msg)
			if err != nil {
				return err
			}
			if err := enc.Encode(record{key, kv.ModRevision, data}); err != nil {
				return err
			}
			var files []*datapb.Binlog
			switch m := msg.(type) {
			case *datapb.SegmentInfo:
				segments[m.ID] = m
				if m.ManifestPath != "" && m.State.String() != "Dropped" {
					paths, err := packed.GetDeltaLogPathsFromManifest(m.ManifestPath, cfg)
					if err != nil {
						return err
					}
					for _, path := range paths {
						files = append(files, &datapb.Binlog{LogPath: path})
					}
				}
			case *datapb.FieldBinlog:
				if len(parts) != 6 {
					return merr.WrapErrDataIntegrityMsg("unexpected deltalog key %s", key)
				}
				id, err := strconv.ParseInt(parts[4], 10, 64)
				if err != nil {
					return err
				}
				s := segments[id]
				if s == nil || s.State.String() == "Dropped" {
					continue
				}
				for _, file := range m.Binlogs {
					if file.LogPath == "" {
						file.LogPath = metautil.BuildDeltaLogPath(cfg.RootPath, s.CollectionID, s.PartitionID, s.ID, file.LogID)
					}
					files = append(files, file)
				}
			}
			for _, f := range files {
				err := storage.ReadMutationDelta(ctx, cm, f, storage.StorageV2, schemapb.DataType_Int64, nil, cfg, nil, func(l deltacodec.Layout, rows []deltacodec.Row) error {
					return enc.Encode(map[string]any{"file": f.LogPath, "referrer": key, "layout": l, "rows": rows})
				})
				if err != nil {
					return merr.Wrapf(err, "decode %q referenced by %s", f.LogPath, key)
				}
			}
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
