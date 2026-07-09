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

// Command l0fanout is a stateless codec primitive for the milvus-backup "wash"
// flow. It reads L0 (delete-only) deltalogs and writes their deletes into new
// per-data-segment deltalogs — the offline equivalent of L0 compaction's
// fan-out. It does NOT rewrite insert data, does NOT read or write backup
// metadata, and has no knowledge of the backup format: the orchestrator
// (milvus-backup) reads the backup metadata, computes the fan-out plan (which
// L0 deltalogs go to which data segments and where), invokes this tool with
// explicit paths, and updates the backup metadata from the reported results.
//
// Usage:
//
//	l0fanout --config milvus.yaml --plan plan.json [--output result.json]
//
// The plan and result are JSON (see Plan / Result below). `--plan -` reads the
// plan from stdin; `--output -` (default) writes the result to stdout.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	fio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Plan is the tool input: a set of fan-out tasks.
type Plan struct {
	Tasks []Task `json:"tasks"`
}

// Task fans one group of L0 deltalogs out into one or more data segments.
// L0DeltaPaths are the source L0 deltalog object keys (already listed by the
// orchestrator). Targets are the data segments to receive the deletes. No PK
// routing is performed: each target receives the whole delete set, and restore
// applies it by primary key (non-matching PKs are no-ops), so Targets must
// already be partition-scoped by the orchestrator.
type Task struct {
	L0DeltaPaths     []string `json:"l0DeltaPaths"`
	L0StorageVersion int64    `json:"l0StorageVersion"` // 0=v1 (default), 2/3=v2/v3
	PkType           string   `json:"pkType"`           // "Int64" | "VarChar"
	Targets          []Target `json:"targets"`
}

// Target identifies a data segment and the output deltalog to write.
type Target struct {
	CollectionID   int64  `json:"collectionID"`
	PartitionID    int64  `json:"partitionID"`
	SegmentID      int64  `json:"segmentID"`
	LogID          int64  `json:"logID"`          // unique within the backup; embedded in the deltalog
	OutputPath     string `json:"outputPath"`     // full object key to write (backup layout)
	StorageVersion int64  `json:"storageVersion"` // target segment's storage version
}

// Result is the tool output: one entry per written deltalog.
type Result struct {
	Written []WrittenDelta `json:"written"`
}

// WrittenDelta describes a deltalog written for a target segment, enough for the
// orchestrator to append a Deltalog FieldBinlog entry to the backup metadata.
type WrittenDelta struct {
	SegmentID     int64  `json:"segmentID"`
	LogID         int64  `json:"logID"`
	Path          string `json:"path"`
	EntriesNum    int64  `json:"entriesNum"`
	MemorySize    int64  `json:"memorySize"`
	TimestampFrom uint64 `json:"timestampFrom"`
	TimestampTo   uint64 `json:"timestampTo"`
}

func main() {
	configPath := flag.String("config", "milvus.yaml", "path to a milvus.yaml providing object-storage (minio) settings")
	planPath := flag.String("plan", "-", "path to the JSON plan, or - for stdin")
	outputPath := flag.String("output", "-", "path to write the JSON result, or - for stdout")
	flag.Parse()

	if err := run(*configPath, *planPath, *outputPath); err != nil {
		fmt.Fprintf(os.Stderr, "l0fanout: %s\n", err.Error())
		os.Exit(1)
	}
}

func run(configPath, planPath, outputPath string) error {
	ctx := context.Background()

	plan, err := readPlan(planPath)
	if err != nil {
		return fmt.Errorf("read plan: %w", err)
	}

	paramtable.Get().Init(paramtable.NewBaseTableFromYamlOnly(configPath))
	params := paramtable.Get()

	cm, err := storage.NewChunkManagerFactoryWithParam(params).NewPersistentStorageChunkManager(ctx)
	if err != nil {
		return fmt.Errorf("create chunk manager: %w", err)
	}
	bio := fio.NewBinlogIO(cm)
	sc := buildStorageConfig(params)

	result := &Result{}
	for ti := range plan.Tasks {
		task := &plan.Tasks[ti]
		pkType, err := parsePkType(task.PkType)
		if err != nil {
			return fmt.Errorf("task %d: %w", ti, err)
		}

		pks, tss, err := readDeltas(ctx, task.L0DeltaPaths, pkType, task.L0StorageVersion, bio, sc)
		if err != nil {
			return fmt.Errorf("task %d: read L0 deltalogs: %w", ti, err)
		}
		if len(pks) == 0 {
			// No deletes to fan out; the orchestrator can simply drop the L0 segment.
			continue
		}

		for _, target := range task.Targets {
			written, err := writeDelta(ctx, target, pkType, pks, tss, bio, sc)
			if err != nil {
				return fmt.Errorf("task %d segment %d: write deltalog: %w", ti, target.SegmentID, err)
			}
			result.Written = append(result.Written, written)
		}
	}

	return writeResult(outputPath, result)
}

// readDeltas reads all L0 deltalog files into (pks, tss). It mirrors the record
// iteration used by L0 compaction (compaction.readFromReader).
func readDeltas(ctx context.Context, paths []string, pkType schemapb.DataType, version int64,
	bio fio.BinlogIO, sc *indexpb.StorageConfig,
) ([]storage.PrimaryKey, []storage.Timestamp, error) {
	if len(paths) == 0 {
		return nil, nil, nil
	}
	reader, err := storage.NewDeltalogReader(pkType, paths,
		storage.WithVersion(version),
		storage.WithDownloader(bio.Download),
		storage.WithStorageConfig(sc),
	)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Close()

	var pks []storage.PrimaryKey
	var tss []storage.Timestamp
	for {
		rec, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, nil, err
		}
		for i := 0; i < rec.Len(); i++ {
			var pk storage.PrimaryKey
			if pkType == schemapb.DataType_Int64 {
				pk = storage.NewInt64PrimaryKey(rec.Column(0).(*array.Int64).Value(i))
			} else {
				pk = storage.NewVarCharPrimaryKey(rec.Column(0).(*array.String).Value(i))
			}
			pks = append(pks, pk)
			tss = append(tss, storage.Timestamp(rec.Column(1).(*array.Int64).Value(i)))
		}
	}
	return pks, tss, nil
}

// writeDelta writes one deltalog containing (pks, tss) for the target segment.
func writeDelta(ctx context.Context, target Target, pkType schemapb.DataType,
	pks []storage.PrimaryKey, tss []storage.Timestamp,
	bio fio.BinlogIO, sc *indexpb.StorageConfig,
) (WrittenDelta, error) {
	record, tsFrom, tsTo, err := storage.BuildDeleteRecord(pks, tss)
	if err != nil {
		return WrittenDelta{}, err
	}
	defer record.Release()

	writer, err := storage.NewDeltalogWriter(ctx,
		target.CollectionID, target.PartitionID, target.SegmentID, target.LogID,
		pkType, target.OutputPath,
		storage.WithUploader(bio.Upload),
		storage.WithStorageConfig(sc),
		storage.WithVersion(target.StorageVersion),
	)
	if err != nil {
		return WrittenDelta{}, err
	}
	if err := writer.Write(record); err != nil {
		return WrittenDelta{}, err
	}
	if err := writer.Close(); err != nil {
		return WrittenDelta{}, err
	}

	return WrittenDelta{
		SegmentID:     target.SegmentID,
		LogID:         target.LogID,
		Path:          target.OutputPath,
		EntriesNum:    int64(len(pks)),
		MemorySize:    int64(writer.GetWrittenUncompressed()),
		TimestampFrom: tsFrom,
		TimestampTo:   tsTo,
	}, nil
}

func parsePkType(s string) (schemapb.DataType, error) {
	switch s {
	case "Int64", "int64":
		return schemapb.DataType_Int64, nil
	case "VarChar", "varchar", "VarCharacter":
		return schemapb.DataType_VarChar, nil
	default:
		return schemapb.DataType_None, fmt.Errorf("unsupported pkType %q (want Int64 or VarChar)", s)
	}
}

// buildStorageConfig mirrors datacoord.createStorageConfig: it builds an
// indexpb.StorageConfig from paramtable so the storage-v2/v3 codec can resolve
// the bucket.
func buildStorageConfig(params *paramtable.ComponentParam) *indexpb.StorageConfig {
	if params.CommonCfg.StorageType.GetValue() == "local" {
		return &indexpb.StorageConfig{
			RootPath:    params.LocalStorageCfg.Path.GetValue(),
			StorageType: params.CommonCfg.StorageType.GetValue(),
		}
	}
	return &indexpb.StorageConfig{
		Address:           params.MinioCfg.Address.GetValue(),
		AccessKeyID:       params.MinioCfg.AccessKeyID.GetValue(),
		SecretAccessKey:   params.MinioCfg.SecretAccessKey.GetValue(),
		UseSSL:            params.MinioCfg.UseSSL.GetAsBool(),
		SslCACert:         params.MinioCfg.SslCACert.GetValue(),
		BucketName:        params.MinioCfg.BucketName.GetValue(),
		RootPath:          params.MinioCfg.RootPath.GetValue(),
		UseIAM:            params.MinioCfg.UseIAM.GetAsBool(),
		IAMEndpoint:       params.MinioCfg.IAMEndpoint.GetValue(),
		StorageType:       params.CommonCfg.StorageType.GetValue(),
		Region:            params.MinioCfg.Region.GetValue(),
		UseVirtualHost:    params.MinioCfg.UseVirtualHost.GetAsBool(),
		CloudProvider:     params.MinioCfg.CloudProvider.GetValue(),
		RequestTimeoutMs:  params.MinioCfg.RequestTimeoutMs.GetAsInt64(),
		GcpCredentialJSON: params.MinioCfg.GcpCredentialJSON.GetValue(),
		SslTlsMinVersion:  params.MinioCfg.SslTLSMinVersion.GetValue(),
		UseCrc32CChecksum: params.MinioCfg.UseCRC32C.GetAsBool(),
	}
}

func readPlan(planPath string) (*Plan, error) {
	var data []byte
	var err error
	if planPath == "-" {
		data, err = io.ReadAll(os.Stdin)
	} else {
		data, err = os.ReadFile(planPath)
	}
	if err != nil {
		return nil, err
	}
	plan := &Plan{}
	if err := json.Unmarshal(data, plan); err != nil {
		return nil, err
	}
	return plan, nil
}

func writeResult(outputPath string, result *Result) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if outputPath == "-" {
		_, err = os.Stdout.Write(append(data, '\n'))
		return err
	}
	return os.WriteFile(outputPath, data, 0o644)
}
