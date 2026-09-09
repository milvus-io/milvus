// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package packed

import "github.com/milvus-io/milvus/pkg/v3/proto/datapb"

// ColumnGroupEntriesToProto marshals column-group descriptors for the wire so
// the datanode can hand a schema-bump materialization's manifest transaction
// input to DataCoord. The reverse is ColumnGroupEntriesFromProto.
func ColumnGroupEntriesToProto(entries []ColumnGroupEntry) []*datapb.ManifestColumnGroup {
	if len(entries) == 0 {
		return nil
	}
	out := make([]*datapb.ManifestColumnGroup, 0, len(entries))
	for _, e := range entries {
		files := make([]*datapb.ManifestColumnGroupFile, 0, len(e.Files))
		for _, f := range e.Files {
			files = append(files, &datapb.ManifestColumnGroupFile{
				Path:       f.Path,
				StartIndex: f.StartIndex,
				EndIndex:   f.EndIndex,
				Properties: f.Properties,
			})
		}
		out = append(out, &datapb.ManifestColumnGroup{
			Columns: e.Columns,
			Format:  e.Format,
			Files:   files,
		})
	}
	return out
}

// ColumnGroupEntriesFromProto reconstructs column-group descriptors on
// DataCoord from the datanode-shipped proto.
func ColumnGroupEntriesFromProto(groups []*datapb.ManifestColumnGroup) []ColumnGroupEntry {
	if len(groups) == 0 {
		return nil
	}
	out := make([]ColumnGroupEntry, 0, len(groups))
	for _, g := range groups {
		files := make([]ColumnGroupFileEntry, 0, len(g.GetFiles()))
		for _, f := range g.GetFiles() {
			files = append(files, ColumnGroupFileEntry{
				Path:       f.GetPath(),
				StartIndex: f.GetStartIndex(),
				EndIndex:   f.GetEndIndex(),
				Properties: f.GetProperties(),
			})
		}
		out = append(out, ColumnGroupEntry{
			Columns: g.GetColumns(),
			Format:  g.GetFormat(),
			Files:   files,
		})
	}
	return out
}

// StatEntriesToProto marshals manifest stat entries (e.g. BM25) for the wire.
func StatEntriesToProto(entries []StatEntry) []*datapb.ManifestStatEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]*datapb.ManifestStatEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, &datapb.ManifestStatEntry{
			Key:      e.Key,
			Files:    e.Files,
			Metadata: e.Metadata,
		})
	}
	return out
}

// StatEntriesFromProto reconstructs manifest stat entries on DataCoord.
func StatEntriesFromProto(entries []*datapb.ManifestStatEntry) []StatEntry {
	if len(entries) == 0 {
		return nil
	}
	out := make([]StatEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, StatEntry{
			Key:      e.GetKey(),
			Files:    e.GetFiles(),
			Metadata: e.GetMetadata(),
		})
	}
	return out
}
