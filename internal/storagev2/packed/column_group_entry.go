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

/*
#cgo pkg-config: milvus_core milvus-storage

#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ColumnGroupEntry is the serializable form of a LoonColumnGroup: the plain
// data (column names, format, and per-file path/row-range/properties) that a
// manifest transaction needs to register a column group. Unlike the native
// *ColumnGroups payload returned by an FFI writer's Close, a ColumnGroupEntry
// owns no C memory and crosses the datanode->DataCoord RPC boundary, so
// DataCoord can re-run the transaction on the current manifest version.
// Mirrors LoonColumnGroup in milvus-storage's ffi_c.h.
type ColumnGroupEntry struct {
	Columns []string
	Format  string
	Files   []ColumnGroupFileEntry
}

// ColumnGroupFileEntry mirrors LoonColumnGroupFile: one file in a column group
// with its inclusive-start/exclusive-end row range and extensible properties.
type ColumnGroupFileEntry struct {
	Path       string
	StartIndex int64
	EndIndex   int64
	Properties map[string]string
}

// ColumnGroupEntries extracts the serializable descriptors of the column
// groups this writer output holds. It reads the native LoonColumnGroups the
// C writer produced without taking ownership: the caller must still Destroy
// the ColumnGroups afterwards. Every property key/value is preserved so the
// descriptor round-trips into an identical transaction add on DataCoord.
func (f *ColumnGroups) ColumnGroupEntries() ([]ColumnGroupEntry, error) {
	if f == nil || f.cColumnGroups == nil {
		return nil, nil
	}
	return columnGroupEntriesFromC(f.cColumnGroups)
}

func columnGroupEntriesFromC(cColumnGroups *C.LoonColumnGroups) ([]ColumnGroupEntry, error) {
	if cColumnGroups == nil {
		return nil, nil
	}
	num := int(cColumnGroups.num_of_column_groups)
	if num == 0 {
		return nil, nil
	}
	if cColumnGroups.column_group_array == nil {
		return nil, merr.WrapErrServiceInternalMsg("column_group_array is nil but num_of_column_groups is %d", num)
	}
	cgArray := unsafe.Slice(cColumnGroups.column_group_array, num)
	entries := make([]ColumnGroupEntry, 0, num)
	for i := range cgArray {
		cg := &cgArray[i]
		entry := ColumnGroupEntry{Format: C.GoString(cg.format)}

		if cg.columns == nil && cg.num_of_columns > 0 {
			return nil, merr.WrapErrServiceInternalMsg("columns array is nil but num_of_columns is %d in column group %d", cg.num_of_columns, i)
		}
		if cg.columns != nil {
			columnArray := unsafe.Slice(cg.columns, int(cg.num_of_columns))
			entry.Columns = make([]string, 0, len(columnArray))
			for _, cColumn := range columnArray {
				if cColumn == nil {
					return nil, merr.WrapErrServiceInternalMsg("nil column name in column group %d", i)
				}
				entry.Columns = append(entry.Columns, C.GoString(cColumn))
			}
		}

		if cg.files == nil && cg.num_of_files > 0 {
			return nil, merr.WrapErrServiceInternalMsg("files array is nil but num_of_files is %d in column group %d", cg.num_of_files, i)
		}
		if cg.files != nil {
			fileArray := unsafe.Slice(cg.files, int(cg.num_of_files))
			entry.Files = make([]ColumnGroupFileEntry, 0, len(fileArray))
			for j := range fileArray {
				file := &fileArray[j]
				if file.path == nil {
					return nil, merr.WrapErrServiceInternalMsg("nil file path in column group %d file %d", i, j)
				}
				fileEntry := ColumnGroupFileEntry{
					Path:       C.GoString(file.path),
					StartIndex: int64(file.start_index),
					EndIndex:   int64(file.end_index),
				}
				if file.num_properties > 0 {
					if file.property_keys == nil || file.property_values == nil {
						return nil, merr.WrapErrServiceInternalMsg("column group %d file %d has %d properties but nil keys/values", i, j, file.num_properties)
					}
					keys := unsafe.Slice(file.property_keys, int(file.num_properties))
					values := unsafe.Slice(file.property_values, int(file.num_properties))
					fileEntry.Properties = make(map[string]string, int(file.num_properties))
					for k := range keys {
						if keys[k] == nil || values[k] == nil {
							continue
						}
						fileEntry.Properties[C.GoString(keys[k])] = C.GoString(values[k])
					}
				}
				entry.Files = append(entry.Files, fileEntry)
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// addColumnGroupEntries stages each serialized column group onto a loon
// transaction via loon_transaction_add_column_group — the same FFI the native
// *ColumnGroups.applyTo uses for the add-new-column-group case, but driven from
// serializable descriptors instead of writer-owned C memory. All C allocations
// made to build the temporary LoonColumnGroup structs are freed before return.
func addColumnGroupEntries(handle C.LoonTransactionHandle, entries []ColumnGroupEntry) error {
	for idx := range entries {
		if err := addOneColumnGroupEntry(handle, entries[idx]); err != nil {
			return err
		}
	}
	return nil
}

func addOneColumnGroupEntry(handle C.LoonTransactionHandle, entry ColumnGroupEntry) error {
	var frees []unsafe.Pointer
	freeAll := func() {
		for _, p := range frees {
			C.free(p)
		}
	}
	defer freeAll()

	cstr := func(s string) *C.char {
		p := C.CString(s)
		frees = append(frees, unsafe.Pointer(p))
		return p
	}
	// alloc returns zero-initialized C memory of size n*elemSize.
	alloc := func(n int, elemSize uintptr) unsafe.Pointer {
		p := C.calloc(C.size_t(n), C.size_t(elemSize))
		frees = append(frees, p)
		return p
	}

	var cg C.LoonColumnGroup
	cg.format = cstr(entry.Format)

	if len(entry.Columns) > 0 {
		cols := alloc(len(entry.Columns), unsafe.Sizeof((*C.char)(nil)))
		colSlice := unsafe.Slice((**C.char)(cols), len(entry.Columns))
		for i, name := range entry.Columns {
			colSlice[i] = cstr(name)
		}
		cg.columns = (**C.char)(cols)
	}
	cg.num_of_columns = C.uint32_t(len(entry.Columns))

	if len(entry.Files) > 0 {
		files := alloc(len(entry.Files), unsafe.Sizeof(C.LoonColumnGroupFile{}))
		fileSlice := unsafe.Slice((*C.LoonColumnGroupFile)(files), len(entry.Files))
		for i := range entry.Files {
			f := entry.Files[i]
			fileSlice[i].path = cstr(f.Path)
			fileSlice[i].start_index = C.int64_t(f.StartIndex)
			fileSlice[i].end_index = C.int64_t(f.EndIndex)
			if len(f.Properties) > 0 {
				keys := alloc(len(f.Properties), unsafe.Sizeof((*C.char)(nil)))
				vals := alloc(len(f.Properties), unsafe.Sizeof((*C.char)(nil)))
				keySlice := unsafe.Slice((**C.char)(keys), len(f.Properties))
				valSlice := unsafe.Slice((**C.char)(vals), len(f.Properties))
				pi := 0
				for k, v := range f.Properties {
					keySlice[pi] = cstr(k)
					valSlice[pi] = cstr(v)
					pi++
				}
				fileSlice[i].property_keys = (**C.char)(keys)
				fileSlice[i].property_values = (**C.char)(vals)
			}
			fileSlice[i].num_properties = C.uint32_t(len(f.Properties))
		}
		cg.files = (*C.LoonColumnGroupFile)(files)
	}
	cg.num_of_files = C.uint32_t(len(entry.Files))

	if err := HandleLoonFFIResult(C.loon_transaction_add_column_group(handle, &cg)); err != nil {
		return merr.WrapErrStorage(err, "commit manifest add_column_group")
	}
	return nil
}
