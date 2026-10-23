// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package flatbuf holds the FlatBuffers accessors the Materialize in-place
// RecordBatch reader and fused writer need: Message, RecordBatch, FieldNode,
// Buffer, BodyCompression and the enums they use. The files are trimmed
// copies of arrow/internal/flatbuf from github.com/milvus-io/arrow/go/v17
// v17.0.2 (generated from Apache Arrow's format/*.fbs), which Go's
// internal-package rule keeps us from importing: only the accessors (and the
// mutators tests use) are kept, and the functions that remain are verbatim.
// Do not edit them; to add one, copy it from the fork version go.mod
// replaces apache/arrow/go/v17 with.
package flatbuf
