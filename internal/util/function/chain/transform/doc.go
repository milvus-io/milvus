// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package transform defines Function Chain transformation contracts.
//
// These builders reuse the public FunctionChain protobuf. They do not register
// operators or route requests by themselves. The parent chain package provides
// the native Executor, and Proxy adapters attach it to INSERT/Search execution.
// PreInsert uses the existing Ingestion stage.
//
// In the v1 contract, ingestion may remove or expand rows. PreProcess and
// PostProcess in HITS mode preserve logical groups, row counts and row identity.
// POST TABLE additionally supports union_all and native or custom aggregate
// with independent output schemas and many-to-one provenance.
// A stage adapter must additionally bind input/output schemas, validate function
// availability and types, enforce resource limits, and validate executor output.
package transform
