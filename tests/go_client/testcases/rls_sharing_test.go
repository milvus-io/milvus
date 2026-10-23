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

package testcases

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/column"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/index"
	client "github.com/milvus-io/milvus/client/v3/milvusclient"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

type rlsSharingRow struct {
	ID            int64     `milvus:"name:id"`
	Owner         string    `milvus:"name:owner"`
	Tenant        string    `milvus:"name:tenant"`
	Content       string    `milvus:"name:content"`
	Active        bool      `milvus:"name:active"`
	ReadUsers     []string  `milvus:"name:read_users"`
	WriteUsers    []string  `milvus:"name:write_users"`
	ReadDepts     []string  `milvus:"name:read_departments"`
	WriteDepts    []string  `milvus:"name:write_departments"`
	ReadProjects  []string  `milvus:"name:read_projects"`
	WriteProjects []string  `milvus:"name:write_projects"`
	Vector        []float32 `milvus:"name:vector"`
}

func (r rlsSharingRow) fields() map[string]any {
	return map[string]any{
		"id": r.ID, "owner": r.Owner, "tenant": r.Tenant, "content": r.Content, "active": r.Active,
		"read_users": r.ReadUsers, "write_users": r.WriteUsers,
		"read_departments": r.ReadDepts, "write_departments": r.WriteDepts,
		"read_projects": r.ReadProjects, "write_projects": r.WriteProjects, "vector": r.Vector,
	}
}

func rlsSharingRows() map[int64]rlsSharingRow {
	rows := make(map[int64]rlsSharingRow, 9)
	for id := int64(1); id <= 9; id++ {
		rows[id] = rlsSharingRow{
			ID: id, Owner: "bob", Tenant: "tenant_a", Content: fmt.Sprintf("document_%d", id), Active: true,
			ReadUsers: []string{}, WriteUsers: []string{}, ReadDepts: []string{}, WriteDepts: []string{},
			ReadProjects: []string{}, WriteProjects: []string{}, Vector: []float32{float32(id), 1, 2, 3},
		}
	}
	for id, r := range rows {
		switch id {
		case 1:
			r.Owner = "alice"
		case 2:
			r.ReadUsers = []string{"alice"}
		case 3:
			r.ReadDepts = []string{"eng"}
		case 4:
			r.ReadProjects = []string{"alpha"}
		case 5:
			r.ReadUsers, r.WriteUsers, r.ReadProjects = []string{"alice"}, []string{"alice"}, []string{"beta"}
		case 6:
			r.ReadDepts, r.WriteDepts = []string{"eng"}, []string{"eng"}
			r.ReadProjects, r.WriteProjects = []string{"alpha", "beta"}, []string{"alpha", "beta"}
			r.Active = false
		case 7:
			r.ReadProjects, r.WriteProjects = []string{"gamma"}, []string{"gamma"}
		case 9:
			r.Tenant = "tenant_b"
			r.ReadUsers, r.WriteUsers = []string{"alice"}, []string{"alice"}
			r.ReadDepts, r.WriteDepts = []string{"eng"}, []string{"eng"}
			r.ReadProjects, r.WriteProjects = []string{"alpha", "beta"}, []string{"alpha", "beta"}
		}
		rows[id] = r
	}
	return rows
}

func rlsSharingColumns(rows ...rlsSharingRow) []column.Column {
	var ids []int64
	var owners, tenants, contents []string
	var active []bool
	var readUsers, writeUsers, readDepts, writeDepts, readProjects, writeProjects [][]string
	var vectors [][]float32
	for _, r := range rows {
		ids, owners, tenants, contents = append(ids, r.ID), append(owners, r.Owner), append(tenants, r.Tenant), append(contents, r.Content)
		active, vectors = append(active, r.Active), append(vectors, r.Vector)
		readUsers, writeUsers = append(readUsers, r.ReadUsers), append(writeUsers, r.WriteUsers)
		readDepts, writeDepts = append(readDepts, r.ReadDepts), append(writeDepts, r.WriteDepts)
		readProjects, writeProjects = append(readProjects, r.ReadProjects), append(writeProjects, r.WriteProjects)
	}
	return []column.Column{
		column.NewColumnInt64("id", ids), column.NewColumnVarChar("owner", owners), column.NewColumnVarChar("tenant", tenants),
		column.NewColumnVarChar("content", contents), column.NewColumnBool("active", active),
		column.NewColumnVarCharArray("read_users", readUsers), column.NewColumnVarCharArray("write_users", writeUsers),
		column.NewColumnVarCharArray("read_departments", readDepts), column.NewColumnVarCharArray("write_departments", writeDepts),
		column.NewColumnVarCharArray("read_projects", readProjects), column.NewColumnVarCharArray("write_projects", writeProjects),
		column.NewColumnFloatVector("vector", 4, vectors),
	}
}

func prepareRLSSharing(ctx context.Context, t *testing.T, mc *client.Client, rows map[int64]rlsSharingRow) string {
	t.Helper()
	name := common.GenRandomString("rls_sharing_", 8)
	schema := entity.NewSchema().WithName(name).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("active").WithDataType(entity.FieldTypeBool)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(4))
	for _, field := range []string{"owner", "tenant", "content"} {
		schema.WithField(entity.NewField().WithName(field).WithDataType(entity.FieldTypeVarChar).WithMaxLength(256))
	}
	for _, field := range []string{"read_users", "write_users", "read_departments", "write_departments", "read_projects", "write_projects"} {
		schema.WithField(entity.NewField().WithName(field).WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeVarChar).WithMaxCapacity(16).WithMaxLength(256))
	}
	common.CheckErr(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithProperty("rls.enabled", true).WithConsistencyLevel(entity.ClStrong)), true)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanup, client.NewDropCollectionOption(name)))
	})
	ordered := make([]rlsSharingRow, 0, len(rows))
	for _, id := range slices.Sorted(maps.Keys(rows)) {
		ordered = append(ordered, rows[id])
	}
	if len(ordered) > 0 {
		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(name, rlsSharingColumns(ordered...)...).WithSkipRLS(true))
		common.CheckErr(t, err, true)
	}
	idx, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewFlatIndex(entity.L2)))
	common.CheckErr(t, err, true)
	common.CheckErr(t, idx.Await(ctx), true)
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	common.CheckErr(t, err, true)
	common.CheckErr(t, load.Await(ctx), true)
	return name
}

func checkRLSSharingResult(t *testing.T, result client.ResultSet, rows map[int64]rlsSharingRow, expected []int64) {
	t.Helper()
	require.NoError(t, result.Err)
	ids := result.GetColumn("id")
	if ids == nil {
		ids = result.IDs
	}
	checkRLSIDs(t, ids, expected...)
	for i := range result.Len() {
		id, err := ids.GetAsInt64(i)
		require.NoError(t, err)
		r, ok := rows[id]
		require.True(t, ok, "unexpected row %d", id)
		for field, want := range r.fields() {
			col := result.GetColumn(field)
			require.NotNil(t, col, "missing field %s", field)
			got, err := col.Get(i)
			require.NoError(t, err)
			if values, ok := want.([]string); ok {
				actual, ok := got.([]string)
				require.True(t, ok, "id=%d field=%s must be a string array", id, field)
				// Non-nullable empty arrays may decode as a nil Go slice.
				require.True(t, slices.Equal(values, actual), "id=%d field=%s expected=%v actual=%v", id, field, values, actual)
			} else if values, ok := want.([]float32); ok {
				require.Equal(t, entity.FloatVector(values), got, "id=%d field=%s", id, field)
			} else {
				require.Equal(t, want, got, "id=%d field=%s", id, field)
			}
		}
	}
}

// Each read starts a fresh request or iterator after the completed metadata update.
func checkRLSSharingReads(ctx context.Context, t *testing.T, mc *client.Client, name, principal string, rows map[int64]rlsSharingRow, expected []int64) {
	t.Helper()
	for _, operation := range []string{"query", "filtered_query", "get", "count", "search", "search_by_id", "hybrid_search", "query_iterator", "search_iterator"} {
		t.Run(principal+"/"+operation, func(t *testing.T) {
			want := slices.Clone(expected)
			switch operation {
			case "query", "filtered_query", "get":
				opt := client.NewQueryOption(name).WithFilter("id >= 0 or owner == 'bob'").WithOutputFields("*").WithRLSPrincipal(principal).WithConsistencyLevel(entity.ClStrong)
				if operation == "filtered_query" {
					opt.WithFilter("id in [1,3,5,7,9]")
					want = slices.DeleteFunc(want, func(id int64) bool { return id%2 == 0 })
				}
				var result client.ResultSet
				var err error
				if operation == "get" {
					opt.WithIDs(column.NewColumnInt64("id", append(slices.Sorted(maps.Keys(rows)), 9999)))
					result, err = mc.Get(ctx, opt)
				} else {
					result, err = mc.Query(ctx, opt)
				}
				common.CheckErr(t, err, true)
				checkRLSSharingResult(t, result, rows, want)
			case "count":
				result, err := mc.Query(ctx, client.NewQueryOption(name).WithOutputFields("count(*)").WithRLSPrincipal(principal).WithConsistencyLevel(entity.ClStrong))
				common.CheckErr(t, err, true)
				count, err := result.GetColumn("count(*)").GetAsInt64(0)
				require.NoError(t, err)
				require.EqualValues(t, len(want), count)
			case "search_by_id":
				if len(want) > 0 {
					result, err := mc.Search(ctx, client.NewSearchByIDsOption(name, 100, column.NewColumnInt64("id", want[:1])).WithANNSField("vector").WithOutputFields("*").WithRLSPrincipal(principal).WithConsistencyLevel(entity.ClStrong))
					common.CheckErr(t, err, true)
					require.Len(t, result, 1)
					checkRLSSharingResult(t, result[0], rows, want)
				}
				hidden := int64(9999)
				for _, id := range slices.Sorted(maps.Keys(rows)) {
					if !slices.Contains(want, id) {
						hidden = id
						break
					}
				}
				_, err := mc.Search(ctx, client.NewSearchByIDsOption(name, 100, column.NewColumnInt64("id", []int64{hidden})).WithANNSField("vector").WithRLSPrincipal(principal).WithConsistencyLevel(entity.ClStrong))
				common.CheckErr(t, err, false, "do not exist")
			case "search", "hybrid_search":
				var result []client.ResultSet
				var err error
				if operation == "search" {
					result, err = mc.Search(ctx, client.NewSearchOption(name, 100, []entity.Vector{entity.FloatVector{0, 1, 2, 3}}).WithOutputFields("*").WithRLSPrincipal(principal).WithConsistencyLevel(entity.ClStrong))
				} else {
					result, err = mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 100,
						client.NewAnnRequest("vector", 100, entity.FloatVector{0, 1, 2, 3}).WithFilter("id in [7,8,9]"),
						client.NewAnnRequest("vector", 100, entity.FloatVector{0, 1, 2, 3})).WithReranker(client.NewRRFReranker()).WithOutputFields("*").WithRLSPrincipal(principal).WithConsistencyLevel(entity.ClStrong))
				}
				common.CheckErr(t, err, true)
				require.Len(t, result, 1)
				checkRLSSharingResult(t, result[0], rows, want)
				if operation == "search" {
					for i := range result[0].Len() {
						id, err := result[0].IDs.GetAsInt64(i)
						require.NoError(t, err)
						vector := rows[id].Vector
						distance := vector[0]*vector[0] + (vector[1]-1)*(vector[1]-1) + (vector[2]-2)*(vector[2]-2) + (vector[3]-3)*(vector[3]-3)
						require.Equal(t, distance, result[0].Scores[i])
					}
				}
			case "query_iterator", "search_iterator":
				var next func(context.Context) (client.ResultSet, error)
				if operation == "query_iterator" {
					it, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(name).WithBatchSize(2).WithOutputFields("*").WithRLSPrincipal(principal).WithConsistencyLevel(entity.ClStrong))
					common.CheckErr(t, err, true)
					next = it.Next
				} else {
					it, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(name, entity.FloatVector{0, 1, 2, 3}).WithBatchSize(2).WithOutputFields("*").WithRLSPrincipal(principal).WithConsistencyLevel(entity.ClStrong))
					common.CheckErr(t, err, true)
					next = it.Next
				}
				var got []int64
				eof := false
				for range len(rows) + 2 {
					result, err := next(ctx)
					if errors.Is(err, io.EOF) {
						eof = true
						break
					}
					common.CheckErr(t, err, true)
					ids := result.GetColumn("id")
					if ids == nil {
						ids = result.IDs
					}
					var batch []int64
					for i := range result.Len() {
						id, err := ids.GetAsInt64(i)
						require.NoError(t, err)
						require.Contains(t, want, id)
						batch = append(batch, id)
					}
					checkRLSSharingResult(t, result, rows, batch)
					got = append(got, batch...)
				}
				require.True(t, eof, "iterator must terminate")
				require.ElementsMatch(t, want, got, "no duplicates, missing rows, or hidden rows")
			}
		})
	}
}

func checkRLSSharingStored(ctx context.Context, t *testing.T, admin *client.Client, name string, rows map[int64]rlsSharingRow) {
	t.Helper()
	result, err := admin.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").WithOutputFields("*").WithSkipRLS(true).WithConsistencyLevel(entity.ClStrong))
	common.CheckErr(t, err, true)
	checkRLSSharingResult(t, result, rows, slices.Sorted(maps.Keys(rows)))
}

// Independent ID tables specify business outcomes instead of reimplementing policy evaluation.
func TestRLSSharingReadLifecycle(t *testing.T) {
	for _, test := range []struct {
		name       string
		exprs      []string
		initial    []int64
		reassigned []int64
		revoked    []int64
	}{
		{"owner", []string{"owner == $current_principal"}, []int64{1}, []int64{1}, []int64{1}},
		{"direct", []string{"array_contains(read_users, $current_principal)"}, []int64{2, 5, 9}, []int64{2, 5, 9}, []int64{2, 5, 9}},
		{"department", []string{"array_contains(read_departments, $current_principal_tags['department'])"}, []int64{3, 6, 9}, nil, nil},
		{"projects_any", []string{"array_contains_any(read_projects, $current_principal_tags['project_ids'])"}, []int64{4, 5, 6, 9}, []int64{7}, nil},
		{"projects_all", []string{"array_contains_all(read_projects, $current_principal_tags['project_ids'])"}, []int64{6, 9}, []int64{7}, nil},
		{"combined", []string{"owner == $current_principal", "array_contains(read_users, $current_principal)", "array_contains(read_departments, $current_principal_tags['department'])", "array_contains_any(read_projects, $current_principal_tags['project_ids'])"}, []int64{1, 2, 3, 4, 5, 6, 9}, []int64{1, 2, 5, 7, 9}, []int64{1, 2, 5, 9}},
		{"restricted", []string{"owner == $current_principal", "array_contains(read_users, $current_principal)", "array_contains(read_departments, $current_principal_tags['department'])", "array_contains_any(read_projects, $current_principal_tags['project_ids'])"}, []int64{1, 2, 3, 4, 5}, []int64{1, 2, 5, 7}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			admin := hp.CreateDefaultMilvusClient(ctx, t).Client
			rows := rlsSharingRows()
			name := prepareRLSSharing(ctx, t, admin, rows)
			reader := admin
			for i, expr := range test.exprs {
				createRLSPolicy(ctx, t, admin, name, fmt.Sprintf("read_%d", i), expr, "", rlsActions[:5]...)
			}
			if test.name == "restricted" {
				for policy, expr := range map[string]string{"tenant": "tenant == $current_principal_tags['tenant']", "active": "active == true"} {
					common.CheckErr(t, admin.CreateRowPolicy(ctx, client.NewCreateRowPolicyOption(name, policy, milvuspb.RowPolicyType_RowPolicyTypeRestrictive, rlsActions[:5]...).WithUsingExpr(expr)), true)
				}
			}
			for _, stage := range []struct {
				name string
				tags string
				want []int64
			}{
				{"initial", `{"department":"eng","project_ids":["alpha","beta"],"tenant":"tenant_a"}`, test.initial},
				{"membership_replaced", `{"department":"sales","project_ids":["gamma"]}`, test.reassigned},
				{"tags_deleted", "", test.revoked},
			} {
				t.Run(stage.name, func(t *testing.T) {
					if stage.tags == "" {
						common.CheckErr(t, admin.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "alice")), true)
					} else {
						common.CheckErr(t, admin.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", stage.tags)), true)
						stored, err := admin.GetRLSPrincipalTags(ctx, client.NewGetRLSPrincipalTagsOption(name, "alice"))
						common.CheckErr(t, err, true)
						var tags map[string]any
						require.NoError(t, json.Unmarshal([]byte(stored), &tags))
						require.Equal(t, "tenant_a", tags["tenant"], "unmentioned keys are preserved")
					}
					checkRLSSharingReads(ctx, t, reader, name, "alice", rows, stage.want)
					checkRLSSharingReads(ctx, t, reader, name, "outsider", rows, nil)
					checkRLSSharingStored(ctx, t, admin, name, rows)
				})
			}
			// Persistence is checked separately from the growing-row lifecycle.
			rlsFlush(ctx, t, admin, name)
			common.CheckErr(t, admin.ReleaseCollection(ctx, client.NewReleaseCollectionOption(name)), true)
			load, err := admin.LoadCollection(ctx, client.NewLoadCollectionOption(name))
			common.CheckErr(t, err, true)
			common.CheckErr(t, load.Await(ctx), true)
			t.Run("reload", func(t *testing.T) { checkRLSSharingReads(ctx, t, reader, name, "alice", rows, test.revoked) })
		})
	}
}

// Exercise the three SDK serializers without changing the expected stored row model.
func rlsSharingWrite(ctx context.Context, t *testing.T, mc *client.Client, name, principal, mode, operation string, rows ...rlsSharingRow) error {
	t.Helper()
	var option interface {
		client.InsertOption
		client.UpsertOption
	}
	partial := operation == "partial_upsert" || operation == "partial_content"
	if mode == "column" {
		cols := rlsSharingColumns(rows...)
		if operation == "partial_content" {
			cols = []column.Column{cols[0], cols[3]}
		}
		option = client.NewColumnBasedInsertOption(name, cols...).WithRLSPrincipal(principal).WithPartialUpdate(partial)
	} else {
		values := make([]any, len(rows))
		for i, row := range rows {
			if mode == "row_map" {
				values[i] = row.fields()
				if operation == "partial_content" {
					values[i] = map[string]any{"id": row.ID, "content": row.Content}
				}
			} else {
				values[i] = row
				if operation == "partial_content" {
					values[i] = struct {
						ID      int64  `milvus:"name:id"`
						Content string `milvus:"name:content"`
					}{row.ID, row.Content}
				}
			}
		}
		option = client.NewRowBasedInsertOption(name, values...).WithRLSPrincipal(principal).WithPartialUpdate(partial)
	}
	if operation == "insert" {
		result, err := mc.Insert(ctx, option)
		if err == nil {
			require.EqualValues(t, len(rows), result.InsertCount)
			checkRLSIDs(t, result.IDs, sharingRowIDs(rows)...)
		}
		return err
	}
	result, err := mc.Upsert(ctx, option)
	if err == nil {
		require.EqualValues(t, len(rows), result.UpsertCount)
		checkRLSIDs(t, result.IDs, sharingRowIDs(rows)...)
	}
	return err
}

func sharingRowIDs(rows []rlsSharingRow) []int64 {
	ids := make([]int64, len(rows))
	for i, r := range rows {
		ids[i] = r.ID
	}
	return ids
}

func TestRLSSharingWritePermissions(t *testing.T) {
	for _, grant := range []struct {
		name, expr string
		editor     int64
		reader     int64
	}{
		{"direct", "array_contains(write_users, $current_principal)", 5, 2},
		{"department", "array_contains(write_departments, $current_principal_tags['department'])", 6, 3},
		{"projects", "array_contains_any(write_projects, $current_principal_tags['project_ids'])", 6, 4},
	} {
		for _, mode := range []string{"column", "row_map", "row_struct"} {
			t.Run(grant.name+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
				defer cancel()
				admin := hp.CreateDefaultMilvusClient(ctx, t).Client
				rows := rlsSharingRows()
				name := prepareRLSSharing(ctx, t, admin, rows)
				app := admin
				createRLSPolicy(ctx, t, admin, name, "owner", "owner == $current_principal", "owner == $current_principal", rlsActions...)
				createRLSPolicy(ctx, t, admin, name, "editor", grant.expr, grant.expr, milvuspb.RowPolicyAction_Upsert)
				for i, expr := range []string{"array_contains(read_users, $current_principal)", "array_contains(read_departments, $current_principal_tags['department'])", "array_contains_any(read_projects, $current_principal_tags['project_ids'])"} {
					createRLSPolicy(ctx, t, admin, name, fmt.Sprintf("reader_%d", i), expr, "", rlsActions[:5]...)
				}
				common.CheckErr(t, admin.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":"eng","project_ids":["alpha","beta"]}`)), true)
				t.Run("read_does_not_grant_write", func(t *testing.T) {
					checkRLSSharingReads(ctx, t, app, name, "alice", rows, []int64{1, 2, 3, 4, 5, 6, 9})
				})
				for _, operation := range []string{"upsert", "partial_upsert", "partial_content"} {
					t.Run(operation, func(t *testing.T) {
						for _, scenario := range []string{"editor_allowed", "reader_denied", "hidden_old_row", "mixed_batch", "check_denied", "owner_transfer_denied"} {
							if scenario == "check_denied" && operation == "partial_content" || scenario == "owner_transfer_denied" && operation == "partial_content" {
								continue // Omitted ACL fields are exercised by editor_allowed.
							}
							t.Run(scenario, func(t *testing.T) {
								row := rows[grant.editor]
								row.Content = mode + "_" + operation + "_" + scenario
								input := []rlsSharingRow{row}
								switch scenario {
								case "reader_denied":
									row = rows[grant.reader]
									row.Content = "unauthorized"
									input = []rlsSharingRow{row}
								case "hidden_old_row":
									row.ID, row.Owner = 8, "alice"
									input = []rlsSharingRow{row}
								case "mixed_batch":
									bad := rows[grant.reader]
									bad.Content = "unauthorized"
									input = append(input, bad)
								case "check_denied":
									row.WriteUsers, row.WriteDepts, row.WriteProjects = []string{}, []string{}, []string{}
									input = []rlsSharingRow{row}
								case "owner_transfer_denied":
									row = rows[1]
									row.Owner = "bob"
									input = []rlsSharingRow{row}
								}
								err := rlsSharingWrite(ctx, t, app, name, "alice", mode, operation, input...)
								if scenario == "editor_allowed" {
									common.CheckErr(t, err, true)
									rows[row.ID] = row
								} else {
									common.CheckErr(t, err, false, "RLS")
								}
								checkRLSSharingStored(ctx, t, admin, name, rows)
							})
						}
						checkRLSSharingReads(ctx, t, app, name, "alice", rows, []int64{1, 2, 3, 4, 5, 6, 9})
					})
				}
				for _, operation := range []string{"insert", "upsert", "partial_upsert"} {
					t.Run(operation+"_new_rows", func(t *testing.T) {
						row := rows[1]
						row.ID = int64(100 + len(rows))
						row.Content = "new_owned_document"
						common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", mode, operation, row), true)
						rows[row.ID] = row
						good, bad := row, row
						good.ID, bad.ID, bad.Owner = row.ID+100, row.ID+101, "bob"
						common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", mode, operation, good, bad), false, "RLS")
						checkRLSSharingStored(ctx, t, admin, name, rows)
					})
				}
				t.Run("owner_is_not_auto_filled", func(t *testing.T) {
					row := rows[1]
					row.ID, row.Owner = 999, ""
					common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", mode, "insert", row), false, "RLS")
					cols := rlsSharingColumns(row)
					cols = slices.Delete(cols, 1, 2)
					_, err := app.Insert(ctx, client.NewColumnBasedInsertOption(name, cols...).WithRLSPrincipal("alice"))
					require.Error(t, err, "required owner is application data, not a server default")
					checkRLSSharingStored(ctx, t, admin, name, rows)
				})
				if grant.name != "direct" {
					t.Run("membership_revokes_edit", func(t *testing.T) {
						common.CheckErr(t, admin.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "alice")), true)
						row := rows[grant.editor]
						row.Content = "revoked_member"
						common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", mode, "partial_content", row), false, "RLS")
						checkRLSSharingStored(ctx, t, admin, name, rows)
						common.CheckErr(t, admin.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":"eng","project_ids":["alpha","beta"]}`)), true)
						common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", mode, "partial_content", row), true)
						rows[row.ID] = row
						checkRLSSharingStored(ctx, t, admin, name, rows)
					})
				}
				for _, operation := range []string{"delete_ids", "delete_filter"} {
					t.Run(operation, func(t *testing.T) {
						if operation == "delete_filter" {
							row := rlsSharingRows()[1]
							row.ID = 900
							common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", mode, "insert", row), true)
							rows[row.ID] = row
						}
						opt := client.NewDeleteOption(name).WithRLSPrincipal("alice").WithExpr("id >= 0")
						if operation == "delete_ids" {
							opt.WithInt64IDs("id", slices.Sorted(maps.Keys(rows)))
						}
						_, err := app.Delete(ctx, opt)
						common.CheckErr(t, err, true)
						// Read and edit sharing do not imply delete permission.
						maps.DeleteFunc(rows, func(_ int64, row rlsSharingRow) bool { return row.Owner == "alice" })
						checkRLSSharingStored(ctx, t, admin, name, rows)
					})
				}
				t.Run("editor_revoked", func(t *testing.T) {
					row := rows[grant.editor]
					row.WriteUsers, row.WriteDepts, row.WriteProjects = []string{}, []string{}, []string{}
					common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "bob", mode, "upsert", row), true)
					rows[row.ID] = row
					row.Content = "must_not_commit"
					common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", mode, "partial_content", row), false, "RLS")
					checkRLSSharingStored(ctx, t, admin, name, rows)
				})
				t.Run("explicit_delete_grant", func(t *testing.T) {
					common.CheckErr(t, admin.UpdateRowPolicy(ctx, client.NewUpdateRowPolicyOption(name, "editor", milvuspb.RowPolicyType_RowPolicyTypePermissive, milvuspb.RowPolicyAction_Upsert, milvuspb.RowPolicyAction_Delete).WithUsingExpr(grant.expr).WithCheckExpr(grant.expr)), true)
					for _, byIDs := range []bool{true, false} {
						row := rlsSharingRows()[9]
						common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "bob", mode, "upsert", row), true)
						rows[9] = row
						opt := client.NewDeleteOption(name).WithRLSPrincipal("alice").WithExpr("id >= 0")
						if byIDs {
							opt.WithInt64IDs("id", []int64{8, 9})
						}
						_, err := app.Delete(ctx, opt)
						common.CheckErr(t, err, true)
						delete(rows, 9)
						checkRLSSharingStored(ctx, t, admin, name, rows)
					}
				})
			})
		}
	}
}

// RLS checks row predicates; it does not implicitly make owner or ACL columns immutable.
func TestRLSSharingEditorPolicyScope(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	admin := hp.CreateDefaultMilvusClient(ctx, t).Client
	rows := rlsSharingRows()
	name := prepareRLSSharing(ctx, t, admin, rows)
	app := admin
	createRLSPolicy(ctx, t, admin, name, "owner", "owner == $current_principal", "owner == $current_principal", append(slices.Clone(rlsActions[:5]), milvuspb.RowPolicyAction_Upsert)...)
	createRLSPolicy(ctx, t, admin, name, "read", "array_contains(read_users, $current_principal)", "", rlsActions[:5]...)
	createRLSPolicy(ctx, t, admin, name, "edit", "array_contains(write_users, $current_principal)", "array_contains(write_users, $current_principal)", milvuspb.RowPolicyAction_Upsert)
	row := rows[5]
	row.Owner, row.ReadUsers = "charlie", []string{"alice", "charlie"}
	common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", "column", "upsert", row), true)
	rows[5] = row
	checkRLSSharingStored(ctx, t, admin, name, rows)
	checkRLSSharingReads(ctx, t, app, name, "charlie", rows, []int64{5})
	// A new PK has no old-row USING check, so the shared-editor CHECK also admits it.
	row.ID, row.Vector = 10, []float32{10, 1, 2, 3}
	common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", "row_map", "upsert", row), true)
	rows[10] = row
	checkRLSSharingStored(ctx, t, admin, name, rows)
	checkRLSSharingReads(ctx, t, app, name, "charlie", rows, []int64{5, 10})
	// Restricting CHECK to ownership changes the contract and blocks Alice's shared edit.
	common.CheckErr(t, admin.CreateRowPolicy(ctx, client.NewCreateRowPolicyOption(name, "owner_check", milvuspb.RowPolicyType_RowPolicyTypeRestrictive, milvuspb.RowPolicyAction_Upsert).WithUsingExpr("true").WithCheckExpr("owner == $current_principal")), true)
	row.Content = "blocked_by_restrictive_check"
	common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", "column", "partial_content", row), false, "RLS")
	checkRLSSharingStored(ctx, t, admin, name, rows)
}

func TestRLSSharingRestrictiveWrites(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Minute)
	defer cancel()
	admin := hp.CreateDefaultMilvusClient(ctx, t).Client
	rows := rlsSharingRows()
	name := prepareRLSSharing(ctx, t, admin, rows)
	app := admin
	createRLSPolicy(ctx, t, admin, name, "shared", "array_contains(write_users, $current_principal)", "array_contains(write_users, $current_principal)", milvuspb.RowPolicyAction_Insert, milvuspb.RowPolicyAction_Upsert, milvuspb.RowPolicyAction_Delete)
	common.CheckErr(t, admin.CreateRowPolicy(ctx, client.NewCreateRowPolicyOption(name, "tenant", milvuspb.RowPolicyType_RowPolicyTypeRestrictive, milvuspb.RowPolicyAction_Insert, milvuspb.RowPolicyAction_Upsert, milvuspb.RowPolicyAction_Delete).WithUsingExpr("tenant == $current_principal_tags['tenant']").WithCheckExpr("tenant == $current_principal_tags['tenant']")), true)
	common.CheckErr(t, admin.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"tenant":"tenant_a"}`)), true)
	for _, mode := range []string{"column", "row_map", "row_struct"} {
		for _, operation := range []string{"insert", "upsert", "partial_upsert"} {
			t.Run(mode+"/"+operation, func(t *testing.T) {
				row := rows[5]
				row.ID, row.Vector = int64(100+len(rows)), []float32{0, 1, 2, 3}
				common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", mode, operation, row), true)
				rows[row.ID] = row
				bad := row
				bad.ID, bad.Tenant = row.ID+100, "tenant_b"
				common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", mode, operation, bad), false, "RLS")
				if operation != "insert" {
					bad = rows[9]
					bad.Tenant = "tenant_a"
					common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", mode, operation, bad), false, "RLS")
					bad = rows[5]
					bad.Tenant = "tenant_b"
					common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", mode, operation, bad), false, "RLS")
				}
				checkRLSSharingStored(ctx, t, admin, name, rows)
			})
		}
	}
	common.CheckErr(t, admin.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "alice", "tenant")), true)
	t.Run("missing_restrictive_tag", func(t *testing.T) {
		row := rows[5]
		row.Content = "must_not_commit"
		common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "alice", "column", "partial_content", row), false, "RLS")
		_, err := app.Delete(ctx, client.NewDeleteOption(name).WithExpr("id >= 0").WithRLSPrincipal("alice"))
		common.CheckErr(t, err, true)
		checkRLSSharingStored(ctx, t, admin, name, rows)
	})
	common.CheckErr(t, admin.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"tenant":"tenant_a"}`)), true)
	_, err := app.Delete(ctx, client.NewDeleteOption(name).WithExpr("id >= 0").WithRLSPrincipal("alice"))
	common.CheckErr(t, err, true)
	delete(rows, 5)
	maps.DeleteFunc(rows, func(id int64, _ rlsSharingRow) bool { return id >= 100 })
	checkRLSSharingStored(ctx, t, admin, name, rows)
}

// Revoking one permissive route leaves the other routes effective.
func TestRLSSharingGrantAndRevoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	admin := hp.CreateDefaultMilvusClient(ctx, t).Client
	identity, err := admin.GetService().Connect(ctx, &milvuspb.ConnectRequest{ClientInfo: &commonpb.ClientInfo{SdkType: "Go", SdkVersion: "RLS sharing E2E"}})
	common.CheckErr(t, err, true)
	require.Zero(t, identity.GetStatus().GetCode())
	require.Equal(t, commonpb.ErrorCode_Success, identity.GetStatus().GetErrorCode())
	require.NotEmpty(t, identity.GetServerInfo().GetGitCommit())
	t.Logf("server identity: %s", identity.GetServerInfo())
	rows := rlsSharingRows()
	name := prepareRLSSharing(ctx, t, admin, rows)
	app := admin
	createRLSPolicy(ctx, t, admin, name, "owner", "owner == $current_principal", "owner == $current_principal", append(slices.Clone(rlsActions[:5]), milvuspb.RowPolicyAction_Upsert)...)
	for policy, expr := range map[string]string{"direct": "array_contains(read_users, $current_principal)", "projects": "array_contains_any(read_projects, $current_principal_tags['project_ids'])"} {
		createRLSPolicy(ctx, t, admin, name, policy, expr, "", rlsActions[:5]...)
	}
	for _, stage := range []struct {
		name string
		want []int64
	}{
		{"initial_direct", []int64{1, 2, 5, 9}},
		{"grant_project", []int64{1, 2, 4, 5, 6, 9}},
		{"revoke_direct_keep_project", []int64{1, 4, 5, 6, 9}},
		{"revoke_project_keep_owner", []int64{1}},
		{"grant_direct_again", []int64{1, 8}},
		{"revoke_last_direct", []int64{1}},
	} {
		t.Run(stage.name, func(t *testing.T) {
			switch stage.name {
			case "grant_project":
				common.CheckErr(t, admin.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"project_ids":["alpha","beta"]}`)), true)
			case "revoke_direct_keep_project":
				for _, id := range []int64{2, 5, 9} {
					row := rows[id]
					row.ReadUsers = []string{}
					common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "bob", "column", "partial_upsert", row), true)
					rows[id] = row
				}
			case "revoke_project_keep_owner":
				common.CheckErr(t, admin.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "alice", "project_ids")), true)
			case "grant_direct_again", "revoke_last_direct":
				row := rows[8]
				row.ReadUsers = []string{}
				if stage.name == "grant_direct_again" {
					row.ReadUsers = []string{"alice"}
				}
				common.CheckErr(t, rlsSharingWrite(ctx, t, app, name, "bob", "row_map", "upsert", row), true)
				rows[8] = row
			}
			checkRLSSharingReads(ctx, t, app, name, "alice", rows, stage.want)
			checkRLSSharingStored(ctx, t, admin, name, rows)
		})
	}
}

func TestRLSSharingArrayTagBoundaries(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	admin := hp.CreateDefaultMilvusClient(ctx, t).Client
	rows := rlsSharingRows()
	row := rows[8]
	row.ReadProjects = []string{"项目甲", `x' or true`, ""}
	rows[8] = row
	name := prepareRLSSharing(ctx, t, admin, rows)
	reader := admin
	for _, operation := range []string{"any", "all", "not_any"} {
		expr := "array_contains_any(read_projects, $current_principal_tags['project_ids'])"
		if operation == "all" {
			expr = "array_contains_all(read_projects, $current_principal_tags['project_ids'])"
		}
		if operation == "not_any" {
			expr = "not " + expr
		}
		createRLSPolicy(ctx, t, admin, name, operation, expr, "", rlsActions[:5]...)
		createRLSPolicy(ctx, t, admin, name, operation+"_write", expr, expr, milvuspb.RowPolicyAction_Insert, milvuspb.RowPolicyAction_Upsert)
		writer := admin
		for _, test := range []struct {
			name, tags string
			anyIDs     []int64
			allIDs     []int64
			notIDs     []int64
		}{
			{"multiple", `{"project_ids":["alpha","beta"]}`, []int64{4, 5, 6, 9}, []int64{6, 9}, []int64{1, 2, 3, 7, 8}},
			{"duplicates", `{"project_ids":["alpha","alpha"]}`, []int64{4, 6, 9}, []int64{4, 6, 9}, []int64{1, 2, 3, 5, 7, 8}},
			{"empty", `{"project_ids":[]}`, nil, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}},
			{"scalar_mismatch", `{"project_ids":"alpha"}`, nil, nil, nil},
			{"numeric_mismatch", `{"project_ids":[1,2]}`, nil, nil, nil},
			{"unicode", `{"project_ids":["项目甲"]}`, []int64{8}, []int64{8}, []int64{1, 2, 3, 4, 5, 6, 7, 9}},
			{"quoted_literal", `{"project_ids":["x' or true"]}`, []int64{8}, []int64{8}, []int64{1, 2, 3, 4, 5, 6, 7, 9}},
			{"empty_string", `{"project_ids":[""]}`, []int64{8}, []int64{8}, []int64{1, 2, 3, 4, 5, 6, 7, 9}},
			{"case_sensitive", `{"project_ids":["ALPHA"]}`, nil, nil, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}},
			{"missing", "", nil, nil, nil},
		} {
			t.Run(operation+"/"+test.name, func(t *testing.T) {
				if test.tags == "" {
					common.CheckErr(t, admin.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "alice")), true)
				} else {
					common.CheckErr(t, admin.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", test.tags)), true)
					stored, err := admin.GetRLSPrincipalTags(ctx, client.NewGetRLSPrincipalTagsOption(name, "alice"))
					common.CheckErr(t, err, true)
					require.JSONEq(t, test.tags, stored)
				}
				want := test.anyIDs
				if operation == "all" {
					want = test.allIDs
				}
				if operation == "not_any" {
					want = test.notIDs
				}
				checkRLSSharingReads(ctx, t, reader, name, "alice", rows, want)
				// Probe empty, single, multi-value and quoted/Unicode row ACLs on CHECK as well.
				t.Cleanup(func() {
					_, err := admin.Delete(ctx, client.NewDeleteOption(name).WithExpr("id >= 100").WithSkipRLS(true))
					require.NoError(t, err)
					maps.DeleteFunc(rows, func(id int64, _ rlsSharingRow) bool { return id >= 100 })
				})
				for _, id := range []int64{1, 4, 6, 8} {
					for _, write := range []string{"insert", "upsert", "partial_upsert"} {
						t.Run(fmt.Sprintf("%s/row_%d", write, id), func(t *testing.T) {
							row := rows[id]
							row.ID, row.Content = id+100, operation+"_"+write
							err := rlsSharingWrite(ctx, t, writer, name, "alice", "column", write, row)
							if slices.Contains(want, id) {
								common.CheckErr(t, err, true)
								rows[row.ID] = row
							} else {
								common.CheckErr(t, err, false, "RLS")
							}
							checkRLSSharingStored(ctx, t, admin, name, rows)
						})
					}
				}
			})
		}
		common.CheckErr(t, admin.DropRowPolicy(ctx, client.NewDropRowPolicyOption(name, operation)), true)
		common.CheckErr(t, admin.DropRowPolicy(ctx, client.NewDropRowPolicyOption(name, operation+"_write")), true)
	}
	createRLSPolicy(ctx, t, admin, name, "read", "array_contains_any(read_projects, $current_principal_tags['project_ids'])", "", rlsActions[:5]...)
	common.CheckErr(t, admin.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"project_ids":["alpha"]}`)), true)
	for _, invalid := range []struct{ name, tags string }{
		{"mixed_types", `{"project_ids":["alpha",1]}`},
		{"nested", `{"project_ids":[["alpha"]]}`},
		{"null", `{"project_ids":null}`},
		{"null_element", `{"project_ids":[null]}`},
		{"bool", `{"project_ids":[true]}`},
		{"object", `{"project_ids":{"alpha":true}}`},
	} {
		t.Run("invalid/"+invalid.name, func(t *testing.T) {
			require.Error(t, admin.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", invalid.tags)))
			stored, err := admin.GetRLSPrincipalTags(ctx, client.NewGetRLSPrincipalTagsOption(name, "alice"))
			common.CheckErr(t, err, true)
			require.JSONEq(t, `{"project_ids":["alpha"]}`, stored)
			checkRLSSharingReads(ctx, t, reader, name, "alice", rows, []int64{4, 6, 9})
		})
	}
}

// ACL array operators must validate both the old row and the merged post-image.
func TestRLSSharingACLPartialOperators(t *testing.T) {
	for _, acl := range []struct{ name, field, member, other, expr string }{
		{"direct", "read_users", "alice", "charlie", "array_contains(read_users, $current_principal)"},
		{"department", "read_departments", "eng", "sales", "array_contains(read_departments, $current_principal_tags['department'])"},
		{"projects", "read_projects", "alpha", "gamma", "array_contains_any(read_projects, $current_principal_tags['project_ids'])"},
	} {
		for _, mode := range []string{"column", "row_map", "row_struct"} {
			t.Run(acl.name+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
				defer cancel()
				admin := hp.CreateDefaultMilvusClient(ctx, t).Client
				fixture := rlsSharingRows()
				rows := map[int64]rlsSharingRow{1: fixture[1], 8: fixture[8]}
				name := prepareRLSSharing(ctx, t, admin, rows)
				app := admin
				actions := append(slices.Clone(rlsActions[:5]), milvuspb.RowPolicyAction_Upsert)
				createRLSPolicy(ctx, t, admin, name, "owner", "owner == $current_principal", "owner == $current_principal", actions...)
				createRLSPolicy(ctx, t, admin, name, "shared", acl.expr, acl.expr, actions...)
				common.CheckErr(t, admin.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "alice", `{"department":"eng","project_ids":["alpha","beta"]}`)), true)
				mutate := func(principal, operation, value string) error {
					var option client.UpsertOption
					if mode == "column" {
						opt := client.NewColumnBasedInsertOption(name, column.NewColumnInt64("id", []int64{8}), column.NewColumnVarCharArray(acl.field, [][]string{{value}})).WithRLSPrincipal(principal)
						switch operation {
						case "append":
							opt.WithArrayAppend(acl.field)
						case "remove":
							opt.WithArrayRemove(acl.field)
						case "path_replace":
							opt.WithPathReplace(acl.field, "[0]")
						}
						option = opt
					} else {
						var row any = map[string]any{"id": int64(8), acl.field: []string{value}}
						if mode == "row_struct" {
							row = rlsSchemaStructInput(rlsSchemaRow{"id": int64(8), acl.field: []string{value}})
						}
						opt := client.NewRowBasedInsertOption(name, row).WithRLSPrincipal(principal)
						switch operation {
						case "append":
							opt.WithArrayAppend(acl.field)
						case "remove":
							opt.WithArrayRemove(acl.field)
						case "path_replace":
							opt.WithPathReplace(acl.field, "[0]")
						}
						option = opt
					}
					_, err := app.Upsert(ctx, option)
					return err
				}
				common.CheckErr(t, mutate("alice", "append", acl.member), false, "RLS")
				checkRLSSharingStored(ctx, t, admin, name, rows)
				for _, stage := range []struct {
					name, operation, operand string
					stored                   []string
					visible                  []int64
				}{
					{"grant", "append", acl.member, []string{acl.member}, []int64{1, 8}},
					{"replace_revokes", "path_replace", acl.other, []string{acl.other}, []int64{1}},
					{"grant_again", "append", acl.member, []string{acl.other, acl.member}, []int64{1, 8}},
					{"remove_revokes", "remove", acl.member, []string{acl.other}, []int64{1}},
				} {
					t.Run(stage.name, func(t *testing.T) {
						if stage.operation != "append" {
							common.CheckErr(t, mutate("alice", stage.operation, stage.operand), false, "RLS")
							checkRLSSharingStored(ctx, t, admin, name, rows)
						}
						common.CheckErr(t, mutate("bob", stage.operation, stage.operand), true)
						row := rows[8]
						switch acl.field {
						case "read_users":
							row.ReadUsers = stage.stored
						case "read_departments":
							row.ReadDepts = stage.stored
						case "read_projects":
							row.ReadProjects = stage.stored
						}
						rows[8] = row
						checkRLSSharingStored(ctx, t, admin, name, rows)
						checkRLSSharingReads(ctx, t, app, name, "alice", rows, stage.visible)
					})
				}
			})
		}
	}
}

// These synthetic resources model tenant-scoped spaces without application identifiers.
type rlsSpaceRow struct {
	NullFields []string  `milvus:"-"`
	ID         int64     `milvus:"name:id"`
	Tenant     string    `milvus:"name:tenant"`
	Spaces     []string  `milvus:"name:spaces"`
	Resource   string    `milvus:"name:resource"`
	Access     []string  `milvus:"name:access"`
	Content    string    `milvus:"name:content"`
	Vector     []float32 `milvus:"name:vector"`
}

func configureRLSSpaces(ctx context.Context, t *testing.T, mc *client.Client, name string) {
	t.Helper()
	wall := "tenant == $current_principal_tags['tenant']"
	write := "array_contains_any(spaces, $current_principal_tags['write_spaces'])"
	common.CheckErr(t, mc.CreateRowPolicy(ctx, client.NewCreateRowPolicyOption(name, "tenant_wall", milvuspb.RowPolicyType_RowPolicyTypeRestrictive, rlsActions...).WithUsingExpr(wall).WithCheckExpr(wall)), true)
	createRLSPolicy(ctx, t, mc, name, "space_writer", write, write, rlsActions...)
	for policy, expr := range map[string]string{
		"space_reader":  "array_contains_any(spaces, $current_principal_tags['read_spaces'])",
		"direct_reader": "array_contains(access, $current_principal)",
		"group_reader":  "array_contains_any(access, $current_principal_tags['groups'])",
	} {
		createRLSPolicy(ctx, t, mc, name, policy, expr, "", rlsActions[:5]...)
	}
	for principal, tags := range map[string]string{
		"member_0": `{"tenant":"tenant_0","read_spaces":["tenant_0/common"],"write_spaces":["tenant_0/red"]}`,
		"member_1": `{"tenant":"tenant_0","read_spaces":["tenant_0/common"],"write_spaces":["tenant_0/blue"]}`,
		"member_2": `{"tenant":"tenant_0","read_spaces":["tenant_0/common"],"write_spaces":["tenant_0/red","tenant_0/blue"]}`,
		"member_3": `{"tenant":"tenant_1","read_spaces":[],"write_spaces":["tenant_1/red"]}`,
		"visitor":  `{"tenant":"tenant_0","read_spaces":["tenant_0/common","tenant_0/red"],"write_spaces":[]}`,
		"personal": `{"tenant":"tenant_2","read_spaces":[],"write_spaces":["tenant_2/personal"]}`,
	} {
		common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, principal, tags)), true)
	}
}

func prepareRLSSpaces(ctx context.Context, t *testing.T, mc *client.Client, enabled, legacy bool) (string, map[int64]rlsSpaceRow) {
	t.Helper()
	// Default CI uses a 1024-row index threshold; verify segment state through public APIs below.
	name := common.GenRandomString("rls_spaces_", 8)
	schema := entity.NewSchema().WithName(name).
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(128))
	for _, field := range []string{"resource", "content"} {
		schema.WithField(entity.NewField().WithName(field).WithDataType(entity.FieldTypeVarChar).WithMaxLength(128))
	}
	permissionFields := []*entity.Field{entity.NewField().WithName("tenant").WithDataType(entity.FieldTypeVarChar).WithMaxLength(128)}
	for field, capacity := range map[string]int64{"spaces": 1, "access": 16} {
		permissionFields = append(permissionFields, entity.NewField().WithName(field).WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeVarChar).WithMaxLength(128).WithMaxCapacity(capacity))
	}
	if !legacy {
		for _, field := range permissionFields {
			schema.WithField(field)
		}
	}
	common.CheckErr(t, mc.CreateCollection(ctx, client.NewCreateCollectionOption(name, schema).WithShardNum(1).WithConsistencyLevel(entity.ClStrong).WithProperty("rls.enabled", enabled).WithProperty("collection.autocompaction.enabled", false)), true)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, mc.DropCollection(cleanup, client.NewDropCollectionOption(name)))
	})
	locations := [][2]string{{"tenant_0", "common"}, {"tenant_0", "red"}, {"tenant_0", "blue"}, {"tenant_1", "red"}, {"tenant_2", "personal"}}
	vectors := hp.GenColumnData(4000, entity.FieldTypeFloatVector, *hp.TNewDataOption().TWithDim(128).TWithFieldName("vector"))
	rows := make(map[int64]rlsSpaceRow, 4000)
	seen := map[string]bool{}
	for i := range 4000 {
		v, err := vectors.Get(i)
		require.NoError(t, err)
		vector := rlsQueryVector("float32", v).(entity.FloatVector)
		encoded := string(vector.Serialize())
		require.False(t, seen[encoded], "generated vectors must be distinct")
		seen[encoded] = true
		location := locations[i%5]
		id := int64(i + 1)
		rows[id] = rlsSpaceRow{ID: id, Tenant: location[0], Spaces: []string{location[0] + "/" + location[1]}, Resource: fmt.Sprintf("resource_%d", i%5), Access: []string{}, Content: fmt.Sprintf("chunk_%d", i), Vector: []float32(vector)}
	}
	for _, batch := range []struct{ start, end int64 }{{1, 3001}, {3001, 3501}, {3501, 4001}} {
		input := make([]any, 0, batch.end-batch.start)
		for id := batch.start; id < batch.end; id++ {
			if legacy {
				row := rows[id]
				input = append(input, map[string]any{"id": id, "resource": row.Resource, "content": row.Content, "vector": row.Vector})
			} else {
				input = append(input, rows[id])
			}
		}
		_, err := mc.Insert(ctx, client.NewRowBasedInsertOption(name, input...).WithSkipRLS(true))
		common.CheckErr(t, err, true)
		if batch.start < 3501 {
			rlsFlush(ctx, t, mc, name)
		}
		if batch.start == 3001 {
			idx, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(name, "vector", index.NewIvfFlatIndex(entity.L2, 64)))
			common.CheckErr(t, err, true)
			common.CheckErr(t, idx.Await(ctx), true)
			load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
			common.CheckErr(t, err, true)
			common.CheckErr(t, load.Await(ctx), true)
		}
	}
	checkRLSMixedSegments(ctx, t, mc, name)
	if legacy {
		before, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(name))
		common.CheckErr(t, err, true)
		for _, field := range permissionFields {
			require.NotContains(t, mapFieldsByName(before.Schema.Fields), field.Name)
			common.CheckErr(t, mc.AddCollectionField(ctx, client.NewAddCollectionFieldOption(name, field.WithNullable(true))), true)
		}
		common.CheckErr(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(name)), true)
		load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
		common.CheckErr(t, err, true)
		common.CheckErr(t, load.Await(ctx), true)
		after, err := mc.DescribeCollection(ctx, client.NewDescribeCollectionOption(name))
		common.CheckErr(t, err, true)
		for _, field := range permissionFields {
			require.True(t, mapFieldsByName(after.Schema.Fields)[field.Name].Nullable)
		}
		for id, row := range rows {
			row.NullFields = []string{"tenant", "spaces", "access"}
			rows[id] = row
		}
		// Schema changes flush growing segments; all original rows are now sealed.
		segments, err := mc.GetService().GetQuerySegmentInfo(ctx, &milvuspb.GetQuerySegmentInfoRequest{CollectionName: name})
		common.CheckErr(t, err, true)
		require.Zero(t, segments.GetStatus().GetCode())
		var counts []int64
		for _, segment := range segments.GetInfos() {
			require.Equal(t, commonpb.SegmentState_Sealed, segment.GetState())
			counts = append(counts, segment.GetNumRows())
		}
		require.ElementsMatch(t, []int64{3000, 500, 500}, counts)
	}
	checkRLSSpaceStored(ctx, t, mc, name, rows)
	sealed := maps.Clone(rows)
	if !legacy {
		maps.DeleteFunc(sealed, func(id int64, _ rlsSpaceRow) bool { return id > 3500 })
	}
	params := index.NewCustomAnnParam()
	params.WithExtraParam("nprobe", 64)
	result, err := mc.Search(ctx, client.NewSearchOption(name, 4000, []entity.Vector{entity.FloatVector(rows[1].Vector)}).WithAnnParam(params).WithIgnoreGrowing(true).WithOutputFields("*").WithSkipRLS(true))
	common.CheckErr(t, err, true)
	require.Len(t, result, 1)
	checkRLSSpaceResult(t, result[0], sealed)
	return name, rows
}

func checkRLSSpaceResult(t *testing.T, result client.ResultSet, expected map[int64]rlsSpaceRow) {
	t.Helper()
	ids := result.GetColumn("id")
	if ids == nil {
		ids = result.IDs
	}
	checkRLSIDs(t, ids, slices.Sorted(maps.Keys(expected))...)
	for i := range result.Len() {
		id, err := ids.GetAsInt64(i)
		require.NoError(t, err)
		r := expected[id]
		for field, want := range map[string]any{"tenant": r.Tenant, "spaces": r.Spaces, "resource": r.Resource, "access": r.Access, "content": r.Content, "vector": entity.FloatVector(r.Vector)} {
			c := result.GetColumn(field)
			require.NotNil(t, c, field)
			isNull, err := c.IsNull(i)
			require.NoError(t, err)
			require.Equal(t, slices.Contains(r.NullFields, field), isNull, "id=%d field=%s", id, field)
			if isNull {
				continue
			}
			got, err := c.Get(i)
			require.NoError(t, err)
			if values, ok := want.([]string); ok {
				actual, ok := got.([]string)
				require.True(t, ok)
				require.True(t, slices.Equal(values, actual), "id=%d field=%s", id, field)
			} else {
				require.Equal(t, want, got, "id=%d field=%s", id, field)
			}
		}
	}
}

func checkRLSSpaceStored(ctx context.Context, t *testing.T, mc *client.Client, name string, rows map[int64]rlsSpaceRow) {
	t.Helper()
	result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").WithOutputFields("*").WithSkipRLS(true))
	common.CheckErr(t, err, true)
	checkRLSSpaceResult(t, result, rows)
}

func checkRLSSpaceReads(ctx context.Context, t *testing.T, mc *client.Client, name, principal string, rows map[int64]rlsSpaceRow, resources ...string) {
	t.Helper()
	want := maps.Clone(rows)
	maps.DeleteFunc(want, func(_ int64, r rlsSpaceRow) bool { return !slices.Contains(resources, r.Resource) })
	selected := maps.Clone(want)
	maps.DeleteFunc(selected, func(id int64, _ rlsSpaceRow) bool {
		return !(id <= 10 || id > 3000 && id <= 3010 || id > 3500 && id <= 3510)
	})
	filter := "id <= 10 or (id > 3000 and id <= 3010) or (id > 3500 and id <= 3510)"
	params := index.NewCustomAnnParam()
	params.WithExtraParam("nprobe", 64)
	q := entity.FloatVector(rows[1].Vector)
	for _, operation := range []string{"query", "get", "count", "search", "hybrid_search", "query_iterator", "search_iterator"} {
		t.Run(principal+"/"+operation, func(t *testing.T) {
			switch operation {
			case "query", "get":
				opt := client.NewQueryOption(name).WithOutputFields("*").WithRLSPrincipal(principal).WithConsistencyLevel(entity.ClStrong)
				var result client.ResultSet
				var err error
				if operation == "get" {
					result, err = mc.Get(ctx, opt.WithIDs(column.NewColumnInt64("id", slices.Sorted(maps.Keys(rows)))))
				} else {
					result, err = mc.Query(ctx, opt.WithFilter("id >= 0"))
				}
				common.CheckErr(t, err, true)
				checkRLSSpaceResult(t, result, want)
			case "count":
				result, err := mc.Query(ctx, client.NewQueryOption(name).WithOutputFields("count(*)").WithRLSPrincipal(principal))
				common.CheckErr(t, err, true)
				count, err := result.GetColumn("count(*)").GetAsInt64(0)
				require.NoError(t, err)
				require.EqualValues(t, len(want), count)
			case "search", "hybrid_search":
				var result []client.ResultSet
				var err error
				if operation == "search" {
					result, err = mc.Search(ctx, client.NewSearchOption(name, 30, []entity.Vector{q}).WithFilter(filter).WithAnnParam(params).WithOutputFields("*").WithRLSPrincipal(principal))
				} else {
					result, err = mc.HybridSearch(ctx, client.NewHybridSearchOption(name, 30,
						client.NewAnnRequest("vector", 30, q).WithFilter(filter).WithAnnParam(params),
						client.NewAnnRequest("vector", 30, q).WithFilter("("+filter+") and tenant == 'tenant_1'").WithAnnParam(params)).WithReranker(client.NewRRFReranker()).WithOutputFields("*").WithRLSPrincipal(principal))
				}
				common.CheckErr(t, err, true)
				require.Len(t, result, 1)
				checkRLSSpaceResult(t, result[0], selected)
			case "query_iterator", "search_iterator":
				var next func(context.Context) (client.ResultSet, error)
				expected := want
				if operation == "query_iterator" {
					it, err := mc.QueryIterator(ctx, client.NewQueryIteratorOption(name).WithBatchSize(127).WithOutputFields("*").WithRLSPrincipal(principal))
					common.CheckErr(t, err, true)
					next = it.Next
				} else {
					it, err := mc.SearchIterator(ctx, client.NewSearchIteratorOption(name, q).WithFilter(filter).WithAnnParam(params).WithBatchSize(7).WithOutputFields("*").WithRLSPrincipal(principal))
					common.CheckErr(t, err, true)
					next, expected = it.Next, selected
				}
				var got []int64
				ended := false
				for range len(rows)/7 + 2 {
					page, err := next(ctx)
					if errors.Is(err, io.EOF) {
						ended = true
						break
					}
					common.CheckErr(t, err, true)
					pageRows := map[int64]rlsSpaceRow{}
					ids := page.GetColumn("id")
					if ids == nil {
						ids = page.IDs
					}
					for i := range page.Len() {
						id, err := ids.GetAsInt64(i)
						require.NoError(t, err)
						require.Contains(t, expected, id)
						pageRows[id] = expected[id]
						got = append(got, id)
					}
					checkRLSSpaceResult(t, page, pageRows)
				}
				require.True(t, ended, "iterator must terminate")
				require.ElementsMatch(t, slices.Sorted(maps.Keys(expected)), got)
			}
		})
	}
}

func TestRLSSpaceReadMatrix(t *testing.T) {
	ctx := hp.CreateContext(t, 10*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name, rows := prepareRLSSpaces(ctx, t, mc, true, false)
	configureRLSSpaces(ctx, t, mc, name)
	for principal, resources := range map[string][]string{
		"member_0": {"resource_0", "resource_1"}, "member_1": {"resource_0", "resource_2"},
		"member_2": {"resource_0", "resource_1", "resource_2"}, "member_3": {"resource_3"},
		"visitor": {"resource_0", "resource_1"}, "personal": {"resource_4"}, "unknown": {},
	} {
		checkRLSSpaceReads(ctx, t, mc, name, principal, rows, resources...)
	}
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "member_0", `{"write_spaces":["tenant_0/red","tenant_1/red"]}`)), true)
	checkRLSSpaceReads(ctx, t, mc, name, "member_0", rows, "resource_0", "resource_1")
}

// Each operation starts with original chunks in indexed, unindexed and growing segments.
func TestRLSSpaceWriteMatrix(t *testing.T) {
	for _, operation := range []string{"insert", "upsert", "partial_upsert", "delete_ids", "delete_filter", "move"} {
		t.Run(operation, func(t *testing.T) {
			ctx := hp.CreateContext(t, 12*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t).Client
			name, original := prepareRLSSpaces(ctx, t, mc, true, false)
			configureRLSSpaces(ctx, t, mc, name)
			want := maps.Clone(original)
			for position, principal := range []string{"member_0", "member_1", "member_2", "member_3", "visitor", "unknown"} {
				t.Run(principal, func(t *testing.T) {
					// Keep each identity's original chunks in their initial segment forms.
					targetIDs := []int64{int64(2 + position*5), int64(3002 + position*5), int64(3502 + position*5)}
					var targets []any
					for _, id := range slices.Sorted(maps.Keys(original)) {
						if operation == "move" && original[id].Resource == "resource_1" || operation != "move" && slices.Contains(targetIDs, id) {
							targets = append(targets, original[id])
						}
					}
					var err error
					allowed := principal == "member_0" || principal == "member_2"
					if operation == "insert" {
						allowed = principal == "member_1" || principal == "member_2"
					}
					if operation == "move" {
						allowed = principal == "member_2"
					}
					var changed []any
					var ids []int64
					for _, value := range targets {
						row := value.(rlsSpaceRow)
						row.Content = "updated"
						if operation == "insert" {
							row.ID += 10000
							row.Vector = common.GenFloatVector(128)
						}
						if operation == "insert" || operation == "move" {
							row.Spaces = []string{"tenant_0/blue"}
						}
						ids = append(ids, row.ID)
						if allowed {
							want[row.ID] = row
						}
						if operation == "partial_upsert" {
							changed = append(changed, map[string]any{"id": row.ID, "content": row.Content})
						} else {
							changed = append(changed, row)
						}
					}
					switch operation {
					case "delete_ids", "delete_filter":
						requested := append(slices.Clone(ids), 1, 3001, 3501)
						opt := client.NewDeleteOption(name).WithRLSPrincipal(principal)
						if operation == "delete_ids" {
							opt.WithInt64IDs("id", requested)
						} else {
							opt.WithExpr("id in {ids}").WithTemplateParam("ids", requested)
						}
						result, err := mc.Delete(ctx, opt)
						common.CheckErr(t, err, true)
						expectedCount := 0
						if allowed {
							expectedCount = len(ids)
							for _, id := range ids {
								delete(want, id)
							}
						}
						require.EqualValues(t, expectedCount, result.DeleteCount)
					case "insert":
						_, err = mc.Insert(ctx, client.NewRowBasedInsertOption(name, changed...).WithRLSPrincipal(principal))
						common.CheckErr(t, err, allowed, "RLS")
					default:
						_, err = mc.Upsert(ctx, client.NewRowBasedInsertOption(name, changed...).WithPartialUpdate(operation == "partial_upsert").WithRLSPrincipal(principal))
						common.CheckErr(t, err, allowed, "RLS")
					}
					checkRLSSpaceStored(ctx, t, mc, name, want)
					if operation == "move" && allowed {
						checkRLSSpaceReads(ctx, t, mc, name, "member_0", want, "resource_0")
						checkRLSSpaceReads(ctx, t, mc, name, "member_1", want, "resource_0", "resource_1", "resource_2")
					}
					if operation == "insert" && allowed {
						_, err := mc.Delete(ctx, client.NewDeleteOption(name).WithInt64IDs("id", ids).WithSkipRLS(true))
						common.CheckErr(t, err, true)
						for _, id := range ids {
							delete(want, id)
						}
					}
					if operation == "move" && allowed {
						_, err := mc.Upsert(ctx, client.NewRowBasedInsertOption(name, targets...).WithSkipRLS(true))
						common.CheckErr(t, err, true)
						want = maps.Clone(original)
					}
				})
			}
		})
	}
}

func TestRLSSpaceInvalidDestinationPreservesRows(t *testing.T) {
	ctx := hp.CreateContext(t, 12*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name, rows := prepareRLSSpaces(ctx, t, mc, true, false)
	configureRLSSpaces(ctx, t, mc, name)
	for _, operation := range []string{"insert", "upsert", "partial_upsert"} {
		for _, scenario := range []string{"multiple_spaces", "empty_spaces", "foreign_tenant", "read_only_space"} {
			t.Run(operation+"/"+scenario, func(t *testing.T) {
				var input []any
				for _, id := range []int64{2, 3002, 3502} {
					r := rows[id]
					if operation == "insert" {
						r.ID += 10000
					}
					switch scenario {
					case "multiple_spaces":
						r.Spaces = []string{"tenant_0/red", "tenant_0/blue"}
					case "empty_spaces":
						r.Spaces = []string{}
					case "foreign_tenant":
						r.Tenant = "tenant_1"
					case "read_only_space":
						r.Spaces = []string{"tenant_0/common"}
					}
					input = append(input, r)
				}
				opt := client.NewRowBasedInsertOption(name, input...).WithRLSPrincipal("member_0").WithPartialUpdate(operation == "partial_upsert")
				var err error
				if operation == "insert" {
					_, err = mc.Insert(ctx, opt)
				} else {
					_, err = mc.Upsert(ctx, opt)
				}
				message := "RLS"
				if scenario == "multiple_spaces" {
					message = "capacity"
				}
				common.CheckErr(t, err, false, message)
				checkRLSSpaceStored(ctx, t, mc, name, rows)
			})
		}
	}
}

func TestRLSSpaceMembershipLifecycle(t *testing.T) {
	ctx := hp.CreateContext(t, 12*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name, rows := prepareRLSSpaces(ctx, t, mc, true, false)
	configureRLSSpaces(ctx, t, mc, name)
	policies, err := mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
	common.CheckErr(t, err, true)
	for _, stage := range []struct {
		name, tags string
		resources  []string
		write      bool
	}{
		{"public_reader", `{"write_spaces":[]}`, []string{"resource_0"}, false},
		{"join_red", `{"write_spaces":["tenant_0/red"]}`, []string{"resource_0", "resource_1"}, true},
		{"join_blue", `{"write_spaces":["tenant_0/red","tenant_0/blue"]}`, []string{"resource_0", "resource_1", "resource_2"}, true},
		{"leave_red", `{"write_spaces":["tenant_0/blue"]}`, []string{"resource_0", "resource_2"}, false},
		{"downgrade", `{"write_spaces":[]}`, []string{"resource_0"}, false},
	} {
		t.Run(stage.name, func(t *testing.T) {
			for range 2 {
				common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "member_0", stage.tags)), true)
			}
			tags, err := mc.GetRLSPrincipalTags(ctx, client.NewGetRLSPrincipalTagsOption(name, "member_0"))
			common.CheckErr(t, err, true)
			var actual map[string]any
			require.NoError(t, json.Unmarshal([]byte(tags), &actual))
			require.Equal(t, "tenant_0", actual["tenant"])
			require.Equal(t, []any{"tenant_0/common"}, actual["read_spaces"])
			checkRLSSpaceReads(ctx, t, mc, name, "member_0", rows, stage.resources...)
			_, err = mc.Upsert(ctx, client.NewRowBasedInsertOption(name, map[string]any{"id": int64(2), "content": rows[2].Content}).WithPartialUpdate(true).WithRLSPrincipal("member_0"))
			common.CheckErr(t, err, stage.write, "RLS")
		})
	}
	for _, allowed := range []bool{true, false} {
		tags := `{"write_spaces":[]}`
		if allowed {
			tags = `{"write_spaces":["tenant_0/common"]}`
		}
		common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "member_0", tags)), true)
		_, err := mc.Upsert(ctx, client.NewRowBasedInsertOption(name, rows[1]).WithRLSPrincipal("member_0"))
		common.CheckErr(t, err, allowed, "RLS")
	}
	common.CheckErr(t, mc.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "member_0")), true)
	checkRLSSpaceReads(ctx, t, mc, name, "member_0", rows)
	fresh := rows[2]
	fresh.ID = 5001
	_, err = mc.Insert(ctx, client.NewRowBasedInsertOption(name, fresh).WithRLSPrincipal("member_0"))
	common.CheckErr(t, err, false, "RLS")
	after, err := mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
	common.CheckErr(t, err, true)
	require.Equal(t, policies, after)
	checkRLSSpaceStored(ctx, t, mc, name, rows)
}

func TestRLSSpaceResourceSharingLifecycle(t *testing.T) {
	ctx := hp.CreateContext(t, 12*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name, rows := prepareRLSSpaces(ctx, t, mc, true, false)
	configureRLSSpaces(ctx, t, mc, name)
	for _, stage := range []struct {
		name      string
		access    []string
		resources []string
	}{
		{"direct_and_group", []string{"member_1", "group_0"}, []string{"resource_0", "resource_1", "resource_2"}},
		{"group_only", []string{"group_0"}, []string{"resource_0", "resource_1", "resource_2"}},
		{"revoke_group", []string{"group_0"}, []string{"resource_0", "resource_2"}},
		{"remove_member", []string{"group_0"}, nil},
	} {
		t.Run(stage.name, func(t *testing.T) {
			var input []any
			for _, id := range slices.Sorted(maps.Keys(rows)) {
				r := rows[id]
				if r.Resource == "resource_1" {
					r.Access = stage.access
					input = append(input, r)
				}
			}
			if stage.name == "direct_and_group" {
				_, err := mc.Upsert(ctx, client.NewRowBasedInsertOption(name, input...).WithRLSPrincipal("member_1"))
				common.CheckErr(t, err, false, "RLS")
				checkRLSSpaceStored(ctx, t, mc, name, rows)
				common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "member_1", `{"groups":["group_0"]}`)), true)
			}
			_, err := mc.Upsert(ctx, client.NewRowBasedInsertOption(name, input...).WithRLSPrincipal("member_0"))
			common.CheckErr(t, err, true)
			for _, v := range input {
				r := v.(rlsSpaceRow)
				rows[r.ID] = r
			}
			if stage.name == "revoke_group" {
				common.CheckErr(t, mc.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "member_1", "groups")), true)
			}
			if stage.name == "remove_member" {
				common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "member_1", `{"groups":["group_0"]}`)), true)
				common.CheckErr(t, mc.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "member_1")), true)
			}
			checkRLSSpaceReads(ctx, t, mc, name, "member_1", rows, stage.resources...)
			_, err = mc.Upsert(ctx, client.NewRowBasedInsertOption(name, map[string]any{"id": int64(2), "access": []string{"visitor"}}).WithPartialUpdate(true).WithRLSPrincipal("member_1"))
			common.CheckErr(t, err, false, "RLS")
			checkRLSSpaceStored(ctx, t, mc, name, rows)
		})
	}
}

func TestRLSSpaceLinkLifecycle(t *testing.T) {
	for _, wall := range []bool{true, false} {
		t.Run(fmt.Sprintf("tenant_wall_%t", wall), func(t *testing.T) {
			ctx := hp.CreateContext(t, 12*time.Minute)
			mc := hp.CreateDefaultMilvusClient(ctx, t).Client
			name, rows := prepareRLSSpaces(ctx, t, mc, true, false)
			configureRLSSpaces(ctx, t, mc, name)
			common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "link_0", `{"tenant":"tenant_0"}`)), true)
			var input []any
			for id, row := range rows {
				if row.Resource == "resource_1" || row.Resource == "resource_3" {
					row.Access = []string{"link_0"}
					rows[id] = row
					input = append(input, row)
				}
			}
			_, err := mc.Upsert(ctx, client.NewRowBasedInsertOption(name, input...).WithSkipRLS(true))
			common.CheckErr(t, err, true)
			expected := []string{"resource_1"}
			if !wall {
				common.CheckErr(t, mc.DropRowPolicy(ctx, client.NewDropRowPolicyOption(name, "tenant_wall")), true)
				expected = append(expected, "resource_3")
				common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "global_member", `{"read_spaces":["tenant_0/red","tenant_1/red"]}`)), true)
				checkRLSSpaceReads(ctx, t, mc, name, "global_member", rows, expected...)
			}
			checkRLSSpaceReads(ctx, t, mc, name, "link_0", rows, expected...)
			fresh := rows[2]
			fresh.ID = 5001
			_, err = mc.Insert(ctx, client.NewRowBasedInsertOption(name, fresh).WithRLSPrincipal("link_0"))
			common.CheckErr(t, err, false, "RLS")
			_, err = mc.Upsert(ctx, client.NewRowBasedInsertOption(name, rows[2]).WithRLSPrincipal("link_0"))
			common.CheckErr(t, err, false, "RLS")
			result, err := mc.Delete(ctx, client.NewDeleteOption(name).WithExpr("id >= 0").WithRLSPrincipal("link_0"))
			common.CheckErr(t, err, true)
			require.Zero(t, result.DeleteCount)
			common.CheckErr(t, mc.DeleteRLSPrincipalTags(ctx, client.NewDeleteRLSPrincipalTagsOption(name, "link_0")), true)
			if wall {
				expected = nil
			}
			checkRLSSpaceReads(ctx, t, mc, name, "link_0", rows, expected...)
			input = nil
			for id, row := range rows {
				if len(row.Access) > 0 {
					row.Access = []string{}
					rows[id] = row
					input = append(input, row)
				}
			}
			_, err = mc.Upsert(ctx, client.NewRowBasedInsertOption(name, input...).WithSkipRLS(true))
			common.CheckErr(t, err, true)
			checkRLSSpaceReads(ctx, t, mc, name, "link_0", rows)
			checkRLSSpaceStored(ctx, t, mc, name, rows)
		})
	}
}

// Prepare the data first, then synchronize metadata, and only then enable enforcement.
func TestRLSSpaceEnableExistingData(t *testing.T) {
	ctx := hp.CreateContext(t, 12*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name, rows := prepareRLSSpaces(ctx, t, mc, false, false)
	policies, err := mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
	common.CheckErr(t, err, true)
	require.Empty(t, policies)
	principals, err := mc.ListRLSPrincipals(ctx, client.NewListRLSPrincipalsOption(name))
	common.CheckErr(t, err, true)
	require.Empty(t, principals)
	result, err := mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").WithOutputFields("*"))
	common.CheckErr(t, err, true)
	checkRLSSpaceResult(t, result, rows)
	configureRLSSpaces(ctx, t, mc, name)
	policies, err = mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
	common.CheckErr(t, err, true)
	result, err = mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").WithOutputFields("*").WithRLSPrincipal("unknown"))
	common.CheckErr(t, err, true)
	checkRLSSpaceResult(t, result, rows)
	common.CheckErr(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(name).WithProperty("rls.enabled", true)), true)
	_, err = mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0"))
	common.CheckErr(t, err, false, "rls_principal")
	for principal, resources := range map[string][]string{
		"member_0": {"resource_0", "resource_1"}, "member_1": {"resource_0", "resource_2"},
		"member_2": {"resource_0", "resource_1", "resource_2"}, "member_3": {"resource_3"}, "unknown": {},
	} {
		checkRLSSpaceReads(ctx, t, mc, name, principal, rows, resources...)
	}

	// Full and partial updates operate on original chunks; rejected writes leave all rows intact.
	for _, id := range []int64{2, 3002, 3502} {
		row := rows[id]
		row.Content = "upgraded_full"
		_, err := mc.Upsert(ctx, client.NewRowBasedInsertOption(name, row).WithRLSPrincipal("member_0"))
		common.CheckErr(t, err, true)
		rows[id] = row
		_, err = mc.Upsert(ctx, client.NewRowBasedInsertOption(name, map[string]any{"id": id, "content": "upgraded_partial"}).WithPartialUpdate(true).WithRLSPrincipal("member_0"))
		common.CheckErr(t, err, true)
		row.Content = "upgraded_partial"
		rows[id] = row
		_, err = mc.Upsert(ctx, client.NewRowBasedInsertOption(name, row).WithRLSPrincipal("member_1"))
		common.CheckErr(t, err, false, "RLS")
	}
	fresh := rows[2]
	fresh.ID = 5001
	fresh.Vector = common.GenFloatVector(128)
	_, err = mc.Insert(ctx, client.NewRowBasedInsertOption(name, fresh).WithRLSPrincipal("member_0"))
	common.CheckErr(t, err, true)
	rows[fresh.ID] = fresh
	bad := rows[3]
	bad.ID = 5002
	_, err = mc.Insert(ctx, client.NewRowBasedInsertOption(name, bad).WithRLSPrincipal("member_0"))
	common.CheckErr(t, err, false, "RLS")
	deleted, err := mc.Delete(ctx, client.NewDeleteOption(name).WithInt64IDs("id", []int64{5001, 3}).WithRLSPrincipal("member_0"))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 1, deleted.DeleteCount)
	delete(rows, 5001)
	checkRLSSpaceStored(ctx, t, mc, name, rows)
	common.CheckErr(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(name).WithProperty("rls.enabled", false)), true)
	common.CheckErr(t, mc.SetRLSPrincipalTags(ctx, client.NewSetRLSPrincipalTagsOption(name, "member_0", `{"write_spaces":["tenant_0/blue"]}`)), true)
	result, err = mc.Query(ctx, client.NewQueryOption(name).WithFilter("id >= 0").WithOutputFields("*"))
	common.CheckErr(t, err, true)
	checkRLSSpaceResult(t, result, rows)
	common.CheckErr(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(name).WithProperty("rls.enabled", true)), true)
	checkRLSSpaceReads(ctx, t, mc, name, "member_0", rows, "resource_0", "resource_2")
	after, err := mc.ListRowPolicies(ctx, client.NewListRowPoliciesOption(name))
	common.CheckErr(t, err, true)
	require.Equal(t, policies, after)
	checkRLSSpaceStored(ctx, t, mc, name, rows)
}

func TestRLSSpaceAddNullableFieldsExistingData(t *testing.T) {
	ctx := hp.CreateContext(t, 12*time.Minute)
	mc := hp.CreateDefaultMilvusClient(ctx, t).Client
	name, rows := prepareRLSSpaces(ctx, t, mc, false, true)
	input := func(row rlsSpaceRow) map[string]any {
		value := map[string]any{"id": row.ID, "tenant": row.Tenant, "spaces": row.Spaces, "access": row.Access, "resource": row.Resource, "content": row.Content, "vector": row.Vector}
		for _, field := range row.NullFields {
			value[field] = nil
		}
		return value
	}
	// Backfill creates new versions; resource_0 retains implicit NULLs in old segments.
	var updates []any
	for id, row := range rows {
		if row.Resource == "resource_0" {
			continue
		}
		switch row.Resource {
		case "resource_1":
			row.Tenant, row.Spaces, row.NullFields = "tenant_0", []string{"tenant_0/red"}, []string{"access"}
		case "resource_2":
			row.Tenant, row.Access, row.NullFields = "tenant_0", []string{"member_1"}, []string{"spaces"}
		case "resource_3":
			row.Spaces, row.Access, row.NullFields = []string{"tenant_0/red"}, []string{"member_0"}, []string{"tenant"}
		case "resource_4":
			row.Tenant, row.Spaces, row.Access, row.NullFields = "tenant_0", []string{}, []string{}, nil
		}
		value := input(row)
		for _, field := range []string{"resource", "content", "vector"} {
			delete(value, field)
		}
		updates = append(updates, value)
		rows[id] = row
	}
	_, err := mc.Upsert(ctx, client.NewRowBasedInsertOption(name, updates...).WithPartialUpdate(true))
	common.CheckErr(t, err, true)
	checkRLSSpaceStored(ctx, t, mc, name, rows)
	configureRLSSpaces(ctx, t, mc, name)
	result, err := mc.Query(ctx, client.NewQueryOption(name).WithOutputFields("*").WithFilter("id >= 0").WithRLSPrincipal("unknown"))
	common.CheckErr(t, err, true)
	checkRLSSpaceResult(t, result, rows)
	common.CheckErr(t, mc.AlterCollectionProperties(ctx, client.NewAlterCollectionPropertiesOption(name).WithProperty("rls.enabled", true)), true)
	for principal, resources := range map[string][]string{"member_0": {"resource_1"}, "member_1": {"resource_2"}, "unknown": {}} {
		checkRLSSpaceReads(ctx, t, mc, name, principal, rows, resources...)
	}

	// NULL access permits a space writer; NULL tenant or space fails its write checks.
	for _, field := range []string{"tenant", "spaces"} {
		_, err = mc.Upsert(ctx, client.NewRowBasedInsertOption(name, map[string]any{"id": int64(2), field: nil}).WithPartialUpdate(true).WithRLSPrincipal("member_0"))
		common.CheckErr(t, err, false, "RLS")
		fresh := input(rows[2])
		fresh["id"], fresh[field], fresh["vector"] = int64(5001), nil, common.GenFloatVector(128)
		_, err = mc.Insert(ctx, client.NewRowBasedInsertOption(name, fresh).WithRLSPrincipal("member_0"))
		common.CheckErr(t, err, false, "RLS")
		checkRLSSpaceStored(ctx, t, mc, name, rows)
	}
	omitted := input(rows[2])
	omitted["id"], omitted["vector"] = int64(5001), common.GenFloatVector(128)
	for _, field := range []string{"tenant", "spaces", "access"} {
		delete(omitted, field)
	}
	_, err = mc.Insert(ctx, client.NewRowBasedInsertOption(name, omitted).WithRLSPrincipal("member_0"))
	common.CheckErr(t, err, false, "RLS")
	fresh := rows[2]
	fresh.ID, fresh.Vector = 5001, common.GenFloatVector(128)
	_, err = mc.Insert(ctx, client.NewRowBasedInsertOption(name, input(fresh)).WithRLSPrincipal("member_0"))
	common.CheckErr(t, err, true)
	fresh.Content = "full_update"
	_, err = mc.Upsert(ctx, client.NewRowBasedInsertOption(name, input(fresh)).WithRLSPrincipal("member_0"))
	common.CheckErr(t, err, true)
	_, err = mc.Upsert(ctx, client.NewRowBasedInsertOption(name, map[string]any{"id": int64(5001), "content": "partial_update", "access": nil}).WithPartialUpdate(true).WithRLSPrincipal("member_0"))
	common.CheckErr(t, err, true)
	fresh.Content = "partial_update"
	rows[5001] = fresh
	checkRLSSpaceStored(ctx, t, mc, name, rows)
	deleted, err := mc.Delete(ctx, client.NewDeleteOption(name).WithInt64IDs("id", []int64{5001, 1, 3001, 3501}).WithRLSPrincipal("member_0"))
	common.CheckErr(t, err, true)
	require.EqualValues(t, 1, deleted.DeleteCount)
	delete(rows, 5001)
	checkRLSSpaceStored(ctx, t, mc, name, rows)

	// A trusted migration job can resume backfill after enforcement is enabled.
	updates = nil
	for id, row := range rows {
		if row.Resource != "resource_0" {
			continue
		}
		row.Tenant, row.Spaces, row.NullFields = "tenant_0", []string{"tenant_0/red"}, []string{"access"}
		updates = append(updates, map[string]any{"id": id, "tenant": row.Tenant, "spaces": row.Spaces})
		rows[id] = row
	}
	_, err = mc.Upsert(ctx, client.NewRowBasedInsertOption(name, updates...).WithPartialUpdate(true).WithSkipRLS(true))
	common.CheckErr(t, err, true)
	checkRLSSpaceReads(ctx, t, mc, name, "member_0", rows, "resource_0", "resource_1")
	checkRLSSpaceReads(ctx, t, mc, name, "member_1", rows, "resource_2")
	common.CheckErr(t, mc.ReleaseCollection(ctx, client.NewReleaseCollectionOption(name)), true)
	load, err := mc.LoadCollection(ctx, client.NewLoadCollectionOption(name))
	common.CheckErr(t, err, true)
	common.CheckErr(t, load.Await(ctx), true)
	checkRLSSpaceReads(ctx, t, mc, name, "member_0", rows, "resource_0", "resource_1")
	checkRLSSpaceStored(ctx, t, mc, name, rows)
}
