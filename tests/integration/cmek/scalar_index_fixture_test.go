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

package cmek

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"plugin"
	"strconv"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

const (
	fixtureFieldName  = "scalar_value"
	fixtureVectorName = "embedding"
	fixturePrimaryKey = "id"
	fixtureVectorDim  = 4
	fixtureRowCount   = 2048
	fixturePluginEnv  = "MILVUS_CMEK_FIXTURE_DIR"
)

var (
	fixtureGoPluginPath     string
	fixtureGoPluginRacePath string
	fixtureCppPluginPath    string
)

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "milvus-cmek-fixture-")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	cmekDir, err := filepath.Abs(".")
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		_ = os.RemoveAll(tmpDir)
		os.Exit(1)
	}
	repoRoot, err := filepath.Abs(filepath.Join(cmekDir, "../../../"))
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		_ = os.RemoveAll(tmpDir)
		os.Exit(1)
	}
	fixtureDir := os.Getenv(fixturePluginEnv)
	if fixtureDir == "" {
		fixtureDir = filepath.Join(repoRoot, "bin", "cmek-fixtures")
	}
	fixtureGoPluginPath, fixtureGoPluginRacePath, fixtureCppPluginPath, err = fixturePluginPaths(fixtureDir)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		_ = os.RemoveAll(tmpDir)
		os.Exit(1)
	}

	if err := configureFixtureConfig(tmpDir, repoRoot, fixtureGoPluginPath, fixtureCppPluginPath); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		_ = os.RemoveAll(tmpDir)
		os.Exit(1)
	}
	if _, err := os.Stat(filepath.Join(os.Getenv("MILVUSCONF"), "hook.yaml")); err != nil { //nolint:gosec // MILVUSCONF is the test-owned fixture directory.
		_, _ = fmt.Fprintln(os.Stderr, "fixture hook config is not visible:", err)
		os.Exit(1)
	}

	code := m.Run()
	_ = os.RemoveAll(tmpDir)
	os.Exit(code)
}

func configureFixtureConfig(tmpDir, repoRoot, goPluginPath, cppPluginPath string) error {
	configDir := filepath.Join(tmpDir, "configs")
	if err := os.Mkdir(configDir, 0o755); err != nil {
		return fmt.Errorf("create fixture config directory: %w", err)
	}

	for _, configFile := range []string{"milvus.yaml", "glog.conf"} {
		defaultConfig, err := os.ReadFile(filepath.Join(repoRoot, "configs", configFile))
		if err != nil {
			return fmt.Errorf("read default %s: %w", configFile, err)
		}
		if err := os.WriteFile(filepath.Join(configDir, configFile), defaultConfig, 0o600); err != nil { //nolint:gosec // configFile is selected from a fixed test-owned list.
			return fmt.Errorf("write fixture %s: %w", configFile, err)
		}
	}

	hookConfig := fmt.Sprintf("cipherPlugin:\n  soPathGo: %q\n  soPathCpp: %q\n", goPluginPath, cppPluginPath)
	if err := os.WriteFile(filepath.Join(configDir, "hook.yaml"), []byte(hookConfig), 0o600); err != nil {
		return fmt.Errorf("write fixture hook config: %w", err)
	}
	return os.Setenv("MILVUSCONF", configDir)
}

func fixturePluginPaths(directory string) (string, string, string, error) {
	goPlugin := filepath.Join(directory, "libGoCipherPlugin.so")
	goRacePlugin := filepath.Join(directory, "libGoCipherPluginRace.so")
	cppPlugin := filepath.Join(directory, "libCipherPlugin.so")
	for _, artifact := range []string{goPlugin, goRacePlugin, cppPlugin} {
		info, err := os.Stat(artifact) //nolint:gosec // The path selects test-only artifacts built by the repository target.
		if err != nil {
			return "", "", "", fmt.Errorf("CMEK fixture artifact %s is unavailable; run make build-cmek-fixtures: %w", artifact, err)
		}
		if !info.Mode().IsRegular() {
			return "", "", "", fmt.Errorf("CMEK fixture artifact %s is not a regular file; run make build-cmek-fixtures", artifact)
		}
	}
	return goPlugin, goRacePlugin, cppPlugin, nil
}

func TestFixtureGoPluginABI(t *testing.T) {
	loaded, err := plugin.Open(testGoPluginPath())
	if err != nil {
		t.Fatalf("open Go cipher fixture: %v", err)
	}
	symbol, err := loaded.Lookup("CipherPlugin")
	if err != nil {
		t.Fatalf("lookup Go cipher fixture symbol: %v", err)
	}
	if _, ok := symbol.(hook.Cipher); !ok {
		t.Fatalf("Go cipher fixture symbol has type %T, want hook.Cipher", symbol)
	}
}

func TestFixtureCipherConfig(t *testing.T) {
	base := paramtable.NewBaseTableFromYamlOnly("hook.yaml")
	configs := base.FileConfigs()
	if got := configs["cipherpluginsopathgo"]; got != fixtureGoPluginPath {
		t.Fatalf("Go cipher plugin config = %q, want %q", got, fixtureGoPluginPath)
	}
	if got := configs["cipherpluginsopathcpp"]; got != fixtureCppPluginPath {
		t.Fatalf("C++ cipher plugin config = %q, want %q", got, fixtureCppPluginPath)
	}
}

type campaign struct {
	name          string
	engineVersion int32
	artifact      int32
}

var (
	legacyV2Campaign = campaign{name: "v2", engineVersion: 2, artifact: 2}
	packedV3Campaign = campaign{name: "v3", engineVersion: 3, artifact: 3}
)

type scalarIndexSuite struct {
	integration.MiniClusterSuite
	campaign campaign
	dbName   string
	ezID     int64
}

type ScalarIndexV2Suite struct {
	scalarIndexSuite
}

type ScalarIndexV3Suite struct {
	scalarIndexSuite
}

func (s *scalarIndexSuite) setup(c campaign) {
	s.campaign = c
	s.WithOptions(integration.WithoutResetDeploymentWhenTestTearDown())
	s.WithMilvusConfig("dataCoord.targetScalarIndexVersion", strconv.Itoa(int(c.engineVersion)))
	s.WithMilvusConfig("dataCoord.forceRebuildScalarSegmentIndex", "true")
	s.WithMilvusConfig("common.storage.useLoonFFI", "false")
	s.SetupSuite()

	ctx := s.Cluster.GetContext()
	s.dbName = "cmek_scalar_" + c.name + "_" + funcutil.GenRandomStr()
	status, err := s.Cluster.MilvusClient.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
		DbName: s.dbName,
		Properties: []*commonpb.KeyValuePair{
			{Key: common.EncryptionEnabledKey, Value: "true"},
			{Key: common.EncryptionRootKeyKey, Value: "fixture-root-key"},
		},
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))

	describe, err := s.Cluster.MilvusClient.DescribeDatabase(ctx, &milvuspb.DescribeDatabaseRequest{
		DbName: s.dbName,
	})
	s.Require().NoError(merr.CheckRPCCall(describe, err))
	s.Require().NotEmpty(describe.GetDbID())
	s.ezID = describe.GetDbID()
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(describe.GetProperties(), common.EncryptionEzIDKey))
	s.Require().Equal("fixture-root-key", propertyValue(describe.GetProperties(), common.EncryptionRootKeyKey))
}

func (s *scalarIndexSuite) tearDown() {
	if s.Cluster != nil && s.dbName != "" {
		status, err := s.Cluster.MilvusClient.DropDatabase(context.Background(), &milvuspb.DropDatabaseRequest{
			DbName: s.dbName,
		})
		s.NoError(merr.CheckRPCCall(status, err))
	}
	s.TearDownSuite()
}

func (s *scalarIndexSuite) runCell(cell scalarCell) {
	ctx := s.Cluster.GetContext()
	collectionName := fmt.Sprintf("cmek_%s_%s_%s", s.campaign.name, cell.name, funcutil.GenRandomStr())
	fieldID := int64(101)

	schema := &schemapb.CollectionSchema{
		Name: collectionName,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: fixturePrimaryKey, IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: fieldID, Name: fixtureFieldName, DataType: cell.dataType},
			{
				FieldID: 102, Name: fixtureVectorName, DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(fixtureVectorDim)}},
			},
		},
	}
	if cell.dataType == schemapb.DataType_VarChar {
		typeParams := []*commonpb.KeyValuePair{
			{Key: common.MaxLengthKey, Value: "256"},
		}
		if cell.textLog {
			typeParams = append(typeParams,
				&commonpb.KeyValuePair{Key: "enable_match", Value: "true"},
				&commonpb.KeyValuePair{Key: common.EnableAnalyzerKey, Value: "true"},
				&commonpb.KeyValuePair{Key: common.AnalyzerParamKey, Value: `{"tokenizer":"standard"}`},
			)
		}
		schema.Fields[1].TypeParams = typeParams
	}
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName: s.dbName, CollectionName: collectionName, Schema: marshaledSchema,
		ShardsNum: common.DefaultShardsNum,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	describeCollection, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{
		DbName: s.dbName, CollectionName: collectionName,
	})
	s.Require().NoError(merr.CheckRPCCall(describeCollection, err))
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(describeCollection.GetProperties(), common.EncryptionEzIDKey))
	s.Require().Equal(strconv.FormatInt(s.ezID, 10), propertyValue(describeCollection.GetSchema().GetProperties(), common.EncryptionEzIDKey))
	defer s.cleanupCollection(collectionName)

	primaryKeys := make([]int64, fixtureRowCount)
	for i := range primaryKeys {
		primaryKeys[i] = int64(i)
	}
	fields := []*schemapb.FieldData{
		newInt64FieldData(fixturePrimaryKey, primaryKeys),
		cell.data(fixtureFieldName, fixtureRowCount),
		integration.NewFloatVectorFieldData(fixtureVectorName, fixtureRowCount, fixtureVectorDim),
	}
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		DbName: s.dbName, CollectionName: collectionName, FieldsData: fields,
		HashKeys: integration.GenerateHashKeys(fixtureRowCount), NumRows: fixtureRowCount,
	})
	s.Require().NoError(merr.CheckRPCCall(insert, err))

	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName: s.dbName, CollectionNames: []string{collectionName},
	})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	s.WaitForFlush(ctx, flush.GetCollSegIDs()[collectionName].GetData(),
		flush.GetCollFlushTs()[collectionName], s.dbName, collectionName)
	segments := s.sealedSegments(collectionName)
	for _, segment := range segments {
		s.Require().Equal(storage.StorageV2, segment.GetStorageVersion(), "segment %d", segment.GetID())
	}
	vectorIndex, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		DbName: s.dbName, CollectionName: collectionName, FieldName: fixtureVectorName,
		IndexName: "cmek_vector", ExtraParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "FLAT"},
			{Key: common.MetricTypeKey, Value: "L2"},
		},
	})
	s.Require().NoError(merr.CheckRPCCall(vectorIndex, err))
	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collectionName, fixtureVectorName)

	if !cell.textLog {
		indexName := "cmek_" + cell.indexType
		indexParams := append([]*commonpb.KeyValuePair(nil), cell.indexParams...)
		if cell.indexType != "HYBRID" {
			indexParams = append([]*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: cell.indexType}}, indexParams...)
		}
		createIndex, err := s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
			DbName: s.dbName, CollectionName: collectionName, FieldName: fixtureFieldName,
			IndexName: indexName, ExtraParams: indexParams,
		})
		s.Require().NoError(merr.CheckRPCCall(createIndex, err))
		s.WaitForIndexBuiltWithDB(ctx, s.dbName, collectionName, fixtureFieldName)
	}

	segments = s.sealedSegments(collectionName)
	for _, segment := range segments {
		s.Require().Equal(storage.StorageV2, segment.GetStorageVersion(), "segment %d", segment.GetID())
	}
	var objects []inspector.Object
	var lastLocateErr error
	locateCtx, locateCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer locateCancel()

locate:
	for {
		if err := locateCtx.Err(); err != nil {
			lastLocateErr = fmt.Errorf("locate index: %w", err)
			break
		}
		if cell.textLog {
			segments = s.sealedSegments(collectionName)
		}
		if cell.textLog {
			objects, err = inspector.LocateTextLog(s.Cluster.RootPath(), segments, fieldID, s.campaign.engineVersion)
		} else {
			requestCtx, cancel := context.WithTimeout(locateCtx, 5*time.Second)
			objects, err = inspector.LocateScalarIndex(requestCtx, s.Cluster.MixCoordClient, segments,
				cell.indexType, fieldID, s.campaign.engineVersion)
			cancel()
		}
		lastLocateErr = err
		if err == nil && len(objects) > 0 {
			break
		}
		select {
		case <-locateCtx.Done():
			lastLocateErr = fmt.Errorf("locate index: %w", locateCtx.Err())
			break locate
		case <-time.After(500 * time.Millisecond):
		}
	}
	s.Require().NoError(lastLocateErr)
	s.Require().NotEmpty(objects)
	reader := inspector.ObjectReader{ChunkManager: s.Cluster.ChunkManager}
	for _, object := range objects {
		raw, readErr := reader.Read(ctx, object)
		s.Require().NoError(readErr, object.Path)
		if s.campaign.artifact == 2 {
			s.Require().NoError(inspector.InspectV2(raw, s.ezID), object.Path)
		} else {
			s.Require().NoError(inspector.InspectV3(raw, s.ezID), object.Path)
		}
	}

	load, err := s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName: s.dbName, CollectionName: collectionName,
	})
	s.Require().NoError(merr.CheckRPCCall(load, err))
	s.WaitForLoadWithDB(ctx, s.dbName, collectionName)
	s.assertOracle(cell, collectionName)

	release, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		DbName: s.dbName, CollectionName: collectionName,
	})
	s.Require().NoError(merr.CheckRPCCall(release, err))
	s.CheckCollectionCacheReleased(segments[0].GetCollectionID())

	load, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName: s.dbName, CollectionName: collectionName,
	})
	s.Require().NoError(merr.CheckRPCCall(load, err))
	s.WaitForLoadWithDB(ctx, s.dbName, collectionName)
	s.assertOracle(cell, collectionName)
}

func (s *scalarIndexSuite) sealedSegments(collectionName string) []*datapb.SegmentInfo {
	var segments []*datapb.SegmentInfo
	var lastErr error
	deadline := time.Now().Add(2 * time.Minute)
	for {
		current, err := s.showSegments(collectionName)
		lastErr = err
		if err == nil {
			sealed := make([]*datapb.SegmentInfo, 0, len(current))
			for _, segment := range current {
				if (segment.GetState() == commonpb.SegmentState_Sealed ||
					segment.GetState() == commonpb.SegmentState_Flushed) &&
					segment.GetNumOfRows() > 0 && !segment.GetCompacted() && !segment.GetIsInvisible() {
					sealed = append(sealed, segment)
				}
			}
			segments = sealed
			if len(segments) > 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	s.Require().NoError(lastErr)
	s.Require().NotEmpty(segments)
	return segments
}

func (s *scalarIndexSuite) showSegments(collectionName string) (segments []*datapb.SegmentInfo, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("show segments: %v", recovered)
		}
	}()
	return s.Cluster.ShowSegmentsWithDB(s.dbName, collectionName)
}

func (s *scalarIndexSuite) cleanupCollection(collectionName string) {
	ctx := context.Background()
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{
		DbName: s.dbName, CollectionName: collectionName,
	})
	if releaseErr := merr.CheckRPCCall(status, err); releaseErr != nil {
		s.T().Logf("release collection %s during cleanup: %v", collectionName, releaseErr)
	}
	status, err = s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		DbName: s.dbName, CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(status, err))
}

func propertyValue(properties []*commonpb.KeyValuePair, key string) string {
	for _, property := range properties {
		if property.GetKey() == key {
			return property.GetValue()
		}
	}
	return ""
}

func newInt64FieldData(fieldName string, values []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type: schemapb.DataType_Int64, FieldName: fieldName,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
		}},
	}
}
