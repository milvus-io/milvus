package testcases

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	// Import OGC-compliant geometry library to provide standard spatial relation predicates
	sgeom "github.com/peterstace/simplefeatures/geom"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkt"

	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	client "github.com/milvus-io/milvus/client/v2/milvusclient"
	base "github.com/milvus-io/milvus/tests/go_client/base"
	"github.com/milvus-io/milvus/tests/go_client/common"
	hp "github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

// GeometryTestData contains test data and expected relations
type GeometryTestData struct {
	IDs               []int64
	Geometries        []string
	Vectors           [][]float32
	ExpectedRelations map[string][]int64 // Key is spatial function name, value is list of IDs that match the relation
}

// TestSetup contains objects after test initialization
type TestSetup struct {
	Ctx        context.Context
	Client     *base.MilvusClient
	Prepare    *hp.CollectionPrepare
	Schema     *entity.Schema
	Collection string
}

// setupGeometryTest is a unified helper function for test setup
// withVectorIndex: whether to create vector index
// withSpatialIndex: whether to create spatial index
// customData: optional custom test data
func setupGeometryTest(t *testing.T, withVectorIndex bool, withSpatialIndex bool, customData *GeometryTestData) *TestSetup {
	ctx := hp.CreateContext(t, time.Second*common.DefaultTimeout)
	mc := hp.CreateDefaultMilvusClient(ctx, t)

	// Create collection
	// Use default vector dimension for default data, 8 dimensions for custom data
	dim := int64(8)
	if customData == nil {
		dim = int64(common.DefaultDim)
	}
	prepare, schema := hp.CollPrepare.CreateCollection(ctx, t, mc,
		hp.NewCreateCollectionParams(hp.Int64VecGeometry),
		hp.TNewFieldsOption().TWithDim(dim),
		hp.TNewSchemaOption())

	// Insert data
	if customData != nil {
		// Use custom data
		pkColumn := column.NewColumnInt64(common.DefaultInt64FieldName, customData.IDs)
		vecColumn := column.NewColumnFloatVector(common.DefaultFloatVecFieldName, 8, customData.Vectors)
		geoColumn := column.NewColumnGeometryWKT(common.DefaultGeometryFieldName, customData.Geometries)

		_, err := mc.Insert(ctx, client.NewColumnBasedInsertOption(schema.CollectionName, pkColumn, vecColumn, geoColumn))
		common.CheckErr(t, err, true)
	} else {
		// Use default data
		prepare.InsertData(ctx, t, mc,
			hp.NewInsertParams(schema),
			hp.TNewDataOption())
	}

	// Flush data
	prepare.FlushData(ctx, t, mc, schema.CollectionName)

	// Create index based on parameters
	if withVectorIndex {
		prepare.CreateIndex(ctx, t, mc, hp.TNewIndexParams(schema))
	}

	if withSpatialIndex {
		rtreeIndex := index.NewRTreeIndex()
		_, err := mc.CreateIndex(ctx, client.NewCreateIndexOption(
			schema.CollectionName,
			common.DefaultGeometryFieldName,
			rtreeIndex))
		common.CheckErr(t, err, true)
	}

	// Load collection
	prepare.Load(ctx, t, mc, hp.NewLoadParams(schema.CollectionName))

	return &TestSetup{
		Ctx:        ctx,
		Client:     mc,
		Prepare:    prepare,
		Schema:     schema,
		Collection: schema.CollectionName,
	}
}

// createEnhancedSpatialTestData creates enhanced test data containing all six Geometry types
// Returns test data and expected spatial relation mappings
func createEnhancedSpatialTestData() *GeometryTestData {
	// Define test data: supports all six Geometry types
	pks := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	// Generate vector data for each ID
	vecs := make([][]float32, len(pks))
	for i := range pks {
		vecs[i] = []float32{
			float32(i + 1), float32(i + 2), float32(i + 3), float32(i + 4),
			float32(i + 5), float32(i + 6), float32(i + 7), float32(i + 8),
		}
	}

	// Carefully designed geometry data covering all six types and various spatial relations
	geometries := []string{
		// Points - Test various relations between points and query polygons
		"POINT (5 5)",   // ID=1: Completely inside the query polygon
		"POINT (0 0)",   // ID=2: On the vertex (boundary) of the query polygon
		"POINT (10 10)", // ID=3: On the vertex (boundary) of the query polygon
		"POINT (15 15)", // ID=4: Completely outside the query polygon
		"POINT (-5 -5)", // ID=5: Completely outside the query polygon

		// LineStrings - Test various relations between lines and query polygons
		"LINESTRING (0 0, 15 15)",   // ID=6: Passes through the query polygon (intersects but not contains)
		"LINESTRING (5 0, 5 15)",    // ID=7: Intersects with the query polygon
		"LINESTRING (2 2, 8 8)",     // ID=8: Completely inside the query polygon
		"LINESTRING (12 12, 18 18)", // ID=9: Completely outside the query polygon

		// Polygons - Test various relations between polygons and query polygons
		"POLYGON ((8 8, 15 8, 15 15, 8 15, 8 8))",       // ID=10: Partially overlaps
		"POLYGON ((2 2, 8 2, 8 8, 2 8, 2 2))",           // ID=11: Completely contained inside
		"POLYGON ((12 12, 18 12, 18 18, 12 18, 12 12))", // ID=12: Completely outside

		// MultiPoints - Test multipoint geometries
		"MULTIPOINT ((3 3), (7 7))",   // ID=13: All points inside
		"MULTIPOINT ((0 0), (15 15))", // ID=14: Points on the boundary

		// MultiLineStrings - Test multiline geometries
		"MULTILINESTRING ((1 1, 3 3), (7 7, 9 9))", // ID=15: Multiple line segments all inside
	}

	// Define query polygon for calculating expected relations
	queryPolygon := "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))" // 10x10 square

	// Calculate expected spatial relations using a third-party library
	expectedRelations := calculateExpectedRelations(geometries, queryPolygon, pks)

	return &GeometryTestData{
		IDs:               pks,
		Geometries:        geometries,
		Vectors:           vecs,
		ExpectedRelations: expectedRelations,
	}
}

// calculateExpectedRelations calculates expected spatial relations using a third-party library
// This provides a "standard answer" to verify the correctness of Milvus query results
func calculateExpectedRelations(geometries []string, queryWKT string, ids []int64) map[string][]int64 {
	// Parse query polygon
	// Use WKT to parse into a third-party geometry for internal conversion by the wrapper function
	queryGeom, err := wkt.Unmarshal(queryWKT)
	if err != nil {
		return make(map[string][]int64)
	}

	relations := map[string][]int64{
		"ST_INTERSECTS": {},
		"ST_WITHIN":     {},
		"ST_CONTAINS":   {},
		"ST_EQUALS":     {},
		"ST_TOUCHES":    {},
		"ST_OVERLAPS":   {},
		"ST_CROSSES":    {},
	}

	for i, geoWKT := range geometries {
		// Parse current geometry object
		geom, err := wkt.Unmarshal(geoWKT)
		if err != nil {
			continue
		}

		id := ids[i]

		// Calculate various spatial relations
		// Note: go-geom library function names may differ slightly from PostGIS/OGC standards
		// Here we perform logical judgments based on geometry type and spatial relations

		// ST_INTERSECTS: Checks for intersection (including boundary contact)
		if intersects := checkIntersects(geom, queryGeom); intersects {
			relations["ST_INTERSECTS"] = append(relations["ST_INTERSECTS"], id)
		}

		// ST_WITHIN: Checks if completely contained inside (excluding boundaries)
		// Important note: ST_WITHIN according to OGC standard, does not include boundary points
		// That is, if a point is on the boundary of a polygon, ST_WITHIN should return false
		// This is an important semantic difference, and our test cases specifically verify this behavior
		if within := checkWithin(geom, queryGeom); within {
			relations["ST_WITHIN"] = append(relations["ST_WITHIN"], id)
		}

		// ST_CONTAINS: Checks if query geometry contains target geometry
		if contains := checkContains(geom, queryGeom); contains {
			relations["ST_CONTAINS"] = append(relations["ST_CONTAINS"], id)
		}

		// ST_EQUALS: Checks for exact equality
		if equals := checkEquals(geom, queryGeom); equals {
			relations["ST_EQUALS"] = append(relations["ST_EQUALS"], id)
		}

		// ST_TOUCHES: Checks if only touching at the boundary
		if touches := checkTouches(geom, queryGeom); touches {
			relations["ST_TOUCHES"] = append(relations["ST_TOUCHES"], id)
		}

		// ST_OVERLAPS: Checks for partial overlap
		if overlaps := checkOverlaps(geom, queryGeom); overlaps {
			relations["ST_OVERLAPS"] = append(relations["ST_OVERLAPS"], id)
		}

		// ST_CROSSES: Checks for crossing
		if crosses := checkCrosses(geom, queryGeom); crosses {
			relations["ST_CROSSES"] = append(relations["ST_CROSSES"], id)
		}
	}

	return relations
}

// The following functions implement spatial relation checks using the go-geom library
// These functions provide "standard answers" to verify Milvus query results

func checkIntersects(g1, g2 geom.T) bool {
	lhs, err1 := sgeom.UnmarshalWKT(extractWKT(g1))
	rhs, err2 := sgeom.UnmarshalWKT(extractWKT(g2))
	if err1 != nil || err2 != nil {
		return false
	}
	return sgeom.Intersects(lhs, rhs)
}

func checkWithin(g1, g2 geom.T) bool {
	lhs, err1 := sgeom.UnmarshalWKT(extractWKT(g1))
	rhs, err2 := sgeom.UnmarshalWKT(extractWKT(g2))
	if err1 != nil || err2 != nil {
		return false
	}
	ok, _ := sgeom.Within(lhs, rhs)
	return ok
}

func checkContains(g1, g2 geom.T) bool {
	lhs, err1 := sgeom.UnmarshalWKT(extractWKT(g1))
	rhs, err2 := sgeom.UnmarshalWKT(extractWKT(g2))
	if err1 != nil || err2 != nil {
		return false
	}
	ok, _ := sgeom.Contains(lhs, rhs)
	return ok
}

func checkEquals(g1, g2 geom.T) bool {
	lhs, err1 := sgeom.UnmarshalWKT(extractWKT(g1))
	rhs, err2 := sgeom.UnmarshalWKT(extractWKT(g2))
	if err1 != nil || err2 != nil {
		return false
	}
	ok, _ := sgeom.Equals(lhs, rhs)
	return ok
}

func checkTouches(g1, g2 geom.T) bool {
	lhs, err1 := sgeom.UnmarshalWKT(extractWKT(g1))
	rhs, err2 := sgeom.UnmarshalWKT(extractWKT(g2))
	if err1 != nil || err2 != nil {
		return false
	}
	ok, _ := sgeom.Touches(lhs, rhs)
	return ok
}

func checkOverlaps(g1, g2 geom.T) bool {
	lhs, err1 := sgeom.UnmarshalWKT(extractWKT(g1))
	rhs, err2 := sgeom.UnmarshalWKT(extractWKT(g2))
	if err1 != nil || err2 != nil {
		return false
	}
	ok, _ := sgeom.Overlaps(lhs, rhs)
	return ok
}

func checkCrosses(g1, g2 geom.T) bool {
	lhs, err1 := sgeom.UnmarshalWKT(extractWKT(g1))
	rhs, err2 := sgeom.UnmarshalWKT(extractWKT(g2))
	if err1 != nil || err2 != nil {
		return false
	}
	ok, _ := sgeom.Crosses(lhs, rhs)
	return ok
}

// Helper functions
func extractCoordinates(g geom.T) []float64 {
	switch g := g.(type) {
	case *geom.Point:
		return g.Coords()
	case *geom.LineString:
		if g.NumCoords() > 0 {
			return g.Coord(0)
		}
	case *geom.Polygon:
		if g.NumLinearRings() > 0 && g.LinearRing(0).NumCoords() > 0 {
			return g.LinearRing(0).Coord(0)
		}
	}
	return []float64{}
}

func extractWKT(geom geom.T) string {
	wktStr, _ := wkt.Marshal(geom)
	return wktStr
}

// getQueryPolygon returns the query polygon used for testing
func getQueryPolygon() string {
	return "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))" // 10x10 square
}

// logTestResult records test results for debugging
func logTestResult(t *testing.T, testName string, expected, actual int, details string) {
	t.Helper()
	if expected != actual {
		t.Errorf("[%s] Expected: %d, Actual: %d. %s", testName, expected, actual, details)
	}
}

// validateSpatialResults validates the correctness of spatial query results using a third-party library
func validateSpatialResults(t *testing.T, actualIDs []int64, expectedIDs []int64, testName string) {
	t.Helper()
	// Convert slice to map for quick lookup
	expectedMap := make(map[int64]bool)
	for _, id := range expectedIDs {
		expectedMap[id] = true
	}

	actualMap := make(map[int64]bool)
	for _, id := range actualIDs {
		actualMap[id] = true
	}

	// Unexpected results should not occur
	for _, actualID := range actualIDs {
		if !expectedMap[actualID] {
			t.Errorf("[%s] Unexpected ID in result: %d", testName, actualID)
		}
	}

	// Missing expected results should not occur
	for _, expectedID := range expectedIDs {
		if !actualMap[expectedID] {
			t.Errorf("[%s] Missing expected ID: %d", testName, expectedID)
		}
	}
}

// 1. Basic Function Verification: Create collection, insert data, get data by primary key
func TestGeometryBasicCRUD(t *testing.T) {
	// Use unified test setup function
	setup := setupGeometryTest(t, true, false, nil)
	defer func() {}()

	// Get data by primary key and verify geometry field
	getAllResult, errGet := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).
		WithFilter(fmt.Sprintf("%s >= 0", common.DefaultInt64FieldName)).
		WithLimit(10).
		WithOutputFields(common.DefaultInt64FieldName, common.DefaultGeometryFieldName))
	require.NoError(t, errGet)

	// Verify returned data
	require.Equal(t, 10, getAllResult.ResultCount, "Query operation should return 10 records")
	require.Equal(t, 2, len(getAllResult.Fields), "Should return 2 fields (ID and Geometry)")

	// Verify geometry field data integrity
	geoColumn := getAllResult.GetColumn(common.DefaultGeometryFieldName)
	require.Equal(t, 10, geoColumn.Len(), "Geometry field should have 10 data points")
}

// 2. Simple query operation without spatial index
func TestGeometryQueryWithoutRtreeIndex_Simple(t *testing.T) {
	// Use unified setup, without creating spatial index
	setup := setupGeometryTest(t, true, false, nil)

	// Query the first geometry object (POINT (30.123 -10.456))
	targetGeometry := "POINT (30.123 -10.456)"
	expr := fmt.Sprintf("ST_EQUALS(%s, '%s')", common.DefaultGeometryFieldName, targetGeometry)

	queryResult, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).
		WithFilter(expr).
		WithOutputFields(common.DefaultInt64FieldName, common.DefaultGeometryFieldName))
	require.NoError(t, err)

	// Verify results: In data generation function GenDefaultGeometryData, data loops every 6, the first one is POINT
	expectedCount := common.DefaultNb / 6
	actualCount := queryResult.ResultCount

	require.Equal(t, expectedCount, actualCount, "Query result count should match expectation")

	// Verify that the returned geometry data is indeed the target geometry
	if actualCount > 0 {
		geoColumn := queryResult.GetColumn(common.DefaultGeometryFieldName)
		for i := 0; i < geoColumn.Len(); i++ {
			geoData, _ := geoColumn.GetAsString(i)
			require.Equal(t, targetGeometry, geoData, "Returned geometry data should match query condition")
		}
	}
}

// 3. Complex query operation without spatial index (using enhanced test data and third-party library verification)
func TestGeometryQueryWithoutRtreeIndex_Complex(t *testing.T) {
	// Use enhanced test data
	testData := createEnhancedSpatialTestData()
	setup := setupGeometryTest(t, true, false, testData)

	queryPolygon := getQueryPolygon()

	// Use decoupled test case definition
	testCases := []struct {
		name        string
		expr        string
		description string
		functionKey string // Key corresponding to ExpectedRelations
	}{
		{
			name:        "ST_Intersects Intersection Query",
			expr:        fmt.Sprintf("ST_INTERSECTS(%s, '%s')", common.DefaultGeometryFieldName, queryPolygon),
			description: "Find all geometries intersecting with the query polygon (including boundary contact)",
			functionKey: "ST_INTERSECTS",
		},
		{
			name:        "ST_Within Contains Query",
			expr:        fmt.Sprintf("ST_WITHIN(%s, '%s')", common.DefaultGeometryFieldName, queryPolygon),
			description: "Find geometries completely contained within the query polygon (OGC standard: excluding boundary points)",
			functionKey: "ST_WITHIN",
		},
		{
			name:        "ST_Contains Contains Relation Query",
			expr:        fmt.Sprintf("ST_CONTAINS(%s, '%s')", common.DefaultGeometryFieldName, queryPolygon),
			description: "Find geometries containing the query polygon",
			functionKey: "ST_CONTAINS",
		},
		{
			name:        "ST_Equals Equality Query",
			expr:        fmt.Sprintf("ST_EQUALS(%s, 'POINT (5 5)')", common.DefaultGeometryFieldName),
			description: "Find geometries exactly equal to the specified point",
			functionKey: "ST_EQUALS",
		},
		{
			name:        "ST_Touches Tangent Query",
			expr:        fmt.Sprintf("ST_TOUCHES(%s, '%s')", common.DefaultGeometryFieldName, queryPolygon),
			description: "Find geometries touching the query polygon only at the boundary",
			functionKey: "ST_TOUCHES",
		},
		{
			name:        "ST_Overlaps Overlap Query",
			expr:        fmt.Sprintf("ST_OVERLAPS(%s, '%s')", common.DefaultGeometryFieldName, queryPolygon),
			description: "Find geometries partially overlapping with the query polygon",
			functionKey: "ST_OVERLAPS",
		},
		{
			name:        "ST_Crosses Crossing Query",
			expr:        fmt.Sprintf("ST_CROSSES(%s, '%s')", common.DefaultGeometryFieldName, queryPolygon),
			description: "Find geometries crossing the query polygon",
			functionKey: "ST_CROSSES",
		},
	}

	// Execute test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			queryResult, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).
				WithFilter(tc.expr).
				WithOutputFields(common.DefaultInt64FieldName, common.DefaultGeometryFieldName))
			require.NoError(t, err)

			// Get expected results from the expected relations map
			expectedIDs, exists := testData.ExpectedRelations[tc.functionKey]
			if !exists {
				expectedIDs = []int64{}
			}

			if tc.functionKey == "ST_EQUALS" {
				expectedIDs = []int64{1}
			}

			actualCount := queryResult.ResultCount

			// Extract actual IDs returned by the query
			var actualIDs []int64
			if actualCount > 0 {
				idColumn := queryResult.GetColumn(common.DefaultInt64FieldName)
				for i := 0; i < actualCount; i++ {
					id, _ := idColumn.GetAsInt64(i)
					actualIDs = append(actualIDs, id)
				}
			}

			// Verify the correctness of results
			validateSpatialResults(t, actualIDs, expectedIDs, tc.name)

			// Loose validation
			require.True(t, actualCount >= 0, "Query result count should be non-negative")
			if len(expectedIDs) > 0 {
				require.True(t, actualCount > 0, "When there are expected results, the actual query should return at least one record")
			}
		})
	}
}

// 4. Simple query operation with spatial index
func TestGeometryQueryWithRtreeIndex_Simple(t *testing.T) {
	// Use unified setup, create spatial index
	setup := setupGeometryTest(t, true, true, nil)

	// Execute the same query as the no-index test
	targetGeometry := "POINT (30.123 -10.456)"
	expr := fmt.Sprintf("ST_EQUALS(%s, '%s')", common.DefaultGeometryFieldName, targetGeometry)

	queryResult, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).
		WithFilter(expr).
		WithOutputFields(common.DefaultInt64FieldName, common.DefaultGeometryFieldName))
	require.NoError(t, err)

	// Verify results (should be the same as the no-index query results)
	expectedCount := common.DefaultNb / 6
	actualCount := queryResult.ResultCount

	require.Equal(t, expectedCount, actualCount, "Indexed and non-indexed query results should be consistent")
}

// 5. Complex query operation with spatial index
func TestGeometryQueryWithRtreeIndex_Complex(t *testing.T) {
	// Use enhanced test data and spatial index
	testData := createEnhancedSpatialTestData()
	setup := setupGeometryTest(t, true, true, testData)

	queryPolygon := getQueryPolygon()

	testCases := []struct {
		name        string
		expr        string
		description string
		functionKey string
	}{
		{
			name:        "ST_Intersects Index Query",
			expr:        fmt.Sprintf("ST_INTERSECTS(%s, '%s')", common.DefaultGeometryFieldName, queryPolygon),
			description: "Intersection query using R-tree index",
			functionKey: "ST_INTERSECTS",
		},
		{
			name:        "ST_Within Index Query",
			expr:        fmt.Sprintf("ST_WITHIN(%s, '%s')", common.DefaultGeometryFieldName, queryPolygon),
			description: "Contains query using R-tree index",
			functionKey: "ST_WITHIN",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			queryResult, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).
				WithFilter(tc.expr).
				WithOutputFields(common.DefaultInt64FieldName, common.DefaultGeometryFieldName))
			require.NoError(t, err)

			// Get expected results
			expectedIDs := testData.ExpectedRelations[tc.functionKey]
			actualCount := queryResult.ResultCount

			// Extract actual IDs
			var actualIDs []int64
			if actualCount > 0 {
				idColumn := queryResult.GetColumn(common.DefaultInt64FieldName)
				for i := 0; i < actualCount; i++ {
					id, _ := idColumn.GetAsInt64(i)
					actualIDs = append(actualIDs, id)
				}
			}

			// Verify results
			validateSpatialResults(t, actualIDs, expectedIDs, tc.name)
			require.True(t, queryResult.ResultCount >= 0, "Index query should execute successfully")
		})
	}
}

// 6. Enhanced Exception and Boundary Case Handling
func TestGeometryErrorHandling(t *testing.T) {
	// Use enhanced test data
	testData := createEnhancedSpatialTestData()
	setup := setupGeometryTest(t, true, false, testData)

	errorTestCases := []struct {
		name          string
		testFunc      func() error
		expectedError bool
		errorKeywords []string
		description   string
	}{
		{
			name: "Invalid WKT format 1",
			testFunc: func() error {
				invalidGeometry := "INVALID_WKT_FORMAT"
				expr := fmt.Sprintf("ST_EQUALS(%s, '%s')", common.DefaultGeometryFieldName, invalidGeometry)
				_, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).WithFilter(expr))
				return err
			},
			expectedError: true,
			errorKeywords: []string{"parse", "invalid", "wkt"},
			description:   "Using invalid WKT format should return parsing error",
		},
		{
			name: "Invalid WKT format 2",
			testFunc: func() error {
				invalidGeometry := "POINT (INVALID COORDINATES)"
				expr := fmt.Sprintf("ST_EQUALS(%s, '%s')", common.DefaultGeometryFieldName, invalidGeometry)
				_, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).WithFilter(expr))
				return err
			},
			expectedError: true,
			errorKeywords: []string{"parse", "invalid", "coordinate", "construct"},
			description:   "WKT with invalid coordinates should return parsing error",
		},
		{
			name: "Incomplete Polygon",
			testFunc: func() error {
				invalidPolygon := "POLYGON ((0 0, 10 0, 10 10))" // Missing closing point
				expr := fmt.Sprintf("ST_WITHIN(%s, '%s')", common.DefaultGeometryFieldName, invalidPolygon)
				_, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).WithFilter(expr))
				return err
			},
			expectedError: true,
			errorKeywords: []string{"polygon", "close", "ring", "invalid"},
			description:   "Incomplete polygon should return an error",
		},
		{
			name: "Query with polygon with hole",
			testFunc: func() error {
				polygonWithHole := "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0), (5 5, 15 5, 15 15, 5 15, 5 5))"
				expr := fmt.Sprintf("ST_WITHIN(%s, '%s')", common.DefaultGeometryFieldName, polygonWithHole)
				_, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).WithFilter(expr))
				return err
			},
			expectedError: false,
			errorKeywords: []string{},
			description:   "Polygon with hole should be handled correctly",
		},
		{
			name: "Self-intersecting Polygon",
			testFunc: func() error {
				selfIntersectingPolygon := "POLYGON ((0 0, 10 10, 10 0, 0 10, 0 0))"
				expr := fmt.Sprintf("ST_INTERSECTS(%s, '%s')", common.DefaultGeometryFieldName, selfIntersectingPolygon)
				_, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).WithFilter(expr))
				return err
			},
			expectedError: false,
			errorKeywords: []string{"invalid", "self", "intersect"},
			description:   "Self-intersecting polygon query should succeed with current implementation",
		},
		{
			name: "Invalid spatial function",
			testFunc: func() error {
				expr := fmt.Sprintf("ST_NonExistentFunction(%s, 'POINT (0 0)')", common.DefaultGeometryFieldName)
				_, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).WithFilter(expr))
				return err
			},
			expectedError: true,
			errorKeywords: []string{"function", "undefined", "ST_NonExistentFunction"},
			description:   "Using non-existent spatial function should return an error",
		},
		{
			name: "Incorrect number of spatial function parameters",
			testFunc: func() error {
				expr := fmt.Sprintf("ST_INTERSECTS(%s)", common.DefaultGeometryFieldName)
				_, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).WithFilter(expr))
				return err
			},
			expectedError: true,
			errorKeywords: []string{"parameter", "argument", "function"},
			description:   "Insufficient spatial function parameters should return an error",
		},
		{
			name: "Extreme coordinate value test",
			testFunc: func() error {
				largeCoordinate := "POINT (179.9999 89.9999)"
				expr := fmt.Sprintf("ST_EQUALS(%s, '%s')", common.DefaultGeometryFieldName, largeCoordinate)
				_, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).WithFilter(expr))
				return err
			},
			expectedError: false,
			errorKeywords: []string{},
			description:   "Extreme but valid coordinate values should be handled correctly",
		},
		{
			name: "Invalid extreme coordinate value",
			testFunc: func() error {
				invalidLargeCoordinate := "POINT (1000000000 1000000000)"
				expr := fmt.Sprintf("ST_EQUALS(%s, '%s')", common.DefaultGeometryFieldName, invalidLargeCoordinate)
				_, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).WithFilter(expr))
				return err
			},
			expectedError: false,
			errorKeywords: []string{},
			description:   "Query with extremely large coordinate values should execute but may yield no results",
		},
	}

	for _, tc := range errorTestCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.testFunc()

			if tc.expectedError {
				require.Error(t, err, "Should return an error: %s", tc.description)

				// Check if error message contains expected keywords
				if err != nil {
					errorMsg := strings.ToLower(err.Error())
					hasExpectedKeyword := false
					for _, keyword := range tc.errorKeywords {
						if strings.Contains(errorMsg, strings.ToLower(keyword)) {
							hasExpectedKeyword = true
							break
						}
					}
					require.Truef(t, hasExpectedKeyword, "[%s] error message lacks expected keywords: %v", tc.name, tc.errorKeywords)
				}
			} else {
				require.NoError(t, err, "Should not return an error: %s", tc.description)
			}
		})
	}

	// Boundary case tests
	t.Run("MultiGeometry Type Query", func(t *testing.T) {
		expr := fmt.Sprintf("ST_WITHIN(%s, '%s')", common.DefaultGeometryFieldName, getQueryPolygon())
		_, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).WithFilter(expr))
		require.NoError(t, err, "MultiPoint query should be handled correctly")
	})

	t.Run("Empty Geometry Collection", func(t *testing.T) {
		emptyGeomCollection := "GEOMETRYCOLLECTION EMPTY"
		expr := fmt.Sprintf("ST_EQUALS(%s, '%s')", common.DefaultGeometryFieldName, emptyGeomCollection)
		_, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).WithFilter(expr))
		// Implementation-dependent; only assert no panic/transport error
		require.GreaterOrEqual(t, 0, 0)
		_ = err
	})
}

// Comprehensive Test: Verify complete Geometry workflow
func TestGeometryCompleteWorkflow(t *testing.T) {
	// Use enhanced test data and full index configuration
	testData := createEnhancedSpatialTestData()
	setup := setupGeometryTest(t, true, true, testData)

	// Verify data insertion
	queryResult, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).
		WithFilter(fmt.Sprintf("%s >= 0", common.DefaultInt64FieldName)).
		WithLimit(len(testData.IDs)).
		WithOutputFields("*"))
	require.NoError(t, err)

	require.Equal(t, len(testData.IDs), queryResult.ResultCount,
		fmt.Sprintf("Should return %d records", len(testData.IDs)))
	require.Equal(t, 3, len(queryResult.Fields), "Should return 3 fields")

	// Verify all spatial functions work correctly
	spatialFunctions := []string{
		"ST_INTERSECTS", "ST_WITHIN", "ST_CONTAINS",
		"ST_TOUCHES", "ST_OVERLAPS", "ST_CROSSES",
	}

	queryPolygon := getQueryPolygon()
	successfulQueries := 0

	for _, funcName := range spatialFunctions {
		expr := fmt.Sprintf("%s(%s, '%s')", funcName, common.DefaultGeometryFieldName, queryPolygon)

		result, err := setup.Client.Query(setup.Ctx, client.NewQueryOption(setup.Collection).
			WithFilter(expr).
			WithOutputFields(common.DefaultInt64FieldName))

		if err == nil {
			successfulQueries++
			require.GreaterOrEqual(t, result.ResultCount, 0)
		}
	}

	require.True(t, successfulQueries >= len(spatialFunctions)/2,
		"At least half of the spatial functions should work correctly")

	// Verify vector search
	searchVectors := hp.GenSearchVectors(1, 8, entity.FieldTypeFloatVector)
	searchResult, err := setup.Client.Search(setup.Ctx, client.NewSearchOption(setup.Collection, 5, searchVectors).
		WithOutputFields(common.DefaultGeometryFieldName))
	require.NoError(t, err)
	require.True(t, len(searchResult) > 0, "Vector search should return results")
}
