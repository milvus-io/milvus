// Telemetry Demo - end-to-end telemetry flow with HTTP API and WebUI.
//
// Usage:
//   go run main.go
//
// Prerequisites:
//   - Milvus standalone or cluster running
//   - Proxy HTTP server enabled (default: http://localhost:9091)
//
// Environment:
//   - MILVUS_ADDRESS (default: localhost:19530)
//   - MILVUS_HTTP_ADDRESS (default: http://localhost:9091)
//
// Features:
//   1. Multiple collections with different schemas
//   2. Random requests to different collections during heartbeat
//   3. Client-side metrics push verification
//   4. Rich filtering dimensions (collection, operation, database)
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

const (
	dim         = 128
	numEntities = 500
)

// Collections to create for the demo
var collections = []collectionConfig{
	{name: "products", category: "ecommerce", categories: []string{"electronics", "clothing", "food", "books", "sports"}},
	{name: "users", category: "profile", categories: []string{"active", "inactive", "premium", "trial", "admin"}},
	{name: "logs", category: "system", categories: []string{"error", "warn", "info", "debug", "trace"}},
}

type collectionConfig struct {
	name       string
	category   string
	categories []string
}

// Track received push_config commands
var (
	receivedPushConfig    atomic.Int32
	lastPushConfigPayload atomic.Value
)

type telemetryClientResponse struct {
	Clients    []telemetryClient `json:"clients"`
	Aggregated *telemetryMetrics `json:"aggregated,omitempty"`
}

type telemetryClient struct {
	ClientID   string               `json:"client_id"`
	ClientInfo telemetryClientInfo  `json:"client_info"`
	Metrics    []telemetryOperation `json:"metrics,omitempty"`
	Databases  []string             `json:"databases,omitempty"`
	Status     string               `json:"status"`
}

type telemetryClientInfo struct {
	Host     string            `json:"host"`
	SdkType  string            `json:"sdk_type"`
	Reserved map[string]string `json:"reserved,omitempty"`
}

type telemetryOperation struct {
	Operation         string                      `json:"operation"`
	Global            telemetryMetrics            `json:"global"`
	CollectionMetrics map[string]telemetryMetrics `json:"collection_metrics,omitempty"`
}

type telemetryMetrics struct {
	RequestCount int64   `json:"request_count"`
	SuccessCount int64   `json:"success_count"`
	ErrorCount   int64   `json:"error_count"`
	AvgLatencyMs float64 `json:"avg_latency_ms"`
	P99LatencyMs float64 `json:"p99_latency_ms"`
}

func main() {
	ctx := context.Background()

	address := getenvDefault("MILVUS_ADDRESS", "localhost:19530")
	httpAddress := getenvDefault("MILVUS_HTTP_ADDRESS", "http://localhost:9091")
	httpAddress = strings.TrimRight(httpAddress, "/")

	printHeader("Milvus Client Telemetry E2E Demo")
	fmt.Printf("GRPC Address: %s\n", address)
	fmt.Printf("HTTP Address: %s\n", httpAddress)
	fmt.Printf("Collections: %d\n", len(collections))

	// Step 1: Create client with telemetry enabled
	printStep(1, "Creating Milvus client with telemetry enabled")
	client, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: address,
		TelemetryConfig: &milvusclient.TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 5 * time.Second,
			SamplingRate:      1.0,
			ErrorMaxCount:     100,
		},
	})
	if err != nil {
		fail("connect to Milvus", err)
	}
	defer client.Close(ctx)
	fmt.Println("  [OK] Client created successfully")

	// Register command handlers to track push_config
	registerCommandHandlers(client)
	waitForTelemetryReady(client)
	fmt.Println("  [OK] Telemetry is ready")

	// Step 2: Create multiple collections with schemas
	printStep(2, "Creating multiple collections")
	for _, coll := range collections {
		_ = client.DropCollection(ctx, milvusclient.NewDropCollectionOption(coll.name))
		if err := createCollection(ctx, client, coll.name); err != nil {
			fail("create collection "+coll.name, err)
		}
		fmt.Printf("  [OK] Collection '%s' created\n", coll.name)
	}

	// Step 3: Insert data into all collections
	printStep(3, "Inserting data into collections")
	for _, coll := range collections {
		if err := insertData(ctx, client, coll.name, coll.categories); err != nil {
			fail("insert data into "+coll.name, err)
		}
		fmt.Printf("  [OK] Inserted %d entities into '%s'\n", numEntities, coll.name)
	}

	// Step 4: Create index and load all collections
	printStep(4, "Creating indexes and loading collections")
	for _, coll := range collections {
		if err := createIndex(ctx, client, coll.name); err != nil {
			fail("create index for "+coll.name, err)
		}
		if err := loadCollection(ctx, client, coll.name); err != nil {
			fail("load collection "+coll.name, err)
		}
		fmt.Printf("  [OK] '%s' indexed and loaded\n", coll.name)
	}

	// Step 5: Execute diverse operations across all collections
	printStep(5, "Executing search and query operations across collections")
	totalSearches := 30
	totalQueries := 15
	for i := 0; i < totalSearches; i++ {
		// Random collection for each search
		coll := collections[rand.Intn(len(collections))]
		_ = performSearch(ctx, client, coll.name)
	}
	fmt.Printf("  [OK] Executed %d random searches across collections\n", totalSearches)

	for i := 0; i < totalQueries; i++ {
		// Random collection for each query
		coll := collections[rand.Intn(len(collections))]
		_ = performQuery(ctx, client, coll.name)
	}
	fmt.Printf("  [OK] Executed %d random queries across collections\n", totalQueries)

	// Wait for heartbeat to send metrics
	fmt.Println("  Waiting for heartbeat cycle (8 seconds)...")
	waitForHeartbeat()

	// Step 6: Verify server-side metrics with different filters
	printStep(6, "Verifying server-side metrics with filters")
	verifyServerMetrics(ctx, httpAddress)

	// Step 7: Enable collection-level metrics for specific collections
	printStep(7, "Enabling collection-level metrics")
	collNames := make([]string, len(collections))
	for i, coll := range collections {
		collNames[i] = coll.name
	}
	if err := pushCommand(ctx, httpAddress, "collection_metrics", map[string]any{
		"enabled":     true,
		"collections": collNames,
	}, 300, false); err != nil {
		fmt.Printf("  [WARN] Failed to push collection_metrics: %v\n", err)
	} else {
		fmt.Printf("  [OK] collection_metrics enabled for: %v\n", collNames)
	}

	// Execute more operations to generate collection-level metrics
	fmt.Println("  Executing more operations for collection-level metrics...")
	for i := 0; i < 20; i++ {
		coll := collections[rand.Intn(len(collections))]
		_ = performSearch(ctx, client, coll.name)
	}

	// Wait for heartbeat
	fmt.Println("  Waiting for heartbeat cycle (8 seconds)...")
	waitForHeartbeat()

	// Step 8: Verify collection-level metrics
	printStep(8, "Verifying collection-level metrics")
	for _, coll := range collections {
		verifyCollectionMetrics(ctx, httpAddress, coll.name)
	}

	// Step 9: Test push_config command
	printStep(9, "Testing push_config command")
	testPushConfig(ctx, httpAddress)

	// Step 10: Demonstrate rich filtering capabilities
	printStep(10, "Demonstrating rich filtering API")
	demonstrateFiltering(ctx, httpAddress)

	// Step 11: Show client-side metrics snapshot
	printStep(11, "Client-side metrics snapshot")
	showClientSideMetrics(client)

	// Step 12: Test Historical View (aggregated snapshot query)
	printStep(12, "Testing Historical View / Aggregated Snapshots")
	testHistoricalView(ctx, client, httpAddress)

	// Step 13: Final summary
	printStep(13, "Test Summary")
	printSummary(ctx, httpAddress)

	// Cleanup
	fmt.Println("\nCleaning up...")
	for _, coll := range collections {
		_ = client.DropCollection(ctx, milvusclient.NewDropCollectionOption(coll.name))
	}
	fmt.Println("Done.")
}

func registerCommandHandlers(client *milvusclient.Client) {
	telemetry := client.GetTelemetry()
	if telemetry == nil {
		return
	}

	// Track push_config commands received by client
	telemetry.RegisterCommandHandler("push_config", func(cmd *milvusclient.ClientCommand) *milvusclient.CommandReply {
		receivedPushConfig.Add(1)
		lastPushConfigPayload.Store(string(cmd.Payload))
		fmt.Printf("  [RECEIVED] push_config command: %s\n", string(cmd.Payload))
		// Use default handler to apply the config
		return telemetry.HandlePushConfigCommand(cmd)
	})
}

func waitForTelemetryReady(client *milvusclient.Client) {
	telemetry := client.GetTelemetry()
	if telemetry == nil {
		return
	}
	for i := 0; i < 40; i++ {
		if telemetry.IsReady() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func waitForHeartbeat() {
	time.Sleep(8 * time.Second)
}

func createCollection(ctx context.Context, client *milvusclient.Client, name string) error {
	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(dim)).
		WithField(entity.NewField().WithName("category").WithDataType(entity.FieldTypeVarChar).WithMaxLength(100))

	return client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(name, schema))
}

func insertData(ctx context.Context, client *milvusclient.Client, name string, categoryOptions []string) error {
	vectors := make([][]float32, numEntities)
	categories := make([]string, numEntities)

	for i := 0; i < numEntities; i++ {
		vec := make([]float32, dim)
		for j := 0; j < dim; j++ {
			vec[j] = rand.Float32()
		}
		vectors[i] = vec
		categories[i] = categoryOptions[rand.Intn(len(categoryOptions))]
	}

	_, err := client.Insert(ctx, milvusclient.NewColumnBasedInsertOption(name).
		WithFloatVectorColumn("vector", dim, vectors).
		WithVarcharColumn("category", categories))
	return err
}

func createIndex(ctx context.Context, client *milvusclient.Client, name string) error {
	idx := index.NewHNSWIndex(entity.L2, 16, 64)
	task, err := client.CreateIndex(ctx, milvusclient.NewCreateIndexOption(name, "vector", idx))
	if err != nil {
		return err
	}
	return task.Await(ctx)
}

func loadCollection(ctx context.Context, client *milvusclient.Client, name string) error {
	loadTask, err := client.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(name))
	if err != nil {
		return err
	}
	return loadTask.Await(ctx)
}

func performSearch(ctx context.Context, client *milvusclient.Client, collName string) error {
	queryVec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		queryVec[i] = rand.Float32()
	}

	_, err := client.Search(ctx, milvusclient.NewSearchOption(collName, 10, []entity.Vector{
		entity.FloatVector(queryVec),
	}))
	return err
}

func performQuery(ctx context.Context, client *milvusclient.Client, collName string) error {
	_, err := client.Query(ctx, milvusclient.NewQueryOption(collName).
		WithFilter("id > 0").
		WithLimit(10))
	return err
}

func verifyServerMetrics(ctx context.Context, apiBase string) {
	resp, err := fetchTelemetry(ctx, apiBase, "")
	if err != nil {
		fmt.Printf("  [FAIL] Failed to fetch telemetry: %v\n", err)
		return
	}

	if len(resp.Clients) == 0 {
		fmt.Println("  [WARN] No clients found in telemetry")
		return
	}

	fmt.Printf("  [OK] Found %d client(s)\n", len(resp.Clients))

	// Check aggregated metrics
	if resp.Aggregated != nil {
		fmt.Printf("  [OK] Aggregated metrics: requests=%d, success=%d, errors=%d\n",
			resp.Aggregated.RequestCount, resp.Aggregated.SuccessCount, resp.Aggregated.ErrorCount)
	}

	// Check per-operation metrics
	client := resp.Clients[0]
	fmt.Printf("  Client ID: %s\n", client.ClientID)
	fmt.Printf("  Status: %s\n", client.Status)

	operationsFound := make(map[string]bool)
	expectedOps := []string{"Search", "Query"}

	for _, op := range client.Metrics {
		operationsFound[op.Operation] = true
		fmt.Printf("  [OK] %s: requests=%d, success=%d, errors=%d, avg_latency=%.2fms, p99_latency=%.2fms\n",
			op.Operation, op.Global.RequestCount, op.Global.SuccessCount, op.Global.ErrorCount,
			op.Global.AvgLatencyMs, op.Global.P99LatencyMs)
	}

	// Verify expected operations are present
	for _, expected := range expectedOps {
		if !operationsFound[expected] {
			fmt.Printf("  [WARN] Expected operation '%s' not found in metrics\n", expected)
		}
	}
}

func verifyCollectionMetrics(ctx context.Context, apiBase string, collection string) {
	resp, err := fetchTelemetry(ctx, apiBase, "")
	if err != nil {
		fmt.Printf("  [FAIL] Failed to fetch telemetry: %v\n", err)
		return
	}

	if len(resp.Clients) == 0 {
		fmt.Println("  [WARN] No clients found")
		return
	}

	client := resp.Clients[0]
	collectionMetricsFound := false

	for _, op := range client.Metrics {
		if op.CollectionMetrics != nil {
			if metrics, ok := op.CollectionMetrics[collection]; ok {
				collectionMetricsFound = true
				fmt.Printf("  [OK] %s [%s]: requests=%d, success=%d, avg=%.2fms, p99=%.2fms\n",
					op.Operation, collection, metrics.RequestCount, metrics.SuccessCount,
					metrics.AvgLatencyMs, metrics.P99LatencyMs)
			}
		}
	}

	if !collectionMetricsFound {
		fmt.Printf("  [INFO] '%s': no collection metrics yet (may need more heartbeats)\n", collection)
	}
}

func testPushConfig(ctx context.Context, apiBase string) {
	// Reset counter
	receivedPushConfig.Store(0)

	// Push a config change
	newSamplingRate := 0.8
	newHeartbeatMs := int64(3000)

	fmt.Printf("  Pushing config: sampling_rate=%.1f, heartbeat_interval=%dms\n", newSamplingRate, newHeartbeatMs)

	if err := pushCommand(ctx, apiBase, "push_config", map[string]any{
		"sampling_rate":         newSamplingRate,
		"heartbeat_interval_ms": newHeartbeatMs,
	}, 0, true); err != nil {
		fmt.Printf("  [FAIL] Failed to push push_config: %v\n", err)
		return
	}
	fmt.Println("  [OK] push_config command pushed")

	// Wait for client to receive the command
	fmt.Println("  Waiting for client to receive command (10 seconds)...")
	time.Sleep(10 * time.Second)

	// Verify client received the command
	count := receivedPushConfig.Load()
	if count > 0 {
		fmt.Printf("  [OK] Client received push_config command (%d times)\n", count)
		if payload, ok := lastPushConfigPayload.Load().(string); ok {
			fmt.Printf("  [OK] Last payload: %s\n", payload)
		}
	} else {
		fmt.Println("  [WARN] Client did not receive push_config command (may need more time)")
	}
}

func demonstrateFiltering(ctx context.Context, apiBase string) {
	fmt.Println("  Demonstrating rich filtering capabilities:")

	// 1. Filter by include_metrics
	fmt.Println("\n  --- Filter: include_metrics=true ---")
	resp, err := fetchTelemetry(ctx, apiBase, "include_metrics=true")
	if err == nil && len(resp.Clients) > 0 {
		client := resp.Clients[0]
		fmt.Printf("  Client %s has %d operation metrics\n", client.ClientID, len(client.Metrics))
		for _, op := range client.Metrics {
			fmt.Printf("    - %s: %d requests\n", op.Operation, op.Global.RequestCount)
			if len(op.CollectionMetrics) > 0 {
				for coll, m := range op.CollectionMetrics {
					fmt.Printf("      [%s]: %d requests\n", coll, m.RequestCount)
				}
			}
		}
	}

	// 2. Filter without metrics (lightweight)
	fmt.Println("\n  --- Filter: include_metrics=false (lightweight) ---")
	resp2, err := fetchTelemetry(ctx, apiBase, "include_metrics=false")
	if err == nil && len(resp2.Clients) > 0 {
		fmt.Printf("  Found %d clients (metrics excluded for performance)\n", len(resp2.Clients))
		for _, c := range resp2.Clients {
			fmt.Printf("    - Client: %s, SDK: %s, Status: %s\n",
				c.ClientID, c.ClientInfo.SdkType, c.Status)
		}
	}

	// 3. Filter by specific client_id
	if len(resp.Clients) > 0 {
		clientID := resp.Clients[0].ClientID
		fmt.Printf("\n  --- Filter: client_id=%s ---\n", clientID)
		resp3, err := fetchTelemetry(ctx, apiBase, "client_id="+clientID+"&include_metrics=true")
		if err == nil && len(resp3.Clients) > 0 {
			fmt.Printf("  Found specific client: %s\n", resp3.Clients[0].ClientID)
		}
	}

	fmt.Println("\n  [OK] Filtering demonstration complete")
}

func showClientSideMetrics(client *milvusclient.Client) {
	telemetry := client.GetTelemetry()
	if telemetry == nil {
		fmt.Println("  [WARN] Telemetry not available")
		return
	}

	fmt.Println("  Client-side metrics snapshot (before push to server):")

	snapshot := telemetry.GetLatestSnapshot()
	if snapshot == nil || len(snapshot.Metrics) == 0 {
		fmt.Println("  [INFO] No metrics collected yet (metrics are reset after each heartbeat)")
		return
	}

	for _, opMetrics := range snapshot.Metrics {
		fmt.Printf("  - %s:\n", opMetrics.Operation)
		fmt.Printf("      Global: requests=%d, success=%d, errors=%d\n",
			opMetrics.Global.RequestCount, opMetrics.Global.SuccessCount, opMetrics.Global.ErrorCount)
		fmt.Printf("      Latency: avg=%.2fms, p99=%.2fms\n",
			opMetrics.Global.AvgLatencyMs, opMetrics.Global.P99LatencyMs)
		if len(opMetrics.CollectionMetrics) > 0 {
			fmt.Println("      Per-collection:")
			for coll, m := range opMetrics.CollectionMetrics {
				fmt.Printf("        [%s]: requests=%d, avg=%.2fms\n", coll, m.RequestCount, m.AvgLatencyMs)
			}
		}
	}

	// Also show recent errors if any
	errors := telemetry.GetRecentErrors(5)
	if len(errors) > 0 {
		fmt.Printf("\n  Recent errors (%d):\n", len(errors))
		for _, e := range errors {
			fmt.Printf("    - [%s] %s: %s\n", e.Operation, e.Collection, e.ErrorMsg)
		}
	}
}

func testHistoricalView(ctx context.Context, client *milvusclient.Client, apiBase string) {
	telemetry := client.GetTelemetry()
	if telemetry == nil {
		fmt.Println("  [WARN] Telemetry not available")
		return
	}

	// Test 1: Client-side snapshot history (all stored snapshots)
	fmt.Println("  Testing client-side snapshot history...")
	snapshots := telemetry.GetMetricsSnapshots()
	fmt.Printf("  [OK] Found %d snapshots in client-side history\n", len(snapshots))

	if len(snapshots) > 0 {
		oldest := snapshots[0]
		newest := snapshots[len(snapshots)-1]
		fmt.Printf("  Snapshot time range: %v to %v\n",
			time.Unix(0, oldest.Timestamp*int64(time.Millisecond)).Format(time.RFC3339),
			time.Unix(0, newest.Timestamp*int64(time.Millisecond)).Format(time.RFC3339))
	}

	// Test 2: Client-side latency history for time range
	fmt.Println("\n  Testing client-side latency history (time range filter)...")
	endTime := time.Now()
	startTime := endTime.Add(-5 * time.Minute) // Last 5 minutes

	filteredSnapshots := telemetry.GetHistoricalLatency(startTime, endTime)
	fmt.Printf("  [OK] Found %d snapshots in time range [%s, %s]\n",
		len(filteredSnapshots),
		startTime.Format(time.RFC3339),
		endTime.Format(time.RFC3339))

	if len(filteredSnapshots) > 0 {
		// Print summary of each snapshot
		for i, snap := range filteredSnapshots {
			if i >= 3 { // Limit output
				fmt.Printf("  ... and %d more snapshots\n", len(filteredSnapshots)-3)
				break
			}
			ts := time.Unix(0, snap.Timestamp*int64(time.Millisecond)).Format(time.RFC3339)
			var totalRequests int64
			for _, op := range snap.Metrics {
				totalRequests += op.Global.RequestCount
			}
			fmt.Printf("    - Snapshot %d [%s]: %d operations, %d total requests\n",
				i+1, ts, len(snap.Metrics), totalRequests)
		}
	}

	// Test 3: HTTP API for history (command-based, with aggregation)
	fmt.Println("\n  Testing HTTP API for historical data (aggregated)...")
	resp, err := fetchTelemetry(ctx, apiBase, "include_metrics=true")
	if err != nil {
		fmt.Printf("  [WARN] Failed to fetch client list: %v\n", err)
		return
	}

	if len(resp.Clients) == 0 {
		fmt.Println("  [WARN] No clients found")
		return
	}

	clientID := resp.Clients[0].ClientID
	historyURL := fmt.Sprintf("%s/api/v1/_telemetry/clients/%s/history?start_time=%s&end_time=%s&aggregate=true",
		apiBase, clientID, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, historyURL, nil)
	if err != nil {
		fmt.Printf("  [WARN] Failed to create history request: %v\n", err)
		return
	}

	histResp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Printf("  [WARN] Failed to call history endpoint: %v\n", err)
		return
	}
	defer histResp.Body.Close()

	body, _ := io.ReadAll(histResp.Body)
	if histResp.StatusCode == http.StatusOK {
		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err == nil {
			fmt.Printf("  [OK] History API response: %v\n", result)
		}
	} else {
		// The history API uses command/reply pattern - it sends a command and returns a command_id
		// The actual results come back via the client's next heartbeat
		var result map[string]interface{}
		if err := json.Unmarshal(body, &result); err == nil {
			if cmdID, ok := result["command_id"]; ok {
				fmt.Printf("  [OK] History command created: command_id=%v\n", cmdID)
				fmt.Println("  Note: Results will be available in client's next heartbeat response")
			} else {
				fmt.Printf("  [INFO] History API returned %d: %s\n", histResp.StatusCode, strings.TrimSpace(string(body)))
			}
		}
	}

	fmt.Println("\n  [OK] Historical View test complete")
}

func printSummary(ctx context.Context, apiBase string) {
	resp, err := fetchTelemetry(ctx, apiBase, "include_metrics=true")
	if err != nil {
		fmt.Printf("  [FAIL] Cannot fetch final telemetry: %v\n", err)
		return
	}

	fmt.Println("\n  ============ Final Telemetry Summary ============")

	if len(resp.Clients) > 0 {
		client := resp.Clients[0]
		fmt.Printf("  Client ID: %s\n", client.ClientID)
		fmt.Printf("  Status: %s\n", client.Status)
		fmt.Printf("  SDK Type: %s\n", client.ClientInfo.SdkType)

		fmt.Println("\n  Operation Metrics:")
		for _, op := range client.Metrics {
			fmt.Printf("    - %s:\n", op.Operation)
			fmt.Printf("        Global: requests=%d, success=%d, errors=%d\n",
				op.Global.RequestCount, op.Global.SuccessCount, op.Global.ErrorCount)
			fmt.Printf("        Latency: avg=%.2fms, p99=%.2fms\n",
				op.Global.AvgLatencyMs, op.Global.P99LatencyMs)

			if len(op.CollectionMetrics) > 0 {
				fmt.Println("        Collection Metrics:")
				for coll, m := range op.CollectionMetrics {
					fmt.Printf("          - %s: requests=%d, success=%d, avg=%.2fms, p99=%.2fms\n",
						coll, m.RequestCount, m.SuccessCount, m.AvgLatencyMs, m.P99LatencyMs)
				}
			}
		}
	}

	if resp.Aggregated != nil {
		fmt.Printf("\n  Aggregated: total_requests=%d, total_success=%d, total_errors=%d\n",
			resp.Aggregated.RequestCount, resp.Aggregated.SuccessCount, resp.Aggregated.ErrorCount)
	}

	pushConfigCount := receivedPushConfig.Load()
	fmt.Printf("\n  Push Config Commands Received: %d\n", pushConfigCount)

	fmt.Println("  =================================================")
}

func fetchTelemetry(ctx context.Context, apiBase string, queryParams string) (*telemetryClientResponse, error) {
	url := apiBase + "/api/v1/_telemetry/clients"
	if queryParams != "" {
		url += "?" + queryParams
	} else {
		url += "?include_metrics=true"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var decoded telemetryClientResponse
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	return &decoded, nil
}

func pushCommand(ctx context.Context, apiBase, commandType string, payload map[string]any, ttlSeconds int64, persistent bool) error {
	reqBody := map[string]any{
		"command_type":     commandType,
		"target_client_id": "",
		"payload":          payload,
		"ttl_seconds":      ttlSeconds,
		"persistent":       persistent,
	}
	data, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiBase+"/api/v1/_telemetry/commands", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("http %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func getenvDefault(key, value string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return value
}

func fail(step string, err error) {
	fmt.Printf("  [FAIL] Failed to %s: %v\n", step, err)
	os.Exit(1)
}

func printHeader(title string) {
	line := strings.Repeat("=", 60)
	fmt.Printf("\n%s\n  %s\n%s\n\n", line, title, line)
}

func printStep(num int, description string) {
	fmt.Printf("\n[Step %d] %s\n", num, description)
}
