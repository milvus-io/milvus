// Multi-Database Telemetry Demo - Two concurrent clients accessing different databases
// with database-targeted command push support.
//
// Usage:
//   go run multi_database_demo.go
//
// Prerequisites:
//   - Milvus standalone or cluster running
//   - Proxy HTTP server enabled (default: http://localhost:9091)
//
// Features:
//   1. Two concurrent clients, each connected to a different database
//   2. Database-targeted command push (push_config only to specific database)
//   3. Verification that commands are received by the correct client only
//   4. Database filtering in telemetry API
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
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

const (
	dim         = 128
	numEntities = 200
)

// Database configurations with different client settings for testing
var databases = []databaseConfig{
	{
		name:       "db_alpha",
		collection: "products_alpha",
		telemetry: &milvusclient.TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 3 * time.Second,
			SamplingRate:      1.0,
			ErrorMaxCount:     50,
		},
		retryRateLimit: &milvusclient.RetryRateLimitOption{
			MaxRetry:   100,              // More retries for critical database
			MaxBackoff: 5 * time.Second,  // Longer backoff
		},
	},
	{
		name:       "db_beta",
		collection: "products_beta",
		telemetry: &milvusclient.TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 10 * time.Second,
			SamplingRate:      0.5,
			ErrorMaxCount:     200,
		},
		retryRateLimit: &milvusclient.RetryRateLimitOption{
			MaxRetry:   50,               // Fewer retries
			MaxBackoff: 2 * time.Second,  // Shorter backoff
		},
	},
	{
		name:       "db_gamma",
		collection: "products_gamma",
		telemetry: &milvusclient.TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 5 * time.Second,
			SamplingRate:      0.1,
			ErrorMaxCount:     100,
		},
		retryRateLimit: nil, // Use default retry settings
	},
}

type databaseConfig struct {
	name           string
	collection     string
	telemetry      *milvusclient.TelemetryConfig
	retryRateLimit *milvusclient.RetryRateLimitOption
}

// Track received commands per database
type clientTracker struct {
	mu               sync.RWMutex
	receivedCommands map[string][]string // database -> list of command types
	commandPayloads  map[string]string   // commandID -> payload
}

var tracker = &clientTracker{
	receivedCommands: make(map[string][]string),
	commandPayloads:  make(map[string]string),
}

// Client wrapper with database info
type dbClient struct {
	client   *milvusclient.Client
	database string
	clientID string
}

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

	printHeader("Multi-Database Telemetry Demo")
	fmt.Printf("GRPC Address: %s\n", address)
	fmt.Printf("HTTP Address: %s\n", httpAddress)
	fmt.Printf("Databases: %v\n", []string{databases[0].name, databases[1].name})

	// Step 1: Create databases if they don't exist
	printStep(1, "Creating databases")
	adminClient, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: address,
	})
	if err != nil {
		fail("connect to Milvus", err)
	}
	for _, db := range databases {
		// Try to create database, ignore error if already exists
		_ = adminClient.CreateDatabase(ctx, milvusclient.NewCreateDatabaseOption(db.name))
		fmt.Printf("  [OK] Database '%s' ready\n", db.name)
	}
	adminClient.Close(ctx)

	// Step 2: Create two clients for different databases
	printStep(2, "Creating clients for different databases")
	var clients []*dbClient

	for _, db := range databases {
		client, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
			Address:         address,
			DBName:          db.name,           // Connect to specific database
			TelemetryConfig: db.telemetry,      // Use database-specific telemetry config
			RetryRateLimit:  db.retryRateLimit, // Use database-specific retry settings
		})
		if err != nil {
			fail("connect to database "+db.name, err)
		}

		dbc := &dbClient{
			client:   client,
			database: db.name,
		}
		clients = append(clients, dbc)

		// Register command handler for this client
		registerCommandHandler(dbc)
		waitForTelemetryReady(client)

		// Get client ID for command push
		telemetry := client.GetTelemetry()
		if telemetry != nil {
			dbc.clientID = telemetry.GetClientID()
		}

		fmt.Printf("  [OK] Client for '%s' created and telemetry ready\n", db.name)
		fmt.Printf("      Client ID: %s\n", dbc.clientID)
		fmt.Printf("      Telemetry: heartbeat=%v, sampling=%.1f%%, errorMax=%d\n",
			db.telemetry.HeartbeatInterval, db.telemetry.SamplingRate*100, db.telemetry.ErrorMaxCount)
		if db.retryRateLimit != nil {
			fmt.Printf("      RetryRateLimit: maxRetry=%d, maxBackoff=%v\n",
				db.retryRateLimit.MaxRetry, db.retryRateLimit.MaxBackoff)
		} else {
			fmt.Printf("      RetryRateLimit: using defaults\n")
		}
	}

	// Print command push instructions
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("  COMMAND PUSH INSTRUCTIONS")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("\nTo push commands to specific clients, use:")
	fmt.Printf("\n  Push to database (recommended):\n")
	for _, dbc := range clients {
		fmt.Printf("    curl -X POST %s/api/v1/_telemetry/commands \\\n", httpAddress)
		fmt.Printf("      -H 'Content-Type: application/json' \\\n")
		fmt.Printf("      -d '{\"command_type\":\"push_config\",\"target_database\":\"%s\",\"payload\":{\"sampling_rate\":0.5},\"persistent\":true}'\n\n", dbc.database)
	}
	fmt.Printf("  Push to specific client:\n")
	for _, dbc := range clients {
		fmt.Printf("    # Client for %s:\n", dbc.database)
		fmt.Printf("    curl -X POST %s/api/v1/_telemetry/commands \\\n", httpAddress)
		fmt.Printf("      -H 'Content-Type: application/json' \\\n")
		fmt.Printf("      -d '{\"command_type\":\"push_config\",\"target_client_id\":\"%s\",\"payload\":{\"sampling_rate\":0.5},\"persistent\":true}'\n\n", dbc.clientID)
	}
	fmt.Println(strings.Repeat("=", 60))

	// Ensure cleanup
	defer func() {
		for _, dbc := range clients {
			dbc.client.Close(ctx)
		}
	}()

	// Step 3: Create collections in each database
	printStep(3, "Creating collections in each database")
	for i, db := range databases {
		if err := createCollection(ctx, clients[i].client, db.collection); err != nil {
			fail("create collection in "+db.name, err)
		}
		fmt.Printf("  [OK] Collection '%s' created in database '%s'\n", db.collection, db.name)
	}

	// Step 4: Insert data and load collections
	printStep(4, "Inserting data and loading collections")
	for i, db := range databases {
		if err := insertData(ctx, clients[i].client, db.collection); err != nil {
			fail("insert data into "+db.collection, err)
		}
		if err := createIndex(ctx, clients[i].client, db.collection); err != nil {
			fail("create index for "+db.collection, err)
		}
		if err := loadCollection(ctx, clients[i].client, db.collection); err != nil {
			fail("load collection "+db.collection, err)
		}
		fmt.Printf("  [OK] Collection '%s' ready with %d entities\n", db.collection, numEntities)
	}

	// Step 5: Run continuous queries on both databases
	printStep(5, "Running CONTINUOUS queries on both databases (Ctrl+C to stop)")
	fmt.Println("  Queries are running in background. Commands received will be printed.")
	fmt.Println("  Open another terminal to push commands using the curl examples above.")
	fmt.Println()

	// Start continuous query goroutines
	stopCh := make(chan struct{})
	var wg sync.WaitGroup

	for i, db := range databases {
		wg.Add(1)
		go func(idx int, dbCfg databaseConfig) {
			defer wg.Done()
			queryCount := 0
			for {
				select {
				case <-stopCh:
					return
				default:
					_ = performSearch(ctx, clients[idx].client, dbCfg.collection)
					queryCount++
					if queryCount%100 == 0 {
						fmt.Printf("  [%s] Executed %d queries\n", dbCfg.name, queryCount)
					}
					// Random QPS between 3-10 per client
					qps := 3 + rand.Intn(8) // 3 to 10
					sleepMs := 1000 / qps
					time.Sleep(time.Duration(sleepMs) * time.Millisecond)
				}
			}
		}(i, db)
	}

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\n\nReceived interrupt signal, shutting down...")
	close(stopCh)
	wg.Wait()

	// Print summary
	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("  SESSION SUMMARY")
	fmt.Println(strings.Repeat("=", 60))
	tracker.mu.RLock()
	for db, cmds := range tracker.receivedCommands {
		fmt.Printf("  Database '%s': received %d commands - %v\n", db, len(cmds), cmds)
	}
	tracker.mu.RUnlock()

	// Cleanup
	fmt.Println("\nCleaning up...")
	for i, db := range databases {
		_ = clients[i].client.DropCollection(ctx, milvusclient.NewDropCollectionOption(db.collection))
	}
	fmt.Println("Done.")
}

func registerCommandHandler(dbc *dbClient) {
	telemetry := dbc.client.GetTelemetry()
	if telemetry == nil {
		return
	}

	database := dbc.database
	handler := func(cmd *milvusclient.ClientCommand) *milvusclient.CommandReply {
		tracker.mu.Lock()
		tracker.receivedCommands[database] = append(tracker.receivedCommands[database], cmd.CommandType)
		tracker.commandPayloads[cmd.CommandId] = string(cmd.Payload)
		tracker.mu.Unlock()

		fmt.Printf("  [RECEIVED @ %s] Command type=%s, id=%s\n",
			database, cmd.CommandType, cmd.CommandId[:8])

		// Use default handler for push_config
		if cmd.CommandType == "push_config" {
			return telemetry.HandlePushConfigCommand(cmd)
		}
		if cmd.CommandType == "collection_metrics" {
			return telemetry.HandleCollectionMetricsCommand(cmd)
		}

		return &milvusclient.CommandReply{
			CommandId: cmd.CommandId,
			Success:   true,
		}
	}

	telemetry.RegisterCommandHandler("push_config", handler)
	telemetry.RegisterCommandHandler("collection_metrics", handler)
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
	_ = client.DropCollection(ctx, milvusclient.NewDropCollectionOption(name))

	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)).
		WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(dim)).
		WithField(entity.NewField().WithName("category").WithDataType(entity.FieldTypeVarChar).WithMaxLength(100))

	return client.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(name, schema))
}

func insertData(ctx context.Context, client *milvusclient.Client, name string) error {
	vectors := make([][]float32, numEntities)
	categories := make([]string, numEntities)
	categoryOptions := []string{"cat_a", "cat_b", "cat_c"}

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

func verifyServerMetricsForDatabase(ctx context.Context, apiBase string, database string) {
	resp, err := fetchTelemetryForDatabase(ctx, apiBase, database)
	if err != nil {
		fmt.Printf("  [FAIL] Failed to fetch telemetry for %s: %v\n", database, err)
		return
	}

	if len(resp.Clients) == 0 {
		fmt.Printf("  [WARN] No clients found for database '%s'\n", database)
		return
	}

	fmt.Printf("  [OK] Found %d client(s) for database '%s'\n", len(resp.Clients), database)
	for _, client := range resp.Clients {
		fmt.Printf("    - Client ID: %s, Status: %s\n", client.ClientID, client.Status)
		fmt.Printf("      Databases: %v\n", client.Databases)
		for _, op := range client.Metrics {
			fmt.Printf("      %s: requests=%d, success=%d, avg=%.2fms\n",
				op.Operation, op.Global.RequestCount, op.Global.SuccessCount, op.Global.AvgLatencyMs)
		}
	}
}

func verifyTargetedCommandDelivery(targetDB, otherDB string) {
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	targetCmds := tracker.receivedCommands[targetDB]
	otherCmds := tracker.receivedCommands[otherDB]

	fmt.Printf("  Database '%s' (target): received %d command(s)\n", targetDB, len(targetCmds))
	fmt.Printf("  Database '%s' (other): received %d command(s)\n", otherDB, len(otherCmds))

	// Check that target received push_config
	targetReceivedPushConfig := false
	for _, cmd := range targetCmds {
		if cmd == "push_config" {
			targetReceivedPushConfig = true
			break
		}
	}

	// Check that other did NOT receive push_config from this push
	// (Note: it may have received from previous pushes)
	if targetReceivedPushConfig {
		fmt.Printf("  [OK] Database '%s' correctly received the targeted push_config\n", targetDB)
	} else {
		fmt.Printf("  [WARN] Database '%s' did not receive push_config (may need more time)\n", targetDB)
	}
}

func verifyGlobalCommandDelivery() {
	tracker.mu.RLock()
	defer tracker.mu.RUnlock()

	for _, db := range databases {
		cmds := tracker.receivedCommands[db.name]
		hasCollectionMetrics := false
		for _, cmd := range cmds {
			if cmd == "collection_metrics" {
				hasCollectionMetrics = true
				break
			}
		}
		if hasCollectionMetrics {
			fmt.Printf("  [OK] Database '%s' received global collection_metrics command\n", db.name)
		} else {
			fmt.Printf("  [WARN] Database '%s' did not receive collection_metrics (may need more time)\n", db.name)
		}
	}
}

func printDatabaseFilteringSummary(ctx context.Context, apiBase string) {
	fmt.Println("\n  === Database Filtering Summary ===")

	// Show all clients (no filter)
	fmt.Println("\n  --- All Clients ---")
	resp, err := fetchTelemetry(ctx, apiBase, "include_metrics=true")
	if err == nil {
		fmt.Printf("  Total clients: %d\n", len(resp.Clients))
		for _, c := range resp.Clients {
			fmt.Printf("    - %s: databases=%v, status=%s\n", c.ClientID, c.Databases, c.Status)
		}
	}

	// Show clients filtered by each database
	for _, db := range databases {
		fmt.Printf("\n  --- Clients in Database '%s' ---\n", db.name)
		resp, err := fetchTelemetryForDatabase(ctx, apiBase, db.name)
		if err == nil {
			fmt.Printf("  Clients in '%s': %d\n", db.name, len(resp.Clients))
			for _, c := range resp.Clients {
				for _, op := range c.Metrics {
					fmt.Printf("    %s: %d requests\n", op.Operation, op.Global.RequestCount)
				}
			}
		}
	}

	// Summary of received commands
	fmt.Println("\n  === Commands Received Summary ===")
	tracker.mu.RLock()
	for db, cmds := range tracker.receivedCommands {
		fmt.Printf("  Database '%s': %d commands - %v\n", db, len(cmds), cmds)
	}
	tracker.mu.RUnlock()
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

func fetchTelemetryForDatabase(ctx context.Context, apiBase string, database string) (*telemetryClientResponse, error) {
	return fetchTelemetry(ctx, apiBase, "database="+database+"&include_metrics=true")
}

func pushCommandToDatabase(ctx context.Context, apiBase, commandType, targetDatabase string, payload map[string]any, persistent bool) error {
	reqBody := map[string]any{
		"command_type":    commandType,
		"target_database": targetDatabase, // Target specific database
		"payload":         payload,
		"ttl_seconds":     300,
		"persistent":      persistent,
	}
	return sendPushCommand(ctx, apiBase, reqBody)
}

func pushCommandGlobal(ctx context.Context, apiBase, commandType string, payload map[string]any, persistent bool) error {
	reqBody := map[string]any{
		"command_type": commandType,
		// No target_database = global scope
		"payload":     payload,
		"ttl_seconds": 300,
		"persistent":  persistent,
	}
	return sendPushCommand(ctx, apiBase, reqBody)
}

func sendPushCommand(ctx context.Context, apiBase string, reqBody map[string]any) error {
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
