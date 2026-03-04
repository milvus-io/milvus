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

//go:build ignore

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/index"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

const (
	milvusAddr  = "localhost:19530"
	dim         = 128
	numRows     = 500
	numSearches = 30
)

// Global reference to telemetry manager for custom handlers
var telemetryManager *milvusclient.ClientTelemetryManager

// Multiple collections for diverse telemetry data
var collections = []collectionDef{
	{name: "telemetry_products", categories: []string{"electronics", "clothing", "food", "books", "sports"}},
	{name: "telemetry_users", categories: []string{"active", "inactive", "premium", "trial", "admin"}},
	{name: "telemetry_logs", categories: []string{"error", "warn", "info", "debug", "trace"}},
}

type collectionDef struct {
	name       string
	categories []string
}

func main() {
	ctx := context.Background()

	fmt.Println("=== Milvus Telemetry WebUI Demo ===")
	fmt.Printf("Collections: %d\n", len(collections))
	fmt.Println()

	// Connect with telemetry enabled
	fmt.Println("[1] Connecting to Milvus with telemetry enabled...")
	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
		TelemetryConfig: &milvusclient.TelemetryConfig{
			Enabled:           true,
			HeartbeatInterval: 5 * time.Second,
			SamplingRate:      1.0,
		},
	})
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}
	defer cli.Close(ctx)
	fmt.Println("    Connected successfully!")

	// Register custom command handlers to print when commands are received
	telemetryManager = cli.GetTelemetry()
	if telemetryManager != nil {
		// Wrap push_config handler to print received config
		telemetryManager.RegisterCommandHandler("push_config", func(cmd *milvusclient.ClientCommand) *milvusclient.CommandReply {
			fmt.Printf("\n>>> [RECEIVED push_config] CommandID=%s, Persistent=%v\n", cmd.CommandId, cmd.Persistent)
			if len(cmd.Payload) > 0 {
				var payload map[string]interface{}
				if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
					fmt.Printf("    Config payload: %v\n", payload)
				} else {
					fmt.Printf("    Config payload (raw): %s\n", string(cmd.Payload))
				}
			} else {
				fmt.Println("    Config payload: (empty)")
			}
			// Call the original handler
			return telemetryManager.HandlePushConfigCommand(cmd)
		})

		// Wrap collection_metrics handler to print received command
		telemetryManager.RegisterCommandHandler("collection_metrics", func(cmd *milvusclient.ClientCommand) *milvusclient.CommandReply {
			fmt.Printf("\n>>> [RECEIVED collection_metrics] CommandID=%s\n", cmd.CommandId)
			if len(cmd.Payload) > 0 {
				var payload map[string]interface{}
				if err := json.Unmarshal(cmd.Payload, &payload); err == nil {
					fmt.Printf("    Payload: %v\n", payload)
				} else {
					fmt.Printf("    Payload (raw): %s\n", string(cmd.Payload))
				}
			} else {
				fmt.Println("    Payload: (empty - will return current enabled collections)")
			}
			// Call the original handler
			return telemetryManager.HandleCollectionMetricsCommand(cmd)
		})

		// Wrap show_errors handler to print received command
		telemetryManager.RegisterCommandHandler("show_errors", func(cmd *milvusclient.ClientCommand) *milvusclient.CommandReply {
			fmt.Printf("\n>>> [RECEIVED show_errors] CommandID=%s\n", cmd.CommandId)
			if len(cmd.Payload) > 0 {
				fmt.Printf("    Payload: %s\n", string(cmd.Payload))
			}
			// Call the original handler
			return telemetryManager.HandleShowErrorsCommand(cmd)
		})

		fmt.Println("    Custom command handlers registered!")
	}
	fmt.Println()

	// Create multiple collections
	fmt.Println("[2] Preparing collections...")
	for _, coll := range collections {
		cli.DropCollection(ctx, milvusclient.NewDropCollectionOption(coll.name))

		schema := entity.NewSchema().
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true).WithIsAutoID(true)).
			WithField(entity.NewField().WithName("vector").WithDataType(entity.FieldTypeFloatVector).WithDim(dim)).
			WithField(entity.NewField().WithName("category").WithDataType(entity.FieldTypeVarChar).WithMaxLength(100))

		err = cli.CreateCollection(ctx, milvusclient.NewCreateCollectionOption(coll.name, schema))
		if err != nil {
			fmt.Printf("Failed to create collection %s: %v\n", coll.name, err)
			return
		}
		fmt.Printf("    Collection '%s' created!\n", coll.name)
	}
	fmt.Println()

	// Insert data into all collections
	fmt.Printf("[3] Inserting %d rows into each collection...\n", numRows)
	for _, coll := range collections {
		vectors := make([][]float32, numRows)
		categoryCol := make([]string, numRows)
		for i := 0; i < numRows; i++ {
			vectors[i] = randomVector(dim)
			categoryCol[i] = coll.categories[rand.Intn(len(coll.categories))]
		}

		insertResult, err := cli.Insert(ctx, milvusclient.NewColumnBasedInsertOption(coll.name).
			WithFloatVectorColumn("vector", dim, vectors).
			WithVarcharColumn("category", categoryCol))
		if err != nil {
			fmt.Printf("Failed to insert into %s: %v\n", coll.name, err)
			return
		}
		fmt.Printf("    Inserted %d rows into '%s'\n", insertResult.InsertCount, coll.name)
	}
	fmt.Println()

	// Create index for all collections
	fmt.Println("[4] Creating indexes...")
	for _, coll := range collections {
		indexTask, err := cli.CreateIndex(ctx, milvusclient.NewCreateIndexOption(coll.name, "vector",
			index.NewHNSWIndex(entity.COSINE, 16, 200)))
		if err != nil {
			fmt.Printf("Failed to create index for %s: %v\n", coll.name, err)
			return
		}
		err = indexTask.Await(ctx)
		if err != nil {
			fmt.Printf("Failed to wait for index on %s: %v\n", coll.name, err)
			return
		}
		fmt.Printf("    Index created for '%s'\n", coll.name)
	}
	fmt.Println()

	// Load all collections
	fmt.Println("[5] Loading collections...")
	for _, coll := range collections {
		loadTask, err := cli.LoadCollection(ctx, milvusclient.NewLoadCollectionOption(coll.name))
		if err != nil {
			fmt.Printf("Failed to load %s: %v\n", coll.name, err)
			return
		}
		err = loadTask.Await(ctx)
		if err != nil {
			fmt.Printf("Failed to wait for load on %s: %v\n", coll.name, err)
			return
		}
		fmt.Printf("    Collection '%s' loaded!\n", coll.name)
	}
	fmt.Println()

	// Perform searches across random collections
	fmt.Printf("[6] Performing %d searches across collections...\n", numSearches)
	for i := 0; i < numSearches; i++ {
		// Pick a random collection
		coll := collections[rand.Intn(len(collections))]
		queryVector := randomVector(dim)
		_, err := cli.Search(ctx, milvusclient.NewSearchOption(coll.name, 10,
			[]entity.Vector{entity.FloatVector(queryVector)}).
			WithOutputFields("category"))
		if err != nil {
			fmt.Printf("    Search %d on %s failed: %v\n", i+1, coll.name, err)
		}
		if (i+1)%10 == 0 {
			fmt.Printf("    Completed %d/%d searches\n", i+1, numSearches)
		}
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Println()

	// Perform queries across random collections
	fmt.Println("[7] Performing queries across collections...")
	for i := 0; i < 15; i++ {
		coll := collections[rand.Intn(len(collections))]
		category := coll.categories[rand.Intn(len(coll.categories))]
		_, err := cli.Query(ctx, milvusclient.NewQueryOption(coll.name).
			WithFilter(fmt.Sprintf("category == '%s'", category)).
			WithOutputFields("category").
			WithLimit(10))
		if err != nil {
			fmt.Printf("    Query %d on %s failed: %v\n", i+1, coll.name, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	fmt.Println("    Completed 15 queries!")
	fmt.Println()

	// Intentionally generate some errors to test WebUI error display
	fmt.Println("[8] Generating some intentional errors for WebUI testing...")

	// Error 1: Search on non-existent collection
	_, err = cli.Search(ctx, milvusclient.NewSearchOption("non_existent_collection", 10,
		[]entity.Vector{entity.FloatVector(randomVector(dim))}))
	if err != nil {
		fmt.Printf("    [Expected Error] Search on non-existent collection: %v\n", err)
	}

	// Error 2: Search with wrong vector dimension (expecting 128, sending 64)
	_, err = cli.Search(ctx, milvusclient.NewSearchOption(collections[0].name, 10,
		[]entity.Vector{entity.FloatVector(randomVector(64))})) // Wrong dimension!
	if err != nil {
		fmt.Printf("    [Expected Error] Search with wrong dimension: %v\n", err)
	}

	// Error 3: Query with invalid filter expression
	_, err = cli.Query(ctx, milvusclient.NewQueryOption(collections[0].name).
		WithFilter("invalid_field == 'value'"). // Invalid field
		WithLimit(10))
	if err != nil {
		fmt.Printf("    [Expected Error] Query with invalid filter: %v\n", err)
	}

	fmt.Println("    Errors generated! Check WebUI to see if they are captured.")
	fmt.Println()

	// Get telemetry stats
	fmt.Println("[9] Getting telemetry metrics...")
	telemetry := cli.GetTelemetry()
	if telemetry != nil {
		snapshot := telemetry.GetLatestSnapshot()
		fmt.Println("    Operations:")
		if snapshot != nil {
			for _, opMetrics := range snapshot.Metrics {
				fmt.Printf("      %s: requests=%d, success=%d, errors=%d, avg_latency=%.2fms, p99_latency=%.2fms\n",
					opMetrics.Operation, opMetrics.Global.RequestCount, opMetrics.Global.SuccessCount, opMetrics.Global.ErrorCount,
					opMetrics.Global.AvgLatencyMs, opMetrics.Global.P99LatencyMs)
				for coll, cm := range opMetrics.CollectionMetrics {
					fmt.Printf("        [%s] requests=%d, avg=%.2fms, p99=%.2fms\n",
						coll, cm.RequestCount, cm.AvgLatencyMs, cm.P99LatencyMs)
				}
			}
		}
	}
	fmt.Println()

	// Keep running to allow viewing in WebUI
	fmt.Println("=== Initial Setup Complete ===")
	fmt.Println()
	fmt.Println("Open WebUI at: http://localhost:9091/webui/telemetry.html")
	fmt.Println("Login: root / Milvus")
	fmt.Println()
	fmt.Println("Press Ctrl+C to exit (client will keep sending heartbeats)...")
	fmt.Println()

	// Keep alive for WebUI viewing - send 10 requests per second for visible QPS
	ticker := time.NewTicker(100 * time.Millisecond) // 10 QPS
	defer ticker.Stop()
	requestCount := 0
	for range ticker.C {
		requestCount++
		// Pick a random collection and operation type
		coll := collections[rand.Intn(len(collections))]
		opType := rand.Intn(2) // 0 = query, 1 = search

		// Every 50 requests, generate an intentional error for WebUI testing
		if requestCount%50 == 0 {
			// Generate a search error with wrong dimension
			cli.Search(ctx, milvusclient.NewSearchOption(coll.name, 10,
				[]entity.Vector{entity.FloatVector(randomVector(32))})) // Wrong dimension!
			fmt.Printf("[%d] Generated intentional error (wrong dimension)\n", requestCount)
			continue
		}

		if opType == 0 {
			category := coll.categories[rand.Intn(len(coll.categories))]
			cli.Query(ctx, milvusclient.NewQueryOption(coll.name).
				WithFilter(fmt.Sprintf("category == '%s'", category)).
				WithLimit(1))
		} else {
			queryVector := randomVector(dim)
			cli.Search(ctx, milvusclient.NewSearchOption(coll.name, 5,
				[]entity.Vector{entity.FloatVector(queryVector)}))
		}

		// Print every 10 requests (once per second)
		if requestCount%10 == 0 {
			fmt.Printf("[%d] Sent 10 requests in last second\n", requestCount)
		}
	}
}

func randomVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := range vec {
		vec[i] = rand.Float32()
	}
	return vec
}
