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

// nolint
package milvusclient_test

import (
	"context"
	"log"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

func ExampleClient_CreateDatabase() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbName := `test_db`
	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	err = cli.CreateDatabase(ctx, milvusclient.NewCreateDatabaseOption(dbName))
	if err != nil {
		// handle err
	}
}

func ExampleClient_CreateDatabase_withProperties() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbName := `test_db_2`
	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	err = cli.CreateDatabase(ctx, milvusclient.NewCreateDatabaseOption(dbName).WithProperty("database.replica.number", 3))
	if err != nil {
		// handle err
	}
}

func ExampleClient_DescribeDatabase() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbName := `test_db`
	cli, err := milvusclient.New(ctx, &milvusclient.ClientConfig{
		Address: milvusAddr,
	})
	if err != nil {
		// handle err
	}

	db, err := cli.DescribeDatabase(ctx, milvusclient.NewDescribeDatabaseOption(dbName))
	if err != nil {
		// handle err
	}
	log.Println(db)
}
