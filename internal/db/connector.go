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

package db

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.uber.org/zap"
)

// Connector MySQL connections
type Connector struct {
	DB *sqlx.DB
}

// Open creates connections to a database
func Open(cfg *paramtable.MetaDBConfig) (*sqlx.DB, error) {
	// load config
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", cfg.Username, cfg.Password, cfg.Address, cfg.Port, cfg.DBName)
	db, err := sqlx.Open(util.MetaStoreTypeMysql, dataSourceName)
	if err != nil {
		log.Error("Fail to connect dataSource:[%v] Err:[%v]", zap.String("dataSource", dataSourceName), zap.Error(err))
		return nil, err
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)

	// force a connection and test that it worked
	err = db.Ping()
	if err != nil {
		log.Error("Fail to Ping DB Err :[%v]", zap.Error(err))
		return nil, err
	}
	return db, nil
}

// Exec db operations like select,update, delete
//func (c *Connector) Exec(ctx context.Context, sqlText string, params ...interface{}) (qr *QueryResults) {
//	qr = &QueryResults{}
//	result, err := c.DB.ExecContext(ctx, sqlText, params...)
//	//defer HandleException()
//	if err != nil {
//		qr.EffectRow = 0
//		qr.Err = err
//		log.Error("Fail to exec sql:[%v] params:[%v] err:[%v]", zap.String("sql", sqlText), zap.Any("params", params), zap.Error(err))
//		return
//	}
//	qr.EffectRow, _ = result.RowsAffected()
//	qr.LastInsertId, _ = result.LastInsertId()
//	return
//}
//
//type QueryResults struct {
//	EffectRow    int64
//	LastInsertId int64
//	Err          error
//}
