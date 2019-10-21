# Milvus test cases

## * Interfaces test

### 1. 连接测试

#### 1.1 连接

| cases            | expected                                     |
| ---------------- | -------------------------------------------- |
| 非法IP 123.0.0.2 | method: connect raise error in given timeout |
| 正常 uri         | attr: connected assert true                  |
| 非法 uri         | method: connect raise error in given timeout |
| 最大连接数       | all connection attrs: connected assert true  |
|                  |                                              |

#### 1.2 断开连接

| cases                    | expected            |
| ------------------------ | ------------------- |
| 正常连接下，断开连接     | connect raise error |
| 正常连接下，重复断开连接 | connect raise error |

### 2. Table operation

#### 2.1 表创建

##### 2.1.1 表名

| cases                     | expected    |
| ------------------------- | ----------- |
| 基础功能，参数正常        | status pass |
| 表名已存在                | status fail |
| 表名："中文"              | status pass |
| 表名带特殊字符: "-39fsd-" | status pass |
| 表名带空格: "test1 2"     | status pass |
| invalid dim: 0            | raise error |
| invalid dim: -1           | raise error |
| invalid dim: 100000000    | raise error |
| invalid dim: "string"     | raise error |
| index_type: 0             | status pass |
| index_type: 1             | status pass |
| index_type: 2             | status pass |
| index_type: string        | raise error |
|                           |             |

##### 2.1.2 维数支持

| cases                 | expected    |
| --------------------- | ----------- |
| 维数: 0               | raise error |
| 维数负数: -1          | raise error |
| 维数最大值: 100000000 | raise error |
| 维数字符串: "string"  | raise error |
|                       |             |

##### 2.1.3 索引类型支持

| cases            | expected    |
| ---------------- | ----------- |
| 索引类型: 0      | status pass |
| 索引类型: 1      | status pass |
| 索引类型: 2      | status pass |
| 索引类型: string | raise error |
|                  |             |

#### 2.2 表说明

| cases                  | expected                         |
| ---------------------- | -------------------------------- |
| 创建表后，执行describe | 返回结构体，元素与创建表参数一致 |
|                        |                                  |

#### 2.3 表删除

| cases          | expected               |
| -------------- | ---------------------- |
| 删除已存在表名 | has_table return False |
| 删除不存在表名 | status fail            |
|                |                        |

#### 2.4 表是否存在

| cases                   | expected     |
| ----------------------- | ------------ |
| 存在表，调用has_table   | assert true  |
| 不存在表，调用has_table | assert false |
|                         |              |

#### 2.5 查询表记录条数

| cases                | expected                 |
| -------------------- | ------------------------ |
| 空表                 | 0                        |
| 空表插入数据（单条） | 1                        |
| 空表插入数据（多条） | assert length of vectors |

#### 2.6 查询表数量

| cases                                         | expected                         |
| --------------------------------------------- | -------------------------------- |
| 两张表，一张空表，一张有数据：调用show tables | assert length of table list == 2 |
|                                               |                                  |

### 3. Add vectors

| interfaces  | cases                                                     | expected                             |
| ----------- | --------------------------------------------------------- | ------------------------------------ |
| add_vectors | add basic                                                 | assert length of ids == nq           |
|             | add vectors into table not existed                        | status fail                          |
|             | dim not match: single vector                              | status fail                          |
|             | dim not match: vector list                                | status fail                          |
|             | single vector element empty                               | status fail                          |
|             | vector list element empty                                 | status fail                          |
|             | query immediately after adding                            | status pass                          |
|             | query immediately after sleep 6s                          | status pass && length of result == 1 |
|             | concurrent add with multi threads(share one connection)   | status pass                          |
|             | concurrent add with multi threads(independent connection) | status pass                          |
|             | concurrent add with multi process(independent connection) | status pass                          |
|             | index_type: 2                                             | status pass                          |
|             | index_type: string                                        | raise error                          |
|             |                                                           |                                      |

### 4. Search vectors

| interfaces     | cases                                             | expected                         |
| -------------- | ------------------------------------------------- | -------------------------------- |
| search_vectors | search basic(query vector in vectors, top-k<nq)   | assert length of result == nq    |
|                | search vectors into table not existed             | status fail                      |
|                | basic top-k                                       | score of query vectors  == 100.0 |
|                | invalid top-k: 0                                  | raise error                      |
|                | invalid top-k: -1                                 | raise error                      |
|                | invalid top-k: "string"                           | raise error                      |
|                | top-k > nq                                        | assert length of result == nq    |
|                | concurrent search                                 | status pass                      |
|                | query_range(get_current_day(), get_current_day()) | assert length of result == nq    |
|                | invalid query_range: ""                           | raise error                      |
|                | query_range(get_last_day(2), get_last_day(1))     | assert length of result == 0     |
|                | query_range(get_last_day(2), get_current_day())   | assert length of result == nq    |
|                | query_range((get_last_day(2), get_next_day(2))    | assert length of result == nq    |
|                | query_range((get_current_day(), get_next_day(2))  | assert length of result == nq    |
|                | query_range(get_next_day(1), get_next_day(2))     | assert length of result == 0     |
|                | score: vector[i] = vector[i]+-0.01                | score > 99.9                     |