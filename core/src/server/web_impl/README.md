# Milvus RESTful API

<!-- TOC -->

- [Overview](#overview)
- [API Reference](#api-reference)
    - [`/state`](#state)
    - [`/devices`](#devices)
    - [`/config/advanced` (GET)](#configadvanced-get)
    - [`/config/advanced` (PUT)](#configadvanced-put)
    - [`/config/advanced` (OPTIONS)](#configadvanced-options)
    - [`/config/gpu_resources` (GET)](#configgpuresources-get)
    - [`/config/gpu_resources` (PUT)](#configgpuresources-put)
    - [`/config/gpu_resources` (OPTIONS)](#configgpuresources-options)
    - [`/tables` (GET)](#tables-get)
    - [`/tables` (POST)](#tables-post)
    - [`/tables` (OPTIONS)](#tables-options)
    - [`/tables/{table_name}` (GET)](#tablestablename-get)
    - [`/tables/{table_name}` (DELETE)](#tablestablename-delete)
    - [`/tables/{table_name}` (OPTIONS)](#tablestablename-options)
    - [`/tables/{table_name}/indexes` (GET)](#tablestablenameindexes-get)
    - [`/tables/{table_name}/indexes` (POST)](#tablestablenameindexes-post)
    - [`/tables/{table_name}/indexes` (DELETE)](#tablestablenameindexes-delete)
    - [`/tables/{table_name}/indexes` (OPTIONS)](#tablestablenameindexes-options)
    - [`/tables/{table_name}/partitions` (GET)](#tablestablenamepartitions-get)
    - [`/tables/{table_name}/partitions` (POST)](#tablestablenamepartitions-post)
    - [`/tables/{table_name}/partitions` (OPTIONS)](#tablestablenamepartitions-options)
    - [`/tables/{table_name}/partitions/{partition_tag}` (DELETE)](#tablestablenamepartitionspartitiontag-delete)
    - [`/tables/{table_name}/partitions/{partition_tag}` (OPTIONS)](#tablestablenamepartitionspartitiontag-options)
    - [`/tables/{table_name}/vectors` (PUT)](#tablestablenamevectors-put)
    - [`/tables/{table_name}/vectors` (POST)](#tablestablenamevectors-post)
    - [`/tables/{table_name}/vectors` (OPTIONS)](#tablestablenamevectors-options)
    - [`/system/{msg}` (GET)](#systemmsg-get)

<!-- /TOC -->

## Overview

With the RESTful API, you can use Milvus by sending HTTP requests to the Milvus server web port. The RESTful API is available as long as you have a running Milvus server. You can set the web port in the Milvus configuration file. Refer to [Milvus Configuration](https://www.milvus.io/docs/reference/milvus_config.md) for more information.

## API Reference

### `/state`

Checks whether the web server is running.

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/state`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   GET    |


#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19122/state" -H "accept: application/json"
```

##### Response

```json
{"message":"Success","code":0}
```

### `/devices`

Gets CPU/GPU information from the host.

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/devices`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   GET    |

#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19122/devices" -H "accept: application/json"
```

##### Response

```json
{"cpu":{"memory":31},"gpus":{"GPU0":{"memory":5}}}
```


### `/config/advanced` (GET)

Gets the values of parameters in `cache_config` and `engine_config` of the Milvus configuration file.

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/config/advanced`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   GET |


#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19122/config/advanced" -H "accept: application/json"
```

##### Response

```json
{"cpu_cache_capacity":4,"cache_insert_data":false,"use_blas_threshold":1100,"gpu_search_threshold":1000}
```

### `/config/advanced` (PUT)

Updates the values of parameters in `cache_config` and `engine_config` of the Milvus configuration file.

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/config/advanced</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "cpu_cache_capacity": integer($int64),
  "cache_insert_data": boolean,
  "use_blas_threshold": integer($int64),
  "gpu_search_threshold": integer($int64)
} 
</code></pre> </td></tr>
<tr><td>Method</td><td>PUT</td></tr>

</table>

> Note: `gpu_search_config` is available only in GPU-supported Milvus.

##### Body Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `cpu_cache_capacity`     |  Value of `cpu_cache_capacity` in the Milvus configuration file. The default is 4.| No   |
| `cache_insert_data`  |  Value of `cache_insert_data` in the Milvus configuration file. The default is false. |  No  |
| `use_blas_threshold`    |  Value of `use_blas_threshold` in the Milvus configuration file. The default is 1100.   |  No |
| `gpu_search_threshold`    |  Value of `gpu_search_threshold` in the Milvus configuration file. The default is 1000.     |   No  |


#### Example

##### Request

```shell
$ curl -X PUT "http://192.168.1.65:19122/config/advanced" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"cpu_cache_capacity\":4,\"cache_insert_data\":false,\"use_blas_threshold\":1100,\"gpu_search_threshold\":1000}"
```

##### Response

```json
{"message": "OK","code": 0}
```

### `/config/advanced` (OPTIONS)

Returns what request method the web server supports. This is useful in Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/config/advanced`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   OPTIONS |


#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19122/config/advanced" -H "accept: text/plain"
```

### `/config/gpu_resources` (GET)

Gets the parameter values in `gpu_resource_threshold` of the Milvus configuration file.

> Note: This method is available only for GPU-supported Milvus.

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/config/gpu_resources`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   GET |


#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19122/config/gpu_resources" -H "accept: text/plain"
```

##### Response

```json
{"enable":true,"cache_capacity":1,"search_resources":["GPU0"],"build_index_resources":["GPU0"]}
```

### `/config/gpu_resources` (PUT)

Updates the parameter values in `gpu_resource_config` of the Milvus configuration file.

> Note: This method is available only for GPU-supported Milvus.

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/config/gpu_resources</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "enable": boolean,
  "cache_capacity": integer($int64),
  "search_resources": [string],
  "build_index_resources": [string]
}
</code></pre> </td></tr>
<tr><td>Method</td><td>PUT</td></tr>

</table>

#### Example

##### Request

```shell
$ curl -X PUT "http://192.168.1.65:19122/config/gpu_resources" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"enable\":true,\"cache_capacity\":1,\"search_resources\":[\"GPU0\"],\"build_index_resources\":[\"GPU0\"]}"
```

##### Response

```json
{"message": "OK","code": 0}
```

### `/config/gpu_resources` (OPTIONS)

Returns what request method the web server supports. This is useful in Cross-Origin Resource Sharing (CORS).

> Note: This method is available only for GPU-supported Milvus.

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/config/gpu_resources`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   OPTIONS |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19122/config/gpu_resources" -H "accept: text/plain"
```

### `/tables` (GET)

Gets information about all tables.

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   GET |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `offset`     |  Row offset from which the data page starts.    | Yes   |
| `page_size`  |  Size of the data page.   |  Yes  |

<!---
`offset` and `page_size` are optional in other SDKs
-->

#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19122/tables?offset=0&page_size=1" -H "accept: application/json"
```

##### Response


```json
{"tables":[{"table_name":"test_table","dimension":1,"index_file_size":10,"metric_type":"L2","count":0,"index":"FLAT","nlist":16384}],"count":58}
```

### `/tables` (POST)

Creates a table.

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/tables</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "table_name": string,
  "dimension": integer($int64),
  "index_file_size": integer($int64),
  "metric_type": string
}
</code></pre> </td></tr>
<tr><td>Method</td><td>POST</td></tr>

</table>

##### Body Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`     |   The name of the table to create, which must be unique within its database.  | Yes   |
| `dimension`  |  The dimension of the vectors that are to be inserted into the created table. |  Yes  |
| `index_file_size`    |  Threshold value that triggers index building for raw data files. The default is 1024.   |  No |
| `metric_type`    |   The method vector distances are compared in Milvus. The default is L2. Currently supported metrics include L2 (Euclidean distance) and IP (Inner Product).    |   No  |


#### Example

##### Request

```shell
$ curl -X POST "http://192.168.1.65:19122/tables" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"table_name\":\"test_table\",\"dimension\":1,\"index_file_size\":10,\"metric_type\":\"L2\"}"
```

##### Response


```json
{"message":"OK","code":0}
```


### `/tables` (OPTIONS)

Returns what request method the web server supports. This is useful in Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   OPTIONS |


#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19122/tables" -H "accept: text/plain"
```

### `/tables/{table_name}` (GET)

Gets all information about a table by name.

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables/{table_name}`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   GET |


##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`     | Name of the table.   | Yes   |


#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19122/tables/test_table" -H "accept: application/json"
```

##### Response

```json
{"table_name":"test_table","dimension":1,"index_file_size":10,"metric_type":"L2","count":0,"index":"FLAT","nlist":16384}
```

### `/tables/{table_name}` (DELETE)

Drops a table by name.

#### Request

| Request Component     | Value  |
|-----------------|-----|
| Name     | `/tables/{table_name}`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   DELETE |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`     | Name of the table.   | Yes   |


#### Example

##### Request

```shell
$ curl -X DELETE "http://192.168.1.65:19122/tables/test_table" -H "accept: application/json"
```

If the deletion is successful, no message will be returned.

### `/tables/{table_name}` (OPTIONS)

Returns what request method the web server supports. This is useful in Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|-----|
| Name     | `/tables/{table_name}`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   OPTIONS |

#### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`     | Name of the table.   | Yes   |


#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19122/tables/test_table" -H "accept: text/plain"
```

### `/tables/{table_name}/indexes` (GET)

Gets the index type and nlist of a table.

#### Request

| Request Component     | Value  |
|-----------------|-----|
| Name     | `/tables/{table_name}/indexes`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   GET |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`     | Name of the table.   | Yes   |


#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19122/tables/test_table/indexes" -H "accept: application/json"
```

##### Response

```json
{"index_type":"FLAT","nlist":16384}
```

### `/tables/{table_name}/indexes` (POST)

Updates the index type and nlist of a table.

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/tables</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "index_type": string,
  "nlist": integer($int64)
}
</code></pre> </td></tr>
<tr><td>Method</td><td>POST</td></tr>

</table>

##### Body Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `index_type`     | The type of indexing method to query the table. Please refer to [Index Types](https://www.milvus.io/docs/reference/index.md) for detailed introduction of supported indexes.  | Yes   |
| `nlist`     |  Number of vector buckets in a file.   | Yes   |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`     | Name of the table.   | Yes   |


#### Example

##### Request

```shell
$ curl -X POST "http://192.168.1.65:19122/tables/test_table/indexes" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"index_type\":\"FLAT\",\"nlist\":16384}"
```

##### Response

```json
{"message":"OK","code":0}
```

### `/tables/{table_name}/indexes` (DELETE)

Drops an index for a table.

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables/{table_name}/indexes`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   DELETE |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`     | Name of the table.   | Yes   |


#### Example

##### Request

```shell
$ curl -X DELETE "http://192.168.1.65:19122/tables/test_table/indexes" -H "accept: application/json"
```

If the deletion is successful, no message will be returned.

### `/tables/{table_name}/indexes` (OPTIONS)

Returns what request method the web server supports. This is useful in Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables/{table_name}/indexes`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   OPTIONS |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`     | Name of the table.   | Yes   |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19122/tables/test_table/indexes" -H "accept: text/plain"
```

### `/tables/{table_name}/partitions` (GET)

Gets all partitions in a table.

#### Request

| Request Component     | Value  |
|-----------------|-----------|
| Name     | `/tables/{table_name}/partitions`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   GET |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`              |        Name of the table.                  |   Yes     |
| `offset`     |  Row offset from which the data page starts.    | Yes   |
| `page_size`  |  Size of the data page.   |  Yes  |


#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19122/tables/test_table/partitions?offset=0&page_size=3" -H "accept: application/json"
```

##### Response

```json
{"partitions":[{"partition_name":"partition_1","partition_tag":"test_tag"},{"partition_name":"partition_2","partition_tag":"test_2"},{"partition_name":"partition_3","partition_tag":"test_3"}]}
```

### `/tables/{table_name}/partitions` (POST)

Creates a partition in a table.

#### Request

| Request Component     | Value  |
|-----------------|-----------|
| Name     | `/tables/{table_name}/partitions`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   POST |

#### Example

##### Request

```shell
$ curl -X POST "http://192.168.1.65:19122/tables/test_table/partitions" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"partition_name\": \"partition_1\",\"partition_tag\": \"test\"}"
```

##### Response

```json
{"message":"OK","code":0}
```

### `/tables/{table_name}/partitions` (OPTIONS)

Returns what request method the web server supports. This is useful in Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables/{table_name}/partitions`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   OPTIONS |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`              |        Name of the table.                  |   Yes     |


#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19122/tables/test_table/partitions" -H "accept: text/plain"
```

### `/tables/{table_name}/partitions/{partition_tag}` (DELETE)

Deletes a partition by tag.

#### Request

| Request Component     | Value  |
|-----------------|-----------|
| Name     | `/tables/{table_name}/partitions/{partition_tag}`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   DELETE |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`              |        Name of the table that contains the partition.               |   Yes     |
| `partition_tag` |    Tag of the partition to delete.      |   yes |

#### Example

##### Request

```shell
$ curl -X DELETE "http://192.168.1.65:19122/tables/test_table/partitions/tags_01" -H "accept: text/plain"
```

The deletion is successful if no information is returned.

### `/tables/{table_name}/partitions/{partition_tag}` (OPTIONS)

Returns what request method the web server supports. This is useful in Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables/{table_name}/partitions/{partition_tag}`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   OPTIONS |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`              |        Name of the table.                  |   Yes     |
| `partition_tag` |    Tag of the partition      |   yes |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19122/tables/test_table/partitions/tag" -H "accept: text/plain"
```

### `/tables/{table_name}/vectors` (PUT)

Searches vectors in a table.

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/tables/{table_name}/vectors</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "topk": integer($int64),
  "nprobe": integer($int64),
  "tags": [string],
  "file_ids": [string],
  "records": [[number($float)]]
}
</code></pre> </td></tr>
<tr><td>Method</td><td>PUT</td></tr>
</table>

##### Body Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `topk`     |  The top k most similar results of each query vector.   | Yes   |
| `nprobe`  |  Number of queried vector buckets. |  Yes  |
| `tags`    |  Tags of partitions that you need to search. You do not have to specify this value if the table is not partitioned or you wish to search the whole table.   |  No |
| `file_ids`    |  IDs of the vector files. You do not have to specify this value if you do not use Milvus in distributed scenarios. Also, if you assign a value to `file_ids`, the value of `tags` is ignored.    |   No  |
| `records`    |   The list of query vectors to be searched in the table. Each vector value must be float data type, with the same dimension as that defined for the table.  |   Yes  |

<!---
`tags` and `file_ids` are optional in other SDKs
-->

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name` |  Name of the table.      |   Yes     |


#### Example

##### Request

```shell
$ curl -X PUT "http://192.168.1.65:19122/tables/test_table/vectors" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"topk\":2,\"nprobe\":16,\"records\":[[0.1]]}"
```

##### Response

```json
{"num":1,"results":[[{"id":"1578989029645098000","distance":"0.000000"},{"id":"1578989029645098001","distance":"0.010000"}]]}
```

### `/tables/{table_name}/vectors` (POST)

Inserts vectors to a table.

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/tables/{table_name}/vectors</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "tag": string,
  "records": [[number($float)]],
  "ids": [integer($int64)]
}
</code></pre> </td></tr>
<tr><td>Method</td><td>POST</td></tr>
</table>

##### Body Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `tag`     |  Tag of the partition to insert vectors to.   | No   |
| `records`  |  Vectors to insert to the table. |  Yes  |
| `ids`    |  IDs of the vectors to insert to the table. If you assign IDs to the vectors, you must provide IDs for all vectors in the table. If you do not specify this parameter, Milvus automatically assigns IDs to the vectors. |  No |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name` |  Name of the table.      |   Yes     |

#### Example

##### Request


```shell
$ curl -X POST "http://192.168.1.65:19122/tables/test_table/vectors" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"records\":[[0.1],[0.2],[0.3],[0.4]]}"
```

##### Response

```json
{"ids":["1578989029645098000","1578989029645098001","1578989029645098002","1578989029645098003"]}
```

### `/tables/{table_name}/vectors` (OPTIONS)

Returns what request method the web server supports. This is useful in Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables/{table_name}/vectors`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   OPTIONS |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19122/tables/test_table/vectors" -H "accept: text/plain"
```

### `/system/{msg}` (GET)

Gets information about the Milvus server.

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/system/{msg}`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   GET |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `msg` |  Type of the message to return. You can use `status` or `version`.     |   Yes     |

#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19122/system/version" -H "accept: application/json"
```

##### Response

```json
{"reply":"0.6.0"}
```
