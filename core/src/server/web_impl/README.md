# Milvus RESTful API

<!-- TOC -->

- [Overview](#overview)
- [API Reference](#api-reference)
    - [`/state`](#state)
    - [`/devices`](#devices)
    - [`/config/advanced` (GET)](#configadvanced-get)
    - [`/config/advanced` (PUT)](#configadvanced-put)
    - [`/config/advanced` (OPTIONS)](#configadvanced-options)
    - [`/config/gpu_resources` (GET)](#configgpu_resources-get)
    - [`/config/gpu_resources` (PUT)](#configgpu_resources-put)
    - [`/config/gpu_resources` (OPTIONS)](#configgpu_resources-options)
    - [`/tables` (GET)](#tables-get)
    - [`/tables` (POST)](#tables-post)
    - [`/tables` (OPTIONS)](#tables-options)
    - [`/tables/{table_name}` (GET)](#tablestable_name-get)
    - [`/tables/{table_name}` (DELETE)](#tablestable_name-delete)
    - [`/tables/{table_name}` (OPTIONS)](#tablestable_name-options)
    - [`/tables/{table_name}/indexes` (GET)](#tablestable_nameindexes-get)
    - [`/tables/{table_name}/indexes` (POST)](#tablestable_nameindexes-post)
    - [`/tables/{table_name}/indexes` (DELETE)](#tablestable_nameindexes-delete)
    - [`/tables/{table_name}/indexes` (OPTIONS)](#tablestable_nameindexes-options)
    - [`/tables/{table_name}/partitions` (GET)](#tablestable_namepartitions-get)
    - [`/tables/{table_name}/partitions` (POST)](#tablestable_namepartitions-post)
    - [`/tables/{table_name}/partitions` (OPTIONS)](#tablestable_namepartitions-options)
    - [`/tables/{table_name}/partitions/{partition_tag}` (DELETE)](#tablestable_namepartitionspartition_tag-delete)
    - [`/tables/{table_name}/partitions/{partition_tag}` (OPTIONS)](#tablestable_namepartitionspartition_tag-options)
    - [`/tables/{table_name}/vectors` (PUT)](#tablestable_namevectors-put)
    - [`/tables/{table_name}/vectors` (POST)](#tablestable_namevectors-post)
    - [`/tables/{table_name}/vectors` (OPTIONS)](#tablestable_namevectors-options)
    - [`/system/{msg}` (GET)](#systemmsg-get)
- [Error Codes](#error-codes) 

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

#### Response

| Status code    | Description |
|-----------------|---|
| 200     | The request is successful.|

#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19121/state" -H "accept: application/json"
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

#### Response

| Status code    | Description |
|-----------------|---|
| 200     | The request is successful.|
| 400     | The request is incorrect. Refer to the error message for details. |


#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19121/devices" -H "accept: application/json"
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

#### Response

| Status code    | Description |
|-----------------|---|
| 200     | The request is successful.|
| 400     | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19121/config/advanced" -H "accept: application/json"
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

#### Response

| Status code    | Description |
|-----------------|---|
| 200     | The request is successful.|
| 400     | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X PUT "http://192.168.1.65:19121/config/advanced" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"cpu_cache_capacity\":4,\"cache_insert_data\":false,\"use_blas_threshold\":1100,\"gpu_search_threshold\":1000}"
```

##### Response


```json
{"message": "OK","code": 0}
```

### `/config/advanced` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/config/advanced`  |
| Header  | N/A  |
| Body    |   N/A |
| Method    |   OPTIONS |


#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19121/config/advanced"
```

### `/config/gpu_resources` (GET)

Gets the parameter values in `gpu_resource_config` of the Milvus configuration file.

> Note: This method is available only for GPU-supported Milvus.

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/config/gpu_resources`  |
| Header  | `accept: application/json`  |
| Body    |   N/A |
| Method    |   GET |

#### Response

| Status code    | Description |
|-----------------|---|
| 200     | The request is successful.|
| 400     | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19121/config/gpu_resources" -H "accept: application/json"
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

##### Body Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `enable`     |   Specifies whether to enable GPU resources.  | Yes   |
| `cache_capacity`  | Size of GPU memory per card used for cache in GBs.  |  Yes  | 
| `search_resources`    |  GPU devices used for search computation, must be in format `gpux`.  |  Yes |
| `build_index_resources`    |   GPU devices used for index building, must be in format `gpux`.   |   Yes  |

#### Response

| Status code    | Description |
|-----------------|---|
| 200     | The request is successful.|
| 400     | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X PUT "http://192.168.1.65:19121/config/gpu_resources" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"enable\":true,\"cache_capacity\":1,\"search_resources\":[\"GPU0\"],\"build_index_resources\":[\"GPU0\"]}"
```

##### Response

```json
{"message": "OK","code": 0}
```

### `/config/gpu_resources` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

> Note: This method is available only for GPU-supported Milvus.

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/config/gpu_resources`  |
| Header  | N/A  |
| Body    |   N/A |
| Method    |   OPTIONS |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19121/config/gpu_resources"
```

### `/tables` (GET)

Gets all tables starting from `offset` and ends with `page_size`.

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
| `offset`     |  Row offset from which the data page starts. The default is 0.    | No   |
| `page_size`  |  Size of the data page. The default is 10.   |  No  |


#### Response

| Status code    | Description |
|-----------------|---|
| 200     | The request is successful.|
| 400     | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19121/tables?offset=0&page_size=1" -H "accept: application/json"
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
| `metric_type`    |   The method vector distances are compared in Milvus. The default is L2. Currently supported metrics include `L2` (Euclidean distance), `IP` (Inner Product), `HAMMING` (Hamming distance), `JACCARD` (Jaccard distance), and `TANIMOTO` (Tanomoto distance).    |   No  |

#### Response

| Status code    | Description |
|-----------------|---|
| 201     | Created |
| 400     | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X POST "http://192.168.1.65:19121/tables" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"table_name\":\"test_table\",\"dimension\":1,\"index_file_size\":10,\"metric_type\":\"L2\"}"
```

##### Response


```json
{"message":"OK","code":0}
```

### `/tables` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables`  |
| Header  | N/A  |
| Body    |   N/A |
| Method    |   OPTIONS |


#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19121/tables"
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


#### Response

| Status code    | Description |
|-----------------|---|
| 200     | The request is successful.|
| 400     | The request is incorrect. Refer to the error message for details. |
| 404     | The required resource does not exist. |

#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19121/tables/test_table" -H "accept: application/json"
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

#### Response

| Status code    | Description |
|-----------------|---|
| 204     | Deleted|
| 400     | The request is incorrect. Refer to the error message for details. |
| 404     | The required resource does not exist. |

#### Example

##### Request


```shell
$ curl -X DELETE "http://192.168.1.65:19121/tables/test_table" -H "accept: application/json"
```

If the deletion is successful, no message will be returned.

### `/tables/{table_name}` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|-----|
| Name     | `/tables/{table_name}`  |
| Header  | N/A  |
| Body    |   N/A |
| Method    |   OPTIONS |

#### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`     | Name of the table.   | Yes   |


#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19121/tables/test_table"
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

#### Response

| Status code    | Description |
|-----------------|---|
| 200     | The request is successful.|
| 400     | The request is incorrect. Refer to the error message for details. |
| 404     | The required resource does not exist. |

#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19121/tables/test_table/indexes" -H "accept: application/json"
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
| `index_type`     | The type of indexing method to query the table. Please refer to [Index Types](https://www.milvus.io/docs/reference/index.md) for detailed introduction of supported indexes. The default is "FLAT".  | No   |
| `nlist`     |  Number of vector buckets in a file. The default is 16384.   | No   |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`     | Name of the table.   | Yes   |

#### Response

| Status code    | Description |
|-----------------|---|
| 201     | Created |
| 400     | The request is incorrect. Refer to the error message for details. |
| 404     | The required resource does not exist. |

#### Response

| Status code    | Description |
|-----------------|---|
| 201     | Created |
| 400     | The request is incorrect. Refer to the error message for details. |
| 404     | The required resource does not exist. |

#### Example

##### Request

```shell
$ curl -X POST "http://192.168.1.65:19121/tables/test_table/indexes" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"index_type\":\"FLAT\",\"nlist\":16384}"
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

#### Response

| Status code    | Description |
|-----------------|---|
| 204     | Deleted |
| 400     | The request is incorrect. Refer to the error message for details. |
| 404     | Resource not available |


#### Example

##### Request

```shell
$ curl -X DELETE "http://192.168.1.65:19121/tables/test_table/indexes" -H "accept: application/json"
```

If the deletion is successful, no message will be returned.


### `/tables/{table_name}/indexes` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables/{table_name}/indexes`  |
| Header  | N/A  |
| Body    |   N/A |
| Method    |   OPTIONS |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`     | Name of the table.   | Yes   |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19121/tables/test_table/indexes"
```

### `/tables/{table_name}/partitions` (GET)

Gets all partitions in a table starting from `offset` and ends with `page_size`.

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
| `offset`     |  Row offset from which the data page starts. The default is 0.    | No   |
| `page_size`  |  Size of the data page. The default is 10.   |  No  |

#### Response

| Status code    | Description |
|-----------------|---|
| 200     | The request is successful.|
| 400     | The request is incorrect. Refer to the error message for details. |
| 404     | The required resource does not exist. |

#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19121/tables/test_table/partitions?offset=0&page_size=3" -H "accept: application/json"
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

#### Response

| Status code    | Description |
|-----------------|---|
| 201     | Created |
| 400     | The request is incorrect. Refer to the error message for details. |
| 404     | The required resource does not exist. |

#### Example

##### Request

```shell
$ curl -X POST "http://192.168.1.65:19121/tables/test_table/partitions" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"partition_name\": \"partition_1\",\"partition_tag\": \"test\"}"
```

##### Response

```json
{"message":"OK","code":0}
```

### `/tables/{table_name}/partitions` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables/{table_name}/partitions`  |
| Header  | N/A  |
| Body    |   N/A |
| Method    |   OPTIONS |

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name`              |        Name of the table.                  |   Yes     |


#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19121/tables/test_table/partitions"
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

#### Response

| Status code    | Description |
|-----------------|---|
| 204     | Deleted |
| 400     | The request is incorrect. Refer to the error message for details. |
| 404     | The requested resource does not exist. |

#### Example

##### Request

```shell
$ curl -X DELETE "http://192.168.1.65:19121/tables/test_table/partitions/tags_01" -H "accept: application/json"
```

The deletion is successful if no information is returned.

### `/tables/{table_name}/partitions/{partition_tag}` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables/{table_name}/partitions/{partition_tag}`  |
| Header  | N/A  |
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
$ curl -X OPTIONS "http://192.168.1.65:19121/tables/test_table/partitions/tag"
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
  "records": [[number($float)]],
  "records_bin": [[number($uint64)]]
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
| `records`  |  Numeric vectors to insert to the table.  |  Yes  |
| `records_bin` | Binary vectors to insert to the table. |    Yes   |

> Note: Select `records` or `records_bin` depending on the metric used by the table. If the table uses `L2` or `IP`, you must use `records`. If the table uses `HAMMING`, `JACCARD`, or `TANIMOTO`, you must use `records_bin`.


##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name` |  Name of the table.      |   Yes     |

#### Response

| Status code    | Description |
|-----------------|---|
| 200     | The request is successful.|
| 400     | The request is incorrect. Refer to the error message for details. |
| 404     | The required resource does not exist. |

#### Example

##### Request

```shell
$ curl -X PUT "http://192.168.1.65:19121/tables/test_table/vectors" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"topk\":2,\"nprobe\":16,\"records\":[[0.1]]}"
```

##### Response

```json
{"num":1,"results":[[{"id":"1578989029645098000","distance":"0.000000"},{"id":"1578989029645098001","distance":"0.010000"}]]}
```

### `/tables/{table_name}/vectors` (POST)

Inserts vectors to a table.

> Note: It is recommended that you do not insert more than 1 million vectors per request.

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/tables/{table_name}/vectors</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "tag": string,
  "records": [[number($float)]],
  “records_bin”:[[number($uint64)]]
  "ids": [integer($int64)]
}
</code></pre> </td></tr>
<tr><td>Method</td><td>POST</td></tr>
</table>

##### Body Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `tag`     |  Tag of the partition to insert vectors to.   | No   |
| `records`  |  Numeric vectors to insert to the table.  |  Yes  |
| `records_bin` | Binary vectors to insert to the table.  |    Yes    |
| `ids`    |  IDs of the vectors to insert to the table. If you assign IDs to the vectors, you must provide IDs for all vectors in the table. If you do not specify this parameter, Milvus automatically assigns IDs to the vectors. |  No |

> Note: Select `records` or `records_bin` depending on the metric used by the table. If the table uses `L2` or `IP`, you must use `records`. If the table uses `HAMMING`, `JACCARD`, or `TANIMOTO`, you must use `records_bin`.

##### Query Parameters

| Parameter  | Description  |  Required? |
|-----------------|---|------|
| `table_name` |  Name of the table.      |   Yes     |

#### Response

| Status code    | Description |
|-----------------|---|
| 201     | Created |
| 400     | The request is incorrect. Refer to the error message for details. |
| 404     | The required resource does not exist. |

#### Example

##### Request

```shell
$ curl -X POST "http://192.168.1.65:19121/tables/test_table/vectors" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"records\":[[0.1],[0.2],[0.3],[0.4]]}"
```

##### Response

```json
{"ids":["1578989029645098000","1578989029645098001","1578989029645098002","1578989029645098003"]}
```

### `/tables/{table_name}/vectors` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component     | Value  |
|-----------------|---|
| Name     | `/tables/{table_name}/vectors`  |
| Header  | N/A |
| Body    |   N/A |
| Method    |   OPTIONS |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://192.168.1.65:19121/tables/test_table/vectors"
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

#### Response

| Status code    | Description |
|-----------------|---|
| 200     | The request is successful.|
| 400     | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X GET "http://192.168.1.65:19121/system/version" -H "accept: application/json"
```

##### Response

```json
{"reply":"0.6.0"}
```

## Error Codes

The RESTful API returns error messages as JSON text. Each type of error message has a specific error code.

| Type    | Code  |
|----------|------|
SUCCESS | 0 |
UNEXPECTED_ERROR | 1 |
CONNECT_FAILED | 2 |
PERMISSION_DENIED | 3 |
TABLE_NOT_EXISTS | 4 |
ILLEGAL_ARGUMENT | 5 |
ILLEGAL_RANGE | 6 |
ILLEGAL_DIMENSION | 7 |
ILLEGAL_INDEX_TYPE | 8 |
ILLEGAL_TABLE_NAME | 9 |
ILLEGAL_TOPK | 10 |
ILLEGAL_ROWRECORD | 11 |
ILLEGAL_VECTOR_ID | 12 |
ILLEGAL_SEARCH_RESULT | 13 |
FILE_NOT_FOUND | 14 |
META_FAILED | 15 |
CACHE_FAILED | 16 |
CANNOT_CREATE_FOLDER | 17 |
CANNOT_CREATE_FILE | 18 |
CANNOT_DELETE_FOLDER | 19 |
CANNOT_DELETE_FILE | 20 |
BUILD_INDEX_ERROR | 21 |
ILLEGAL_NLIST | 22 |
ILLEGAL_METRIC_TYPE | 23 |
OUT_OF_MEMORY | 24 |
PATH_PARAM_LOSS | 31 |
QUERY_PARAM_LOSS | 32 |
BODY_FIELD_LOSS | 33 |
ILLEGAL_QUERY_PARAM | 36 |
