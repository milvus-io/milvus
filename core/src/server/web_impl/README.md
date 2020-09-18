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
  - [`/collections` (GET)](#collections-get)
  - [`/collections` (POST)](#collections-post)
  - [`/collections` (OPTIONS)](#collections-options)
  - [`/collections/{collection_name}` (GET)](#collectionscollection_name-get)
  - [`/collections/{collection_name}` (DELETE)](#collectionscollection_name-delete)
  - [`/collections/{collection_name}` (OPTIONS)](#collectionscollection_name-options)
  - [`/collections/{collection_name}/fields/{field_name}/indexes` (POST)](#collectionscollection_namefieldsfield_nameindexes-post)
  - [`/collections/{collection_name}/fields/{field_name}/indexes` (DELETE)](#collectionscollection_namefieldsfield_nameindexes-delete)
  - [`/collections/{collection_name}/fields/{field_name}/indexes` (OPTIONS)](#collectionscollection_namefieldsfield_nameindexes-options)
  - [`/collections/{collection_name}/partitions` (GET)](#collectionscollection_namepartitions-get)
  - [`/collections/{collection_name}/partitions` (POST)](#collectionscollection_namepartitions-post)
  - [`/collections/{collection_name}/partitions` (OPTIONS)](#collectionscollection_namepartitions-options)
  - [`/collections/{collection_name}/partitions` (DELETE)](#collectionscollection_namepartitions-delete)
  - [`/collections/{collection_name}/segments/{segment_name}/ids` (GET)](#collectionscollection_namesegmentssegment_nameids-get)
  - [`/collections/{collection_name}/entities` (PUT)](#collectionscollection_nameentities-put)
  - [`/collections/{collection_name}/entities` (POST)](#collectionscollection_nameentities-post)
  - [`/collections/{collection_name}/entities` (DELETE)](#collectionscollection_nameentities-delete)
  - [`/collections/{collection_name}/entities` (GET)](#collectionscollection_nameentities_id-get)
  - [`/collections/{collection_name}/entities` (OPTIONS)](#collectionscollection_nameentities-options)
  - [`/system/{msg}` (GET)](#systemmsg-get)
  - [`/system/{op}` (PUT)](#systemop-put)
- [Error Codes](#error-codes)

<!-- /TOC -->

## Overview

With the RESTful API, you can use Milvus by sending HTTP requests to the Milvus server web port. The RESTful API is available as long as you have a running Milvus server. You can set the web port in the Milvus configuration file. Refer to [Milvus Configuration](https://www.milvus.io/docs/reference/milvus_config.md) for more information.

## API Reference

### `/state`

Checks whether the web server is running.

#### Request

| Request Component | Value                      |
| ----------------- | -------------------------- |
| Name              | `/state`                   |
| Header            | `accept: application/json` |
| Body              | N/A                        |
| Method            | GET                        |

#### Response

| Status code | Description                |
| ----------- | -------------------------- |
| 200         | The request is successful. |

#### Example

##### Request

```shell
$ curl -X GET "http://127.0.0.1:19121/state" -H "accept: application/json"
```

##### Response

```json
{ "message": "Success", "code": 0 }
```

### `/devices`<h2 id="devices"></h2>

Gets CPU/GPU information from the host.

#### Request

| Request Component | Value                      |
| ----------------- | -------------------------- |
| Name              | `/devices`                 |
| Header            | `accept: application/json` |
| Body              | N/A                        |
| Method            | GET                        |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X GET "http://127.0.0.1:19121/devices" -H "accept: application/json"
```

##### Response

```json
{ "cpu": { "memory": 31 }, "gpus": { "GPU0": { "memory": 5 } } }
```

### `/config/advanced` (GET)

Gets the values of parameters in `cache_config` and `engine_config` of the Milvus configuration file.

#### Request

| Request Component | Value                      |
| ----------------- | -------------------------- |
| Name              | `/config/advanced`         |
| Header            | `accept: application/json` |
| Body              | N/A                        |
| Method            | GET                        |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X GET "http://127.0.0.1:19121/config/advanced" -H "accept: application/json"
```

##### Response

```json
{
  "cpu_cache_capacity": 4,
  "cache_insert_data": false,
  "use_blas_threshold": 1100,
  "gpu_search_threshold": 1000
}
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

| Parameter              | Description                                                                            | Required? |
| ---------------------- | -------------------------------------------------------------------------------------- | --------- |
| `cpu_cache_capacity`   | Value of `cpu_cache_capacity` in the Milvus configuration file. The default is 4.      | No        |
| `cache_insert_data`    | Value of `cache_insert_data` in the Milvus configuration file. The default is false.   | No        |
| `use_blas_threshold`   | Value of `use_blas_threshold` in the Milvus configuration file. The default is 1100.   | No        |
| `gpu_search_threshold` | Value of `gpu_search_threshold` in the Milvus configuration file. The default is 1000. | No        |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X PUT "http://127.0.0.1:19121/config/advanced" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"cpu_cache_capacity\":4,\"cache_insert_data\":false,\"use_blas_threshold\":1100,\"gpu_search_threshold\":1000}"
```

##### Response

```json
{ "message": "OK", "code": 0 }
```

### `/config/advanced` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component | Value              |
| ----------------- | ------------------ |
| Name              | `/config/advanced` |
| Header            | N/A                |
| Body              | N/A                |
| Method            | OPTIONS            |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://127.0.0.1:19121/config/advanced"
```

### `/config/gpu_resources` (GET)

Gets the parameter values in `gpu_resource_config` of the Milvus configuration file.

> Note: This method is available only for GPU-supported Milvus.

#### Request

| Request Component | Value                      |
| ----------------- | -------------------------- |
| Name              | `/config/gpu_resources`    |
| Header            | `accept: application/json` |
| Body              | N/A                        |
| Method            | GET                        |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X GET "http://127.0.0.1:19121/config/gpu_resources" -H "accept: application/json"
```

##### Response

```json
{
  "enable": true,
  "cache_capacity": 1,
  "search_resources": ["GPU0"],
  "build_index_resources": ["GPU0"]
}
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

| Parameter               | Description                                                        | Required? |
| ----------------------- | ------------------------------------------------------------------ | --------- |
| `enable`                | Specifies whether to enable GPU resources.                         | Yes       |
| `cache_capacity`        | Size of GPU memory per card used for cache in GBs.                 | Yes       |
| `search_resources`      | GPU devices used for search computation, must be in format `gpux`. | Yes       |
| `build_index_resources` | GPU devices used for index building, must be in format `gpux`.     | Yes       |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X PUT "http://127.0.0.1:19121/config/gpu_resources" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"enable\":true,\"cache_capacity\":1,\"search_resources\":[\"GPU0\"],\"build_index_resources\":[\"GPU0\"]}"
```

##### Response

```json
{ "message": "OK", "code": 0 }
```

### `/config/gpu_resources` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

> Note: This method is available only for GPU-supported Milvus.

#### Request

| Request Component | Value                   |
| ----------------- | ----------------------- |
| Name              | `/config/gpu_resources` |
| Header            | N/A                     |
| Body              | N/A                     |
| Method            | OPTIONS                 |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://127.0.0.1:19121/config/gpu_resources"
```

### `/collections` (GET)

Gets all collections starting from `offset` and ends with `page_size`.

#### Request

| Request Component | Value                      |
| ----------------- | -------------------------- |
| Name              | `/collections`             |
| Header            | `accept: application/json` |
| Body              | N/A                        |
| Method            | GET                        |

##### Query Parameters

| Parameter   | Description                                                   | Required? |
| ----------- | ------------------------------------------------------------- | --------- |
| `offset`    | Row offset from which the data page starts. The default is 0. | No        |
| `page_size` | Size of the data page. The default is 10.                     | No        |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X GET "http://127.0.0.1:19121/collections?offset=0&page_size=1" -H "accept: application/json"
```

##### Response

```json
{
    "collections": [
        {
            "collection_name": "test_collection",
            "fields": [
                {
                "field_name": "field_vec",
                "field_type": "VECTOR_FLOAT",
                "index_params": {"name": "index_1", "index_type": "IVFFLAT", "nlist":  4096},
                "extra_params": {"dimension": 128, "metric_type":  "L2"}
                }
            ],
            "segment_size": 1024
        }
    ],
    "count": 58
}
```

### `/collections` (POST)

Creates a collection.

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/tables</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
    "collection_name": "test_collection",
    "fields": [
        {
            "field_name": "field_vec",
            "field_type": "VECTOR_FLOAT",
            "index_params": {"name": "index_1", "index_type": "IVFFLAT", "nlist":  4096},
            "extra_params": {"dimension": 128, "metric_type":  "L2"}
        } 
    ]
}
</code></pre> </td></tr>
<tr><td>Method</td><td>POST</td></tr>

</table>

##### Body Parameters

| Parameter         | Description                                                                               | Required? |
| ----------------- | ----------------------------------------------------------------------------------------- | --------- |
| `collection_name` | The name of the collection to create, which must be unique within its database.           | Yes       |
| `dimension`       | The dimension of the vectors that are to be inserted into the created collection.         | Yes       |
| `index_file_size` | Threshold value that triggers index building for raw data files. The default is 1024.     | No        |
| `metric_type`     | The method vector distances are compared in Milvus. The default is L2.                    | No        |

* Currently supported metrics include:
    - `L2` (Euclidean distance),
    - `IP` (Inner Product)
    - `HAMMING` (Hamming distance)
    - `JACCARD` (Jaccard distance)
    - `TANIMOTO` (Tanomoto distance)
    - `SUBSTRUCTURE` (Sub structure distance)
    - `SUPERSTRUCTURE` (Super structure distance)

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 201         | Created                                                           |
| 400         | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X POST "http://127.0.0.1:19121/collections" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"collection_name\":\"test_collection\",\"dimension\":1,\"index_file_size\":10,\"metric_type\":\"L2\"}"
```

##### Response

```json
{ "message": "OK", "code": 0 }
```

### `/collections` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component | Value          |
| ----------------- | -------------- |
| Name              | `/collections` |
| Header            | N/A            |
| Body              | N/A            |
| Method            | OPTIONS        |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://127.0.0.1:19121/collections"
```

### `/collections/{collection_name}` (GET)

Gets all information about a collection by name.

#### Request

| Request Component | Value                            |
| ----------------- | -------------------------------- |
| Name              | `/collections/{collection_name}` |
| Header            | `accept: application/json`       |
| Body              | N/A                              |
| Method            | GET                              |

##### Query Parameters

| Parameter         | Description                                                                                                                                                                                                                                                                                                               | Required? |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `collection_name` | Name of the collection.                                                                                                                                                                                                                                                                                                   | Yes       |
| `info`            | Type of information to acquire. `info` must either be empty or `stat`. When `info` is empty, Milvus returns collection name, dimension, index file size, metric type, offset, index type, and nlist of the collection. When `info` is `stat`, Milvus returns the collection offset, partition status, and segment status. | No        |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | The required resource does not exist.                             |

#### Example

##### Request

```shell
$ curl -X GET "http://127.0.0.1:19121/collections/test_collection" -H "accept: application/json"
```

##### Response

```json
{
    "code": 0,
    "message":"OK",
    "collection_name": "test_collection",
    "fields": [
        {
            "field_name": "field_vec",
            "field_type": "VECTOR_FLOAT",
            "index_params": {"name": "index_1", "index_type": "IVFFLAT", "nlist":  4096},
            "extra_params": {"dimension": 128, "metric_type":  "L2"}
        } 
    ],
    "row_count": 10000
}
```

##### Request

```shell
$ curl -X GET "http://127.0.0.1:19121/collections/test_collection?info=stat" -H "accept: application/json"
```

##### Response

```json
{
  "partitions":[
    {
      "row_count":10000,
      "segments":[
        {
          "data_size":5284922,
          "index_name":"IVFFLAT",
          "name":"1589468453228582000",
          "row_count":10000
        }
      ],
      "tag":"_default"
    }
  ],
  "row_count":10000
}

```

### `/collections/{collection_name}` (DELETE)

Drops a collection by name.

#### Request

| Request Component | Value                            |
| ----------------- | -------------------------------- |
| Name              | `/collections/{collection_name}` |
| Header            | `accept: application/json`       |
| Body              | N/A                              |
| Method            | DELETE                           |

##### Query Parameters

| Parameter         | Description             | Required? |
| ----------------- | ----------------------- | --------- |
| `collection_name` | Name of the collection. | Yes       |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 204         | Deleted                                                           |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | The required resource does not exist.                             |

#### Example

##### Request

```shell
$ curl -X DELETE "http://127.0.0.1:19121/collections/test_collection" -H "accept: application/json"
```

If the deletion is successful, no message will be returned.

### `/collections/{collection_name}` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component | Value                            |
| ----------------- | -------------------------------- |
| Name              | `/collections/{collection_name}` |
| Header            | N/A                              |
| Body              | N/A                              |
| Method            | OPTIONS                          |

#### Query Parameters

| Parameter         | Description             | Required? |
| ----------------- | ----------------------- | --------- |
| `collection_name` | Name of the collection. | Yes       |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://127.0.0.1:19121/collections/test_collection"
```

### `/collections/{collection_name}/fields/{field_name}/indexes` (POST)

Updates the index type and nlist of a collection.

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/tables</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "index_type": "IVF_FLAT",
  "metric_type": "L2",
  "params": {
      "nlist": 16384
  }
}
</code></pre> </td></tr>
<tr><td>Method</td><td>POST</td></tr>

</table>

##### Body Parameters

| Parameter    | Description                                                                                                                                                                                              | Required? |
| ------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `index_type` | The type of indexing method to query the collection. Please refer to [Milvus Indexes](https://www.milvus.io/docs/guides/index.md) for detailed introduction of supported indexes. The default is "FLAT". | No        |
| `metric_type`| The type of metric to query the collection. Please refer to [Milvus Indexes](https://www.milvus.io/docs/guides/index.md) for detailed introduction of supported indexes. The default is "FLAT". | No        |
| `params`     | The extra params of indexing method to query the collection. Please refer to [Index and search parameters](#Index-and-search-parameters) for detailed introduction of supported indexes.                                              | No        |

##### Query Parameters

| Parameter         | Description             | Required? |
| ----------------- | ----------------------- | --------- |
| `collection_name` | Name of the collection. | Yes       |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 201         | Created                                                           |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | The required resource does not exist.                             |

#### Example

##### Request

```shell
$ curl -X POST "http://127.0.0.1:19121/collections/test_collection/indexes" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"index_type\":\"IVFFLAT\",\"params\": {\"nlist\":4096}}"
```

##### Response

```json
{ "message": "OK", "code": 0 }
```

### `/collections/{collection_name}/fields/{field_name}/indexes/{index_name}` (DELETE)

Drops an index for a collection.

#### Request

| Request Component | Value                                    |
| ----------------- | ---------------------------------------- |
| Name              | `/collections/{collection_name}/indexes` |
| Header            | `accept: application/json`               |
| Body              | N/A                                      |
| Method            | DELETE                                   |

##### Query Parameters

| Parameter         | Description             | Required? |
| ----------------- | ----------------------- | --------- |
| `collection_name` | Name of the collection. | Yes       |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 204         | Deleted                                                           |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | Resource not available                                            |

#### Example

##### Request

```shell
$ curl -X DELETE "http://127.0.0.1:19121/collections/test_collection/indexes" -H "accept: application/json"
```

If the deletion is successful, no message will be returned.

### `/collections/{collection_name}/fields/{field_name}/indexes` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component | Value                                    |
| ----------------- | ---------------------------------------- |
| Name              | `/collections/{collection_name}/fields/{field_name}/indexes` |
| Header            | N/A                                      |
| Body              | N/A                                      |
| Method            | OPTIONS                                  |

##### Query Parameters

| Parameter         | Description             | Required? |
| ----------------- | ----------------------- | --------- |
| `collection_name` | Name of the collection. | Yes       |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://127.0.0.1:19121/collections/test_collection/indexes"
```

### `/collections/{collection_name}/partitions` (GET)

Gets all partitions in a collection starting from `offset` and ends with `page_size`.

#### Request

| Request Component | Value                                       |
| ----------------- | ------------------------------------------- |
| Name              | `/collections/{collection_name}/partitions` |
| Header            | `accept: application/json`                  |
| Body              | N/A                                         |
| Method            | GET                                         |

##### Query Parameters

| Parameter         | Description                                                   | Required? |
| ----------------- | ------------------------------------------------------------- | --------- |
| `collection_name` | Name of the collection.                                       | Yes       |
| `offset`          | Row offset from which the data page starts. The default is 0. | No        |
| `page_size`       | Size of the data page. The default is 10.                     | No        |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | The required resource does not exist.                             |

#### Example

##### Request

```shell
$ curl -X GET "http://127.0.0.1:19121/collections/test_collection/partitions?offset=0&page_size=3" -H "accept: application/json"
```

##### Response

```json
{
  "partitions": [
    { "partition_tag": "_default" },
    { "partition_tag": "test_tag" },
    { "partition_tag": "test_2" }
  ],
  "count": 10
}
```

### `/collections/{collection_name}/partitions` (POST)

Creates a partition in a collection.

#### Request

| Request Component | Value                                       |
| ----------------- | ------------------------------------------- |
| Name              | `/collections/{collection_name}/partitions` |
| Header            | `accept: application/json`                  |
| Body              | N/A                                         |
| Method            | POST                                        |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 201         | Created                                                           |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | The required resource does not exist.                             |

#### Example

##### Request

```shell
$ curl -X POST "http://127.0.0.1:19121/collections/test_collection/partitions" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"partition_tag\": \"test\"}"
```

##### Response

```json
{ "message": "OK", "code": 0 }
```

### `/collections/{collection_name}/partitions` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component | Value                                       |
| ----------------- | ------------------------------------------- |
| Name              | `/collections/{collection_name}/partitions` |
| Header            | N/A                                         |
| Body              | N/A                                         |
| Method            | OPTIONS                                     |

##### Query Parameters

| Parameter         | Description             | Required? |
| ----------------- | ----------------------- | --------- |
| `collection_name` | Name of the collection. | Yes       |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://127.0.0.1:19121/collections/test_collection/partitions"
```

### `/collections/{collection_name}/partitions` (DELETE)

Deletes a partition by tag.

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/collections/{collection_name}/partitions</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "partition_tag": string
}
</code></pre> </td></tr>
<tr><td>Method</td><td>POST</td></tr>

</table>

##### Query Parameters

| Parameter         | Description                                         | Required? |
| ----------------- | --------------------------------------------------- | --------- |
| `collection_name` | Name of the collection that contains the partition. | Yes       |
| `partition_tag`   | Tag of the partition to delete.                     | yes       |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 204         | Deleted                                                           |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | The requested resource does not exist.                            |

#### Example

##### Request

```shell
$ curl -X DELETE "http://127.0.0.1:19121/collections/test_collection/partitions" -H "accept: application/json" -d "{\"partition_tag\": \"tags_01\"}"
```

The deletion is successful if no information is returned.

### `/collections/{collection_name}/partitions/{partition_tag}/entities?offset=0&page_size=1` (GET)
##### Response

```json
{
    "entities": [
        {
            "__id": "1578989029645098000",
            "field_1": 1,
            "field_vec": []
        },
        {
            "__id": "1578989029645098001",
            "field_1": 2,
            "field_vec": []
        }
    ]
}
```


### `/collections/{collection_name}/segments/{segment_name}/ids` (GET)

Gets all vector ids of segment in a collection starting from `offset` and ends with `page_size`.

#### Request

| Request Component | Value                                     |
| ----------------- | ----------------------------------------- |
| Name              | `/collections/{collection_name}/segments` |
| Header            | `accept: application/json`                |
| Body              | N/A                                       |
| Method            | GET                                       |

##### Query Parameters

| Parameter         | Description                                                   | Required? |
| ----------------- | ------------------------------------------------------------- | --------- |
| `collection_name` | Name of the collection.                                       | Yes       |
| `segment_name`    | Name of the segment.                                          | Yes       |
| `offset`          | Row offset from which the data page starts. The default is 0. | No        |
| `page_size`       | Size of the data page. The default is 10.                     | No        |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | The required resource does not exist.                             |

#### Example

##### Request

```shell
$ curl -X GET "http://127.0.0.1:19121/collections/test_collection/segments/1583727470444700000/ids?offset=0&page_size=1" -H "accept: application/json"
```

##### Response

```json
{
  "ids": ["1583727470435045000"],
  "count": 10000
}
```

### `/collections/{collection_name}/entities` (GET)

1. Searches entities in a collection.

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/tables/{table_name}/entities</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
    "query": {
        "bool": {
            "must":[
                {"vector": 
                    {
                        "field_vec": {
                            "nprobe": 64,
                            "topk": 100,
                            "metric_type": "L2"
                            "values": [
                                [0.1, 0.2, "..."],
                                "..."
                            ]
                        }
                    }
                }
            ],
        }
    },
    "fields": ["field_vec"]
}
</code></pre> </td></tr>
<tr><td>Method</td><td>PUT</td></tr>
</table>

##### Body Parameters

| Parameter  | Description                                                                                                                                                                                  | Required? |
| ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `dsl`      | query dsl.                                                                                                                                                                            | Yes       |
| `params`   | Extra params for search. Please refer to [Index and search parameters](#Index-and-search-parameters) to get more detail information.                                                                                        | Yes       |

> Note: Type of items of vectors depends on the metric used by the collection. If the collection uses `L2` or `IP`, you must use `float`. If the collection uses `HAMMING`, `JACCARD`, or `TANIMOTO`, you must use `uint8`.

##### Query Parameters

| Parameter         | Description             | Required? |
| ----------------- | ----------------------- | --------- |
| `collection_name` | Name of the collection. | Yes       |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | The required resource does not exist.                             |

#### Example

##### Request

```shell
$ curl -X PUT "http://127.0.0.1:19121/collections/test_collection/entities" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"search\":{\"topk\":2,\"vectors\":[[0.1]],\"params\":{\"nprobe\":16}}}"
```

##### Response

```json
{
    "num": 2,
    "result": [
        [
            {
                "id": "1578989029645098000",
                "distance": "0.000000",
                "entity": {
                    "field_1": 1,
                    "field_2": 2,
                    "field_vec": []
                } 
            },
            {
                "id": "1578989029645098001",
                "distance": "0.010000",
                "entity": {
                    "field_1": 10,
                    "field_2": 20,
                    "field_vec": []
                } 
            }
        ]
    ]
}
```

2. Obtain entities by ID.

#### Request

| Request Component | Value                                    |
| ----------------- | ---------------------------------------- |
| Name              | `/collections/{collection_name}/entities`|
| Header            | `accept: application/json`               |
| Body              | N/A                                      |
| Method            | GET                                      |

#### Query Parameters

| Parameter         | Description                           | Required? |
| ----------------- | ------------------------------------- | --------- |
| `collection_name` | Name of the collection.               | Yes       |
| `entitity_id_list`  | Vector id list, separated by commas.  | Yes       |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 201         | Created                                                           |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | The required resource does not exist.                             |

#### Example

##### Request

```shell
$ curl -X GET "http://127.0.0.1:19121/collections/test_collection/entities?ids=1578989029645098000,1578989029645098001" -H "accept: application/json" -H "Content-Type: application/json"
```

##### Response

```json
{
    "entities": [
        {
            "__id": "1578989029645098000",
            "field_1": 1,
            "field_vec": []
        },
        {
            "__id": "1578989029645098001",
            "field_1": 2,
            "field_vec": []
        }
    ]
}
```

3. get page entities

#### Request

| Request Component | Value                                    |
| ----------------- | ---------------------------------------- |
| Name              | `/collections/{collection_name}/entities`|
| Header            | `accept: application/json`               |
| Body              | N/A                                      |
| Method            | GET                                      |

#### Query Parameters

| Parameter         | Description                           | Required? |
| ----------------- | ------------------------------------- | --------- |
| `collection_name` | Name of the collection.               | Yes       |
| `page_size`       | page size, separated by commas.       | Yes       |
| `offset`          | offset, separated by commas.          | Yes       |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 201         | Created                                                           |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | The required resource does not exist.                             |

#### Example

##### Request

```shell
$ curl -X GET "http://127.0.0.1:19121/collections/test_collection/entities?page_size=10&offset=0" -H "accept: application/json" -H "Content-Type: application/json"
```

##### Response

```json
{
    "entities": [
        {
            "__id": "1578989029645098000",
            "field_1": 1,
            "field_vec": []
        },
        {
            "__id": "1578989029645098001",
            "field_1": 2,
            "field_vec": []
        }
    ]
}
```

### `/collections/{collection_name}/entities` (DELETE)

Delete entities

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/tables/{table_name}/entities</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "delete": {
     "ids": [$string]
  }
}
</code></pre> </td></tr>
<tr><td>Method</td><td>PUT</td></tr>
</table>

##### Body Parameters

| Parameter | Description     | Required? |
| --------- | --------------- | --------- |
| ids       | IDs of entities. | Yes       |

##### Query Parameters

| Parameter         | Description             | Required? |
| ----------------- | ----------------------- | --------- |
| `collection_name` | Name of the collection. | Yes       |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | The required resource does not exist.                             |

#### Example

##### Request

```shell
$ curl -X PUT "http://127.0.0.1:19121/collections/test_collection/entities" -H "accept: application/json" -H "Content-Type: application/json" -d "{"delete": {"ids": ["1578989029645098000"]}}"
```

##### Response

```json
{ "code": 0, "message": "success" }
```

### `/collections/{collection_name}/entities` (POST)

Inserts entities to a collection.

> Note: It is recommended that you do not insert more than 1 million entities per request.

#### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/tables/{table_name}/entities</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
    "partition_tag": "part",
    "entities": {
        "__id": [123456789,234567]
        "field_1": [1, 2]
        "field_vec": [[], []]
    }
}
</code></pre> </td></tr>
<tr><td>Method</td><td>POST</td></tr>
</table>

##### Body Parameters

| Parameter       | Description                                                                                                                                                                                                                      | Required? |
| --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `partition_tag` | Tag of the partition to insert entities to.                                                                                                                                                                                       | No        |
| `entities`      | entities to insert to the collection.                                                                                                                                                                                             | Yes       |
| `ids`           | IDs of the entities to insert to the collection. If you assign IDs to the entities, you must provide IDs for all vectors in the collection. If you do not specify this parameter, Milvus automatically assigns IDs to the vectors. | No        |

> Note: Type of items of `entities` depends on the metric used by the collection. If the collection uses `L2` or `IP`, you must use `float`. If the collection uses `HAMMING`, `JACCARD`, or `TANIMOTO`, you must use `uint8`.

##### Query Parameters

| Parameter         | Description             | Required? |
| ----------------- | ----------------------- | --------- |
| `collection_name` | Name of the collection. | Yes       |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 201         | Created                                                           |
| 400         | The request is incorrect. Refer to the error message for details. |
| 404         | The required resource does not exist.                             |

#### Example

##### Request

```shell
$ curl -X POST "http://127.0.0.1:19121/collections/test_collection/entities" -H "accept: application/json" -H "Content-Type: application/json" -d "{\"vectors\":[[0.1],[0.2],[0.3],[0.4]]}"
```

##### Response

```json
{
  "ids": [
    "1578989029645098000",
    "1578989029645098001",
    "1578989029645098002",
    "1578989029645098003"
  ]
}
```

### `/collections/{collection_name}/entities` (OPTIONS)

Use this API for Cross-Origin Resource Sharing (CORS).

#### Request

| Request Component | Value                                    |
| ----------------- | ---------------------------------------- |
| Name              | `/collections/{collection_name}/entities` |
| Header            | N/A                                      |
| Body              | N/A                                      |
| Method            | OPTIONS                                  |

#### Example

##### Request

```shell
$ curl -X OPTIONS "http://127.0.0.1:19121/collections/test_collection/entities"
```

### `/system/{msg}` (GET)

Gets information about the Milvus server.

#### Request

| Request Component | Value                      |
| ----------------- | -------------------------- |
| Name              | `/system/{msg}`            |
| Header            | `accept: application/json` |
| Body              | N/A                        |
| Method            | GET                        |

##### Query Parameters

| Parameter | Description                                                       | Required? |
| --------- | ----------------------------------------------------------------- | --------- |
| `msg`     | Type of the message to return. You can use `status` or `version`. | Yes       |

#### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |

#### Example

##### Request

```shell
$ curl -X GET "http://127.0.0.1:19121/system/version" -H "accept: application/json"
```

##### Response

```json
{"code":0,"message":"OK","reply": "0.8.0" }
```

### `system/{op}` (PUT)

#### Flush a collection

##### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/system/task</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "flush": {
     "collection_names": [$string]
  }
}
</code></pre> </td></tr>
<tr><td>Method</td><td>PUT</td></tr>
</table>

##### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |

##### Example

###### Request

```shell
$ curl -X PUT "http://127.0.0.1:19121/system/task" -H "accept: application/json" -d "{\"flush\": {\"collection_names\": [\"test_collection\"]}}"
```

###### Response

```json
{ "code": 0, "message": "success" }
```

#### Compact segments in a collection

##### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/system/task</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "compact": {
     "collection_name": $string,
     "threshold": 0.1
  }
}
</code></pre> </td></tr>
<tr><td>Method</td><td>PUT</td></tr>
</table>

##### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |

##### Example

###### Request

```shell
$ curl -X PUT "http://127.0.0.1:19121/system/task" -H "accept: application/json" -d "{\"compact\": {\"collection_name\": \"test_collection\", \"threshold\": 0.1}}"
```

###### Response

```json
{ "code": 0, "message": "success" }
```

#### Load a collection to memory

##### Request

<table>
<tr><th>Request Component</th><th>Value</th></tr>
<tr><td> Name</td><td><pre><code>/system/task</code></pre></td></tr>
<tr><td>Header </td><td><pre><code>accept: application/json</code></pre> </td></tr>
<tr><td>Body</td><td><pre><code>
{
  "load": {
     "collection_name": $string
  }
}
</code></pre> </td></tr>
<tr><td>Method</td><td>PUT</td></tr>
</table>

##### Response

| Status code | Description                                                       |
| ----------- | ----------------------------------------------------------------- |
| 200         | The request is successful.                                        |
| 400         | The request is incorrect. Refer to the error message for details. |

##### Example

###### Request

```shell
$ curl -X PUT "http://127.0.0.1:19121/system/task" -H "accept: application/json" -d "{\"load\": {\"collection_name\": \"test_collection\"}}"
```

###### Response

```json
{ "code": 0, "message": "success" }
```

## Index and search parameters

For each index type, the RESTful API has specific index parameters and search parameters.

<table>
<tr><th>Index type</th><th>Create index param</th><th>Search param</th></tr>
<tr>
 <td> IVFFLAT</td>
 <td><pre><code>{"nlist": $int}</code></pre></td>
 <td><pre><code>{"nprobe": $int}</code></pre></td>
</tr>
<tr>
 <td> IVFPQ</td>
 <td><pre><code>{"m": $int, "nlist": $int}</code></pre></td>
 <td><pre><code>{"nprobe": $int}</code></pre></td>
</tr>
<tr>
 <td> IVFSQ8</td>
 <td><pre><code>{"nlist": $int}</code></pre></td>
 <td><pre><code>{"nprobe": $int}</code></pre></td>
</tr>
<tr>
 <td> IVFSQ8H</td>
 <td><pre><code>{"nlist": $int}</code></pre></td>
 <td><pre><code>{"nprobe": $int}</code></pre></td>
</tr>
<tr>
 <td> HNSW</td>
 <td><pre><code>{"M": $int, "efConstruction": $int}</code></pre></td>
 <td><pre><code>{"ef": $int}</code></pre></td>
</tr>
<tr>
 <td> ANNOY</td>
 <td><pre><code>{"n_trees": $int}</code></pre></td>
 <td><pre><code>{"search_k": $int}</code></pre></td>
</tr>
</table>

For detailed information about the parameters above, refer to [Milvus Indexes](https://milvus.io/docs/guides/index.md)

## Error Codes

The RESTful API returns error messages as JSON text. Each type of error message has a specific error code.

| Type                  | Code |
| --------------------- | ---- |
| SUCCESS               | 0    |
| UNEXPECTED_ERROR      | 1    |
| CONNECT_FAILED        | 2    |
| PERMISSION_DENIED     | 3    |
| TABLE_NOT_EXISTS      | 4    |
| ILLEGAL_ARGUMENT      | 5    |
| ILLEGAL_RANGE         | 6    |
| ILLEGAL_DIMENSION     | 7    |
| ILLEGAL_INDEX_TYPE    | 8    |
| ILLEGAL_TABLE_NAME    | 9    |
| ILLEGAL_TOPK          | 10   |
| ILLEGAL_ROWRECORD     | 11   |
| ILLEGAL_VECTOR_ID     | 12   |
| ILLEGAL_SEARCH_RESULT | 13   |
| FILE_NOT_FOUND        | 14   |
| META_FAILED           | 15   |
| CACHE_FAILED          | 16   |
| CANNOT_CREATE_FOLDER  | 17   |
| CANNOT_CREATE_FILE    | 18   |
| CANNOT_DELETE_FOLDER  | 19   |
| CANNOT_DELETE_FILE    | 20   |
| BUILD_INDEX_ERROR     | 21   |
| ILLEGAL_NLIST         | 22   |
| ILLEGAL_METRIC_TYPE   | 23   |
| OUT_OF_MEMORY         | 24   |
| PATH_PARAM_LOSS       | 31   |
| UNKNOWN_PATH          | 32   |
| QUERY_PARAM_LOSS      | 33   |
| BODY_FIELD_LOSS       | 34   |
| ILLEGAL_BODY          | 35   |
| BODY_PARSE_FAIL       | 36   |
| ILLEGAL_QUERY_PARAM   | 37   |
