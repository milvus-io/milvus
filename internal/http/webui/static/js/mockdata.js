var sysmetrics = `{
  "nodes_info": [
    {
      "identifier": 1,
      "connected": null,
      "infos": {
        "has_error": false,
        "error_reason": "",
        "name": "querynode1",
        "hardware_infos": {
          "ip": "172.18.20.7:21123",
          "cpu_core_count": 8,
          "cpu_core_usage": 0,
          "memory": 34359738368,
          "memory_usage": 18362302464,
          "disk": 104857600,
          "disk_usage": 2097152
        },
        "system_info": {
          "system_version": "34cf5352ec",
          "deploy_mode": "STANDALONE[MOCK]",
          "build_version": "v2.2-testing-20240702-804-g34cf5352ec-dev",
          "build_time": "Wed, Oct 23, 2024 13:58:54 UTC",
          "used_go_version": "go version go1.22.3 darwin/amd64"
        },
        "created_time": "2024-10-23 22:01:45.891414 +0800 CST m=+8.035797839",
        "updated_time": "2024-10-23 22:01:45.891415 +0800 CST m=+8.035798239",
        "type": "querynode",
        "id": 1,
        "system_configurations": {
          "simd_type": "auto"
        },
        "quota_metrics": {
          "Hms": {
            "ip": "172.18.20.7:21123",
            "cpu_core_count": 8,
            "cpu_core_usage": 0,
            "memory": 34359738368,
            "memory_usage": 18362302464,
            "disk": 104857600,
            "disk_usage": 2097152
          },
          "Rms": [
            {
              "Label": "InsertConsumeThroughput",
              "Rate": 0
            },
            {
              "Label": "DeleteConsumeThroughput",
              "Rate": 0
            }
          ],
          "Fgm": {
            "MinFlowGraphChannel": "",
            "MinFlowGraphTt": 18446744073709551615,
            "NumFlowGraph": 0
          },
          "GrowingSegmentsSize": 0,
          "Effect": {
            "NodeID": 1,
            "CollectionIDs": []
          },
          "DeleteBufferInfo": {
            "CollectionDeleteBufferNum": {},
            "CollectionDeleteBufferSize": {}
          }
        },
        "collection_metrics": {
          "CollectionRows": {}
        }
      }
    },
    {
      "identifier": 1,
      "connected": [
        {
          "connected_identifier": 1,
          "type": "manage",
          "target_type": "querynode"
        }
      ],
      "infos": {
        "has_error": false,
        "error_reason": "",
        "name": "querycoord1",
        "hardware_infos": {
          "ip": "172.18.20.7:19531",
          "cpu_core_count": 8,
          "cpu_core_usage": 0,
          "memory": 34359738368,
          "memory_usage": 18362302464,
          "disk": 104857600,
          "disk_usage": 2097152
        },
        "system_info": {
          "system_version": "34cf5352ec",
          "deploy_mode": "STANDALONE[MOCK]",
          "build_version": "v2.2-testing-20240702-804-g34cf5352ec-dev",
          "build_time": "Wed, Oct 23, 2024 13:58:54 UTC",
          "used_go_version": "go version go1.22.3 darwin/amd64"
        },
        "created_time": "2024-10-23 22:01:45.891414 +0800 CST m=+8.035797839",
        "updated_time": "2024-10-23 22:01:45.891415 +0800 CST m=+8.035798239",
        "type": "querycoord",
        "id": 1,
        "system_configurations": {
          "search_channel_prefix": "",
          "search_result_channel_prefix": ""
        }
      }
    },
    {
      "identifier": 1,
      "connected": null,
      "infos": {
        "has_error": false,
        "error_reason": "",
        "name": "datanode1",
        "hardware_infos": {
          "ip": "172.18.20.7:21124",
          "cpu_core_count": 8,
          "cpu_core_usage": 0,
          "memory": 34359738368,
          "memory_usage": 18362302464,
          "disk": 104857600,
          "disk_usage": 2097152
        },
        "system_info": {
          "system_version": "34cf5352ec",
          "deploy_mode": "STANDALONE[MOCK]",
          "build_version": "v2.2-testing-20240702-804-g34cf5352ec-dev",
          "build_time": "Wed, Oct 23, 2024 13:58:54 UTC",
          "used_go_version": "go version go1.22.3 darwin/amd64"
        },
        "created_time": "2024-10-23 22:01:45.891414 +0800 CST m=+8.035797839",
        "updated_time": "2024-10-23 22:01:45.891415 +0800 CST m=+8.035798239",
        "type": "datanode",
        "id": 1,
        "system_configurations": {
          "flush_insert_buffer_size": 16777216
        },
        "quota_metrics": {
          "Hms": {
            "ip": "172.18.20.7:21124",
            "cpu_core_count": 8,
            "cpu_core_usage": 0,
            "memory": 34359738368,
            "memory_usage": 18362302464,
            "disk": 104857600,
            "disk_usage": 2097152
          },
          "Rms": [
            {
              "Label": "InsertConsumeThroughput",
              "Rate": 0
            },
            {
              "Label": "DeleteConsumeThroughput",
              "Rate": 0
            }
          ],
          "Fgm": {
            "MinFlowGraphChannel": "",
            "MinFlowGraphTt": 18446744073709551615,
            "NumFlowGraph": 0
          },
          "Effect": {
            "NodeID": 1,
            "CollectionIDs": []
          }
        }
      }
    },
    {
      "identifier": 1,
      "connected": null,
      "infos": {
        "has_error": false,
        "error_reason": "",
        "name": "indexnode1",
        "hardware_infos": {
          "ip": "172.18.20.7:21121",
          "cpu_core_count": 8,
          "cpu_core_usage": 0,
          "memory": 34359738368,
          "memory_usage": 18362302464,
          "disk": 104857600,
          "disk_usage": 2097152
        },
        "system_info": {
          "system_version": "34cf5352ec",
          "deploy_mode": "STANDALONE[MOCK]",
          "build_version": "v2.2-testing-20240702-804-g34cf5352ec-dev",
          "build_time": "Wed, Oct 23, 2024 13:58:54 UTC",
          "used_go_version": "go version go1.22.3 darwin/amd64"
        },
        "created_time": "2024-10-23 22:01:45.891414 +0800 CST m=+8.035797839",
        "updated_time": "2024-10-23 22:01:45.891415 +0800 CST m=+8.035798239",
        "type": "indexnode",
        "id": 1,
        "system_configurations": {
          "minio_bucket_name": "a-bucket",
          "simd_type": "auto"
        }
      }
    },
    {
      "identifier": 1,
      "connected": [
        {
          "connected_identifier": 1,
          "type": "manage",
          "target_type": "datanode"
        },
        {
          "connected_identifier": 1,
          "type": "manage",
          "target_type": "indexnode"
        }
      ],
      "infos": {
        "has_error": false,
        "error_reason": "",
        "name": "datacoord1",
        "hardware_infos": {
          "ip": "172.18.20.7:13333",
          "cpu_core_count": 8,
          "cpu_core_usage": 0,
          "memory": 34359738368,
          "memory_usage": 18362302464,
          "disk": 104857600,
          "disk_usage": 2097152
        },
        "system_info": {
          "system_version": "34cf5352ec",
          "deploy_mode": "STANDALONE[MOCK]",
          "build_version": "v2.2-testing-20240702-804-g34cf5352ec-dev",
          "build_time": "Wed, Oct 23, 2024 13:58:54 UTC",
          "used_go_version": "go version go1.22.3 darwin/amd64"
        },
        "created_time": "2024-10-23 22:01:45.891414 +0800 CST m=+8.035797839",
        "updated_time": "2024-10-23 22:01:45.891415 +0800 CST m=+8.035798239",
        "type": "datacoord",
        "id": 1,
        "system_configurations": {
          "segment_max_size": 1024
        },
        "quota_metrics": {
          "TotalBinlogSize": 0,
          "CollectionBinlogSize": {},
          "PartitionsBinlogSize": {},
          "CollectionL0RowCount": {}
        },
        "collection_metrics": {
          "Collections": {}
        }
      }
    },
    {
      "identifier": 1,
      "connected": [],
      "infos": {
        "has_error": false,
        "error_reason": "",
        "name": "rootcoord1",
        "hardware_infos": {
          "ip": "172.18.20.7:53100",
          "cpu_core_count": 8,
          "cpu_core_usage": 0,
          "memory": 34359738368,
          "memory_usage": 18362302464,
          "disk": 104857600,
          "disk_usage": 2097152
        },
        "system_info": {
          "system_version": "34cf5352ec",
          "deploy_mode": "STANDALONE[MOCK]",
          "build_version": "v2.2-testing-20240702-804-g34cf5352ec-dev",
          "build_time": "Wed, Oct 23, 2024 13:58:54 UTC",
          "used_go_version": "go version go1.22.3 darwin/amd64"
        },
        "created_time": "2024-10-23 22:01:45.891414 +0800 CST m=+8.035797839",
        "updated_time": "2024-10-23 22:01:45.891415 +0800 CST m=+8.035798239",
        "type": "rootcoord",
        "id": 1,
        "system_configurations": {
          "min_segment_size_to_enable_index": 1024
        }
      }
    },
    {
      "identifier": 1,
      "connected": [
        {
          "connected_identifier": 1,
          "type": "forward",
          "target_type": "querycoord"
        },
        {
          "connected_identifier": 1,
          "type": "forward",
          "target_type": "datacoord"
        },
        {
          "connected_identifier": 1,
          "type": "forward",
          "target_type": "rootcoord"
        }
      ],
      "infos": {
        "has_error": false,
        "error_reason": "",
        "name": "proxy1",
        "hardware_infos": {
          "ip": "172.18.20.7:19529",
          "cpu_core_count": 8,
          "cpu_core_usage": 30.52004762940343,
          "memory": 34359738368,
          "memory_usage": 18362302464,
          "disk": 104857600,
          "disk_usage": 2097152
        },
        "system_info": {
          "system_version": "34cf5352ec",
          "deploy_mode": "STANDALONE[MOCK]",
          "build_version": "v2.2-testing-20240702-804-g34cf5352ec-dev",
          "build_time": "Wed, Oct 23, 2024 13:58:54 UTC",
          "used_go_version": "go version go1.22.3 darwin/amd64"
        },
        "created_time": "2024-10-23 22:01:45.891414 +0800 CST m=+8.035797839",
        "updated_time": "2024-10-23 22:01:45.891415 +0800 CST m=+8.035798239",
        "type": "proxy",
        "id": 1,
        "system_configurations": {
          "default_partition_name": "_default",
          "default_index_name": "_default_idx"
        },
        "quota_metrics": null
      }
    }
  ]
}`

var clientInfos = `[
  {
    "sdk_type": "python",
    "sdk_version": "1.0.0",
    "local_time": "2023-10-01T12:00:00Z",
    "user": "user1",
    "host": "127.0.0.1",
    "reserved": {
      "last_active_time": ""
    }
  },
  {
    "sdk_type": "golang",
    "sdk_version": "1.1.0",
    "local_time": "2023-10-01T12:05:00Z",
    "user": "user2",
    "host": "127.0.0.2",
    "reserved": {
      "last_active_time": ""
    }
  }
]`

var dependencies = `
{
  "metastore": {
    "health_status": true,
    "unhealthy_reason": "",
    "members_health": [
      {
        "endpoint": "http://127.0.0.1:2379",
        "health": true
      }
    ],
    "meta_type": "etcd"
  },
  "mq": {
    "health_status": false,
    "unhealthy_reason": "health check failed, err: Get \\"http://localhost:80/admin/v2/brokers/health\\": dial tcp [::1]:80: connect: connection refused",
    "members_health": null,
    "mq_type": "pulsar"
  }
}
`

var mconfigs = `
{
    "MILVUS_GIT_BUILD_TAGS": "v2.2-testing-20240702-811-g38211f2b81-dev",
    "MILVUS_GIT_COMMIT": "38211f2b81",
    "common.bloomfilterapplybatchsize": "1000",
    "common.bloomfiltersize": "100000",
    "common.bloomfiltertype": "BlockedBloomFilter",
    "common.buildindexthreadpoolratio": "0.75",
    "common.defaultindexname": "_default_idx",
    "common.defaultpartitionname": "_default",
    "common.diskindex.beamwidthratio": "4",
    "common.diskindex.buildnumthreadsratio": "1",
    "common.diskindex.loadnumthreadratio": "8",
    "common.diskindex.maxdegree": "56",
    "common.diskindex.pqcodebudgetgbratio": "0.125",
    "common.diskindex.searchcachebudgetgbratio": "0.1",
    "common.diskindex.searchlistsize": "100",
    "common.enablevectorclusteringkey": "false",
    "common.entityexpiration": "-1",
    "common.gracefulstoptimeout": "1800",
    "common.gracefultime": "5000",
    "common.indexslicesize": "16",
    "common.locks.metrics.enable": "false"
}
`;

var qcTasks = `
[
  {
    "task_name": "balance_checker-ChannelTask[1]-ch1",
    "collection_id": 67890,
    "replica_id": 11111,
    "task_type": "Move",
    "task_status": "started",
    "priority": "Normal",
    "actions": [
      "type:Grow node id : 1 channel name:channel_1"
    ],
    "step": 1,
    "reason": "some reason"
  },
  {
    "task_name": "index_checker-SegmentTask[2]-54321",
    "collection_id": 12345,
    "replica_id": 22222,
    "task_type": "Grow",
    "task_status": "completed",
    "priority": "High",
    "actions": [
      "type:Grow node id: 2 segment id:123 scope:DataScope_Streaming"
    ],
    "step": 2,
    "reason": "another reason"
  },
  {
    "task_name": "leader_checker-LeaderSegmentTask[3]-1",
    "collection_id": 54321,
    "replica_id": 33333,
    "task_type": "Grow",
    "task_status": "failed",
    "priority": "Low",
    "actions": [
      "type:Grow node id: 3 leader id:456 segment id:789 version:1"
    ],
    "step": 3,
    "reason": "yet another reason"
  }
]
`

var dc_build_index_task = `
[
  {
    "index_id": 1,
    "collection_id": 1001,
    "segment_id": 2001,
    "build_id": 3001,
    "index_state": "Finished",
    "index_size": 1024,
    "index_version": 1,
    "create_time": 1633036800
  },
  {
    "index_id": 2,
    "collection_id": 1002,
    "segment_id": 2002,
    "build_id": 3002,
    "index_state": "Failed",
    "fail_reason": "Disk full",
    "index_size": 2048,
    "index_version": 2,
    "create_time": 1633123200
  }
]`

var dc_compaction_task = `
[
  {
    "plan_id": 1,
    "collection_id": 1001,
    "type": "Merge",
    "state": "Completed",
    "start_time": 1633036800,
    "end_time": 1633040400,
    "total_rows": 100000,
    "input_segments": [1, 2, 3],
    "result_segments": [4]
  },
  {
    "plan_id": 2,
    "collection_id": 1002,
    "type": "Merge",
    "state": "Failed",
    "fail_reason": "Disk full",
    "start_time": 1633123200,
    "end_time": 1633126800,
    "total_rows": 200000,
    "input_segments": [5, 6, 7],
    "result_segments": [8]
  }
]`

var dc_sync_task = `
[
  {
    "segment_id": 1,
    "batch_rows": 1000,
    "segment_level": "L1",
    "ts_from": 1633036800,
    "ts_to": 1633040400,
    "delta_row_count": 10,
    "flush_size": 1024,
    "running_time": 100000000
  },
  {
    "segment_id": 2,
    "batch_rows": 2000,
    "segment_level": "L2",
    "ts_from": 1633123200,
    "ts_to": 1633126800,
    "delta_row_count": 20,
    "flush_size": 2048,
    "running_time": 200000000
  }
]
`

var dc_import_task = `
[
  {
    "job_id": 1,
    "task_id": 2,
    "collection_id": 3,
    "node_id": 4,
    "state": "Pending",
    "reason": "",
    "task_type": "PreImportTask",
    "created_time": "2023-10-01T00:00:00Z",
    "complete_time": "2023-10-01T01:00:00Z"
  },
  {
    "job_id": 5,
    "task_id": 6,
    "collection_id": 7,
    "node_id": 8,
    "state": "ImportTaskStateCompleted",
    "reason": "",
    "task_type": "Completed",
    "created_time": "2023-10-01T00:00:00Z",
    "complete_time": "2023-10-01T01:00:00Z"
  },
  {
    "job_id": 9,
    "task_id": 10,
    "collection_id": 11,
    "node_id": 12,
    "state": "Failed",
    "reason": "some failure reason",
    "task_type": "ImportTask",
    "created_time": "2023-10-01T00:00:00Z",
    "complete_time": "2023-10-01T01:00:00Z"
  }
]
`