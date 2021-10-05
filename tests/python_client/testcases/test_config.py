import time
import random
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from utils.utils import *
import ujson

# CONFIG_TIMEOUT = 80

# class TestCacheConfig:
#     """
#     ******************************************************************
#       The following cases are used to test `get_config` function
#     ******************************************************************
#     """
#     @pytest.fixture(scope="function", autouse=True)
#     def skip_http_check(self, args):
#         if args["handler"] == "HTTP":
#             pytest.skip("skip in http mode")
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def reset_configs(self, connect):
#         '''
#         reset configs so the tests are stable
#         '''
#         relpy = connect.set_config("cache.cache_size", '4GB')
#         config_value = connect.get_config("cache.cache_size")
#         assert config_value == '4GB'
#         #relpy = connect.set_config("cache", "insert_buffer_size", '2GB')
#         #config_value = connect.get_config("cache", "insert_buffer_size")
#         #assert config_value == '1073741824'
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_cache_size_invalid_parent_key(self, connect, collection):
#         '''
#         target: get invalid parent key
#         method: call get_config without parent_key: cache
#         expected: status not ok
#         '''
#         invalid_configs = ["Cache_config", "cache config", "cache_Config", "cacheconfig"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config(config+str(".cache_size"))
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_cache_size_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: cache_size
#         expected: status not ok
#         '''
#         invalid_configs = ["Cpu_cache_size", "cpu cache_size", "cpucachecapacity"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("cache."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_cache_size_valid(self, connect, collection):
#         '''
#         target: get cache_size
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("cache.cache_size")
#         assert config_value
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_insert_buffer_size_invalid_parent_key(self, connect, collection):
#         '''
#         target: get invalid parent key
#         method: call get_config without parent_key: cache
#         expected: status not ok
#         '''
#         invalid_configs = ["Cache_config", "cache config", "cache_Config", "cacheconfig"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config(config+".insert_buffer_size")
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_insert_buffer_size_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: insert_buffer_size
#         expected: status not ok
#         '''
#         invalid_configs = ["Insert_buffer size", "insert buffer_size", "insertbuffersize"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("cache."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_insert_buffer_size_valid(self, connect, collection):
#         '''
#         target: get insert_buffer_size
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("cache.insert_buffer_size")
#         assert config_value
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_preload_collection_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: preload_collection
#         expected: status not ok
#         '''
#         invalid_configs = ["preloadtable", "preload collection "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("cache."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_preload_collection_valid(self, connect, collection):
#         '''
#         target: get preload_collection
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("cache.preload_collection")
#         assert config_value == ''
#
#     """
#     ******************************************************************
#       The following cases are used to test `set_config` function
#     ******************************************************************
#     """
#     def get_memory_available(self, connect):
#         info = connect._cmd("get_system_info")
#         mem_info = ujson.loads(info)
#         mem_total = int(mem_info["memory_total"])
#         mem_used = int(mem_info["memory_used"])
#         logging.getLogger().info(mem_total)
#         logging.getLogger().info(mem_used)
#         mem_available = mem_total - mem_used
#         return int(mem_available / 1024 / 1024 / 1024)
#
#     def get_memory_total(self, connect):
#         info = connect._cmd("get_system_info")
#         mem_info = ujson.loads(info)
#         mem_total = int(mem_info["memory_total"])
#         return int(mem_total / 1024 / 1024 / 1024)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_cache_size_invalid_parent_key(self, connect, collection):
#         '''
#         target: set invalid parent key
#         method: call set_config without parent_key: cache
#         expected: status not ok
#         '''
#         self.reset_configs(connect)
#         invalid_configs = ["Cache_config", "cache config", "cache_Config", "cacheconfig"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config(config+".cache_size", '4294967296')
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_cache_invalid_child_key(self, connect, collection):
#         '''
#         target: set invalid child key
#         method: call set_config with invalid child_key
#         expected: status not ok
#         '''
#         self.reset_configs(connect)
#         invalid_configs = ["abc", 1]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("cache."+config, '4294967296')
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_cache_size_valid(self, connect, collection):
#         '''
#         target: set cache_size
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         self.reset_configs(connect)
#         relpy = connect.set_config("cache.cache_size", '2147483648')
#         config_value = connect.get_config("cache.cache_size")
#         assert config_value == '2GB'
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_set_cache_size_valid_multiple_times(self, connect, collection):
#         '''
#         target: set cache_size
#         method: call set_config correctly and repeatedly
#         expected: status ok
#         '''
#         self.reset_configs(connect)
#         for i in range(20):
#             relpy = connect.set_config("cache.cache_size", '4294967296')
#             config_value = connect.get_config("cache.cache_size")
#             assert config_value == '4294967296'
#         for i in range(20):
#             relpy = connect.set_config("cache.cache_size", '2147483648')
#             config_value = connect.get_config("cache.cache_size")
#             assert config_value == '2147483648'
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_set_insert_buffer_size_invalid_parent_key(self, connect, collection):
#         '''
#         target: set invalid parent key
#         method: call set_config without parent_key: cache
#         expected: status not ok
#         '''
#         self.reset_configs(connect)
#         invalid_configs = ["Cache_config", "cache config", "cache_Config", "cacheconfig"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config(config+".insert_buffer_size", '1073741824')
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_insert_buffer_size_valid(self, connect, collection):
#         '''
#         target: set insert_buffer_size
#         method: call get_config correctly
#         expected: status ok, set successfully
#         '''
#         self.reset_configs(connect)
#         relpy = connect.set_config("cache.insert_buffer_size", '2GB')
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_set_insert_buffer_size_valid_multiple_times(self, connect, collection):
#         '''
#         target: set insert_buffer_size
#         method: call get_config correctly and repeatedly
#         expected: status ok
#         '''
#         self.reset_configs(connect)
#         for i in range(20):
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("cache.insert_buffer_size", '1GB')
#         for i in range(20):
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("cache.insert_buffer_size", '2GB')
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_cache_out_of_memory_value_A(self, connect, collection):
#         '''
#         target: set cache_size / insert_buffer_size to be out-of-memory
#         method: call set_config with child values bigger than current system memory
#         expected: status not ok (cache_size + insert_buffer_size < system memory)
#         '''
#         self.reset_configs(connect)
#         mem_total = self.get_memory_total(connect)
#         logging.getLogger().info(mem_total)
#         with pytest.raises(Exception) as e:
#             relpy = connect.set_config("cache.cache_size", str(int(mem_total + 1)+''))
#
#
#
# class TestGPUConfig:
#     """
#     ******************************************************************
#       The following cases are used to test `get_config` function
#     ******************************************************************
#     """
#     @pytest.fixture(scope="function", autouse=True)
#     def skip_http_check(self, args):
#         if args["handler"] == "HTTP":
#             pytest.skip("skip in http mode")
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_gpu_search_threshold_invalid_parent_key(self, connect, collection):
#         '''
#         target: get invalid parent key
#         method: call get_config without parent_key: gpu
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Engine_config", "engine config"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config(config+".gpu_search_threshold")
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_gpu_search_threshold_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: gpu_search_threshold
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Gpu_search threshold", "gpusearchthreshold"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("gpu."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_gpu_search_threshold_valid(self, connect, collection):
#         '''
#         target: get gpu_search_threshold
#         method: call get_config correctly
#         expected: status ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         config_value = connect.get_config("gpu.gpu_search_threshold")
#         assert config_value
#
#     """
#     ******************************************************************
#       The following cases are used to test `set_config` function
#     ******************************************************************
#     """
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_gpu_invalid_child_key(self, connect, collection):
#         '''
#         target: set invalid child key
#         method: call set_config with invalid child_key
#         expected: status not ok
#         '''
#         invalid_configs = ["abc", 1]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("gpu."+config, 1000)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_gpu_search_threshold_invalid_parent_key(self, connect, collection):
#         '''
#         target: set invalid parent key
#         method: call set_config without parent_key: gpu
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Engine_config", "engine config"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config(config+".gpu_search_threshold", 1000)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_gpu_search_threshold_valid(self, connect, collection):
#         '''
#         target: set gpu_search_threshold
#         method: call set_config correctly
#         expected: status ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         relpy = connect.set_config("gpu.gpu_search_threshold", 2000)
#         config_value = connect.get_config("gpu.gpu_search_threshold")
#         assert config_value == '2000'
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_gpu_invalid_values(self, connect, collection):
#         '''
#         target: set gpu
#         method: call set_config with invalid child values
#         expected: status not ok
#         '''
#         for i in [-1, "1000\n", "1000\t", "1000.0", 1000.35]:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("gpu.use_blas_threshold", i)
#             if str(connect._cmd("mode")) == "GPU":
#                 with pytest.raises(Exception) as e:
#                     relpy = connect.set_config("gpu.gpu_search_threshold", i)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def reset_configs(self, connect):
#         '''
#         reset configs so the tests are stable
#         '''
#         relpy = connect.set_config("gpu.cache_size", 1)
#         config_value = connect.get_config("gpu.cache_size")
#         assert config_value == '1'
#
#         #follows can not be changed
#         #relpy = connect.set_config("gpu", "enable", "true")
#         #config_value = connect.get_config("gpu", "enable")
#         #assert config_value == "true"
#         #relpy = connect.set_config("gpu", "search_devices", "gpu0")
#         #config_value = connect.get_config("gpu", "search_devices")
#         #assert config_value == 'gpu0'
#         #relpy = connect.set_config("gpu", "build_index_devices", "gpu0")
#         #config_value = connect.get_config("gpu", "build_index_devices")
#         #assert config_value == 'gpu0'
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_gpu_enable_invalid_parent_key(self, connect, collection):
#         '''
#         target: get invalid parent key
#         method: call get_config without parent_key: gpu
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Gpu_resource_config", "gpu resource config", \
#             "gpu_resource"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config(config+".enable")
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_gpu_enable_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: enable
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Enab_le", "enab_le ", "disable", "true"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("gpu."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_gpu_enable_valid(self, connect, collection):
#         '''
#         target: get enable status
#         method: call get_config correctly
#         expected: status ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         config_value = connect.get_config("gpu.enable")
#         assert config_value == "true" or config_value == "false"
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_cache_size_invalid_parent_key(self, connect, collection):
#         '''
#         target: get invalid parent key
#         method: call get_config without parent_key: gpu
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Gpu_resource_config", "gpu resource config", \
#             "gpu_resource"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config(config+".cache_size")
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_cache_size_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: cache_size
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Cache_capacity", "cachecapacity"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("gpu."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_cache_size_valid(self, connect, collection):
#         '''
#         target: get cache_size
#         method: call get_config correctly
#         expected: status ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         config_value = connect.get_config("gpu.cache_size")
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_search_devices_invalid_parent_key(self, connect, collection):
#         '''
#         target: get invalid parent key
#         method: call get_config without parent_key: gpu
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Gpu_resource_config", "gpu resource config", \
#             "gpu_resource"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config(config+".search_devices")
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_search_devices_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: search_devices
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Search_resources"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("gpu."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_search_devices_valid(self, connect, collection):
#         '''
#         target: get search_devices
#         method: call get_config correctly
#         expected: status ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         config_value = connect.get_config("gpu.search_devices")
#         logging.getLogger().info(config_value)
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_build_index_devices_invalid_parent_key(self, connect, collection):
#         '''
#         target: get invalid parent key
#         method: call get_config without parent_key: gpu
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Gpu_resource_config", "gpu resource config", \
#             "gpu_resource"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config(config+".build_index_devices")
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_build_index_devices_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: build_index_devices
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Build_index_resources"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("gpu."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_build_index_devices_valid(self, connect, collection):
#         '''
#         target: get build_index_devices
#         method: call get_config correctly
#         expected: status ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         config_value = connect.get_config("gpu.build_index_devices")
#         logging.getLogger().info(config_value)
#         assert config_value
#
#     """
#     ******************************************************************
#       The following cases are used to test `set_config` function
#     ******************************************************************
#     """
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_gpu_enable_invalid_parent_key(self, connect, collection):
#         '''
#         target: set invalid parent key
#         method: call set_config without parent_key: gpu
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Gpu_resource_config", "gpu resource config", \
#             "gpu_resource"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config(config+".enable", "true")
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_gpu_invalid_child_key(self, connect, collection):
#         '''
#         target: set invalid child key
#         method: call set_config with invalid child_key
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Gpu_resource_config", "gpu resource config", \
#             "gpu_resource"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("gpu."+config, "true")
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_gpu_enable_invalid_values(self, connect, collection):
#         '''
#         target: set "enable" param
#         method: call set_config with invalid child values
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         for i in [-1, -2, 100]:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("gpu.enable", i)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_gpu_enable_valid(self, connect, collection):
#         '''
#         target: set "enable" param
#         method: call set_config correctly
#         expected: status ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         valid_configs = ["off", "False", "0", "nO", "on", "True", 1, "yES"]
#         for config in valid_configs:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("gpu.enable", config)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_cache_size_invalid_parent_key(self, connect, collection):
#         '''
#         target: set invalid parent key
#         method: call set_config without parent_key: gpu
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Gpu_resource_config", "gpu resource config", \
#             "gpu_resource"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config(config+".cache_size", 2)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_cache_size_valid(self, connect, collection):
#         '''
#         target: set cache_size
#         method: call set_config correctly
#         expected: status ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         relpy = connect.set_config("gpu.cache_size", 2)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_cache_size_invalid_values(self, connect, collection):
#         '''
#         target: set cache_size
#         method: call set_config with invalid child values
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         self.reset_configs(connect)
#         for i in [-1, "1\n", "1\t"]:
#             logging.getLogger().info(i)
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("gpu", "cache_size", i)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_search_devices_invalid_parent_key(self, connect, collection):
#         '''
#         target: set invalid parent key
#         method: call set_config without parent_key: gpu
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Gpu_resource_config", "gpu resource config", \
#             "gpu_resource"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config(config, "search_devices", "gpu0")
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_search_devices_valid(self, connect, collection):
#         '''
#         target: set search_devices
#         method: call set_config correctly
#         expected: status ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         with pytest.raises(Exception) as e:
#             relpy = connect.set_config("gpu", "search_devices", "gpu0")
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_search_devices_invalid_values(self, connect, collection):
#         '''
#         target: set search_devices
#         method: call set_config with invalid child values
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         for i in [-1, "10", "gpu-1", "gpu0, gpu1", "gpu22,gpu44","gpu10000","gpu 0","-gpu0"]:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("gpu", "search_devices", i)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_build_index_devices_invalid_parent_key(self, connect, collection):
#         '''
#         target: set invalid parent key
#         method: call set_config without parent_key: gpu
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         invalid_configs = ["Gpu_resource_config", "gpu resource config", \
#             "gpu_resource"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config(config, "build_index_devices", "gpu0")
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_build_index_devices_valid(self, connect, collection):
#         '''
#         target: set build_index_devices
#         method: call set_config correctly
#         expected: status ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         with pytest.raises(Exception) as e:
#             relpy = connect.set_config("gpu", "build_index_devices", "gpu0")
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_build_index_devices_invalid_values(self, connect, collection):
#         '''
#         target: set build_index_devices
#         method: call set_config with invalid child values
#         expected: status not ok
#         '''
#         if str(connect._cmd("mode")) == "CPU":
#             pytest.skip("Only support GPU mode")
#         for i in [-1, "10", "gpu-1", "gpu0, gpu1", "gpu22,gpu44","gpu10000","gpu 0","-gpu0"]:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("gpu", "build_index_devices", i)
#         self.reset_configs(connect)
#
#
# class TestNetworkConfig:
#     """
#     ******************************************************************
#       The following cases are used to test `get_config` function
#     ******************************************************************
#     """
#     @pytest.fixture(scope="function", autouse=True)
#     def skip_http_check(self, args):
#         if args["handler"] == "HTTP":
#             pytest.skip("skip in http mode")
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_address_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: address
#         expected: status not ok
#         '''
#         invalid_configs = ["Address", "addresses", "address "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("network."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_address_valid(self, connect, collection):
#         '''
#         target: get address
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("network.bind.address")
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_port_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: port
#         expected: status not ok
#         '''
#         invalid_configs = ["Port", "PORT", "port "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("network."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_port_valid(self, connect, collection):
#         '''
#         target: get port
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("network.http.port")
#         assert config_value
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_http_port_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: http.port
#         expected: status not ok
#         '''
#         invalid_configs = ["webport", "Web_port", "http port "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("network."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_http_port_valid(self, connect, collection):
#         '''
#         target: get http.port
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("network.http.port")
#         assert config_value
#
#     """
#     ******************************************************************
#       The following cases are used to test `set_config` function
#     ******************************************************************
#     """
#     def gen_valid_timezones(self):
#         timezones = []
#         for i in range(0, 13):
#             timezones.append("UTC+" + str(i))
#             timezones.append("UTC-" + str(i))
#         timezones.extend(["UTC+13", "UTC+14"])
#         return timezones
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_network_invalid_child_key(self, connect, collection):
#         '''
#         target: set invalid child key
#         method: call set_config with invalid child_key
#         expected: status not ok
#         '''
#         with pytest.raises(Exception) as e:
#             relpy = connect.set_config("network.child_key", 19530)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_address_valid(self, connect, collection):
#         '''
#         target: set address
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         relpy = connect.set_config("network.bind.address", '0.0.0.0')
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_port_valid(self, connect, collection):
#         '''
#         target: set port
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         for valid_port in [1025, 65534, 12345, "19530"]:
#             relpy = connect.set_config("network.http.port", valid_port)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_port_invalid(self, connect, collection):
#         '''
#         target: set port
#         method: call set_config with port number out of range(1024, 65535)
#         expected: status not ok
#         '''
#         for invalid_port in [1024, 65535, "0", "True", "100000"]:
#             logging.getLogger().info(invalid_port)
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("network.http.port", invalid_port)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_http_port_valid(self, connect, collection):
#         '''
#         target: set http.port
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         for valid_http_port in [1025, 65534, "12345", 19121]:
#             relpy = connect.set_config("network.http.port", valid_http_port)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_http_port_invalid(self, connect, collection):
#         '''
#         target: set http.port
#         method: call set_config with http.port number out of range(1024, 65535)
#         expected: status not ok
#         '''
#         for invalid_http_port in [1024, 65535, "0", "True", "1000000"]:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("network.http.port", invalid_http_port)
#
#
# class TestGeneralConfig:
#     """
#     ******************************************************************
#       The following cases are used to test `get_config` function
#     ******************************************************************
#     """
#     @pytest.fixture(scope="function", autouse=True)
#     def skip_http_check(self, args):
#         if args["handler"] == "HTTP":
#             pytest.skip("skip in http mode")
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_meta_uri_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: meta_uri
#         expected: status not ok
#         '''
#         invalid_configs = ["backend_Url", "backend-url", "meta uri "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("general."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_meta_uri_valid(self, connect, collection):
#         '''
#         target: get meta_uri
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("general.meta_uri")
#         assert config_value
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_timezone_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: timezone
#         expected: status not ok
#         '''
#         invalid_configs = ["time", "time_zone "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("general."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_timezone_valid(self, connect, collection):
#         '''
#         target: get timezone
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("general.timezone")
#         assert "UTC" in config_value
#
#     """
#     ******************************************************************
#       The following cases are used to test `set_config` function
#     ******************************************************************
#     """
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_timezone_invalid(self, connect, collection):
#         '''
#         target: set timezone
#         method: call set_config with invalid timezone
#         expected: status not ok
#         '''
#         for invalid_timezone in ["utc++8", "UTC++8"]:
#             logging.getLogger().info(invalid_timezone)
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("general.timezone", invalid_timezone)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_general_invalid_child_key(self, connect, collection):
#         '''
#         target: set invalid child key
#         method: call set_config with invalid child_key
#         expected: status not ok
#         '''
#         with pytest.raises(Exception) as e:
#             relpy = connect.set_config("general.child_key", 1)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_meta_uri_valid(self, connect, collection):
#         '''
#         target: set meta_uri
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         relpy = connect.set_config("general.meta_uri", 'sqlite://:@:/')
#
#
# class TestStorageConfig:
#     """
#     ******************************************************************
#       The following cases are used to test `get_config` function
#     ******************************************************************
#     """
#     @pytest.fixture(scope="function", autouse=True)
#     def skip_http_check(self, args):
#         if args["handler"] == "HTTP":
#             pytest.skip("skip in http mode")
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_path_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: path
#         expected: status not ok
#         '''
#         invalid_configs = ["Primary_path", "primarypath", "pa_th "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("storage."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_path_valid(self, connect, collection):
#         '''
#         target: get path
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("storage.path")
#         assert config_value
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_auto_flush_interval_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: auto_flush_interval
#         expected: status not ok
#         '''
#         invalid_configs = ["autoFlushInterval", "auto_flush", "auto_flush interval "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("storage."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_auto_flush_interval_valid(self, connect, collection):
#         '''
#         target: get auto_flush_interval
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("storage.auto_flush_interval")
#
#     """
#     ******************************************************************
#       The following cases are used to test `set_config` function
#     ******************************************************************
#     """
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_storage_invalid_child_key(self, connect, collection):
#         '''
#         target: set invalid child key
#         method: call set_config with invalid child_key
#         expected: status not ok
#         '''
#         with pytest.raises(Exception) as e:
#             relpy = connect.set_config("storage.child_key", "")
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_path_valid(self, connect, collection):
#         '''
#         target: set path
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         relpy = connect.set_config("storage.path", '/var/lib/milvus')
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_auto_flush_interval_valid(self, connect, collection):
#         '''
#         target: set auto_flush_interval
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         for valid_auto_flush_interval in [2, 1]:
#             logging.getLogger().info(valid_auto_flush_interval)
#             relpy = connect.set_config("storage.auto_flush_interval", valid_auto_flush_interval)
#             config_value = connect.get_config("storage.auto_flush_interval")
#             assert config_value == str(valid_auto_flush_interval)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_auto_flush_interval_invalid(self, connect, collection):
#         '''
#         target: set auto_flush_interval
#         method: call set_config with invalid auto_flush_interval
#         expected: status not ok
#         '''
#         for invalid_auto_flush_interval in [-1, "1.5", "invalid", "1+2"]:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("storage.auto_flush_interval", invalid_auto_flush_interval)
#
#
# class TestMetricConfig:
#     """
#     ******************************************************************
#       The following cases are used to test `get_config` function
#     ******************************************************************
#     """
#     @pytest.fixture(scope="function", autouse=True)
#     def skip_http_check(self, args):
#         if args["handler"] == "HTTP":
#             pytest.skip("skip in http mode")
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_enable_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: enable
#         expected: status not ok
#         '''
#         invalid_configs = ["enablemonitor", "Enable_monitor", "en able "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("metric."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_enable_valid(self, connect, collection):
#         '''
#         target: get enable
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("metric.enable")
#         assert config_value
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_address_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: address
#         expected: status not ok
#         '''
#         invalid_configs = ["Add ress", "addresses", "add ress "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("metric."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_address_valid(self, connect, collection):
#         '''
#         target: get address
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("metric.address")
#         assert config_value
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_port_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: port
#         expected: status not ok
#         '''
#         invalid_configs = ["Po_rt", "PO_RT", "po_rt "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("metric."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_port_valid(self, connect, collection):
#         '''
#         target: get port
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("metric.port")
#         assert config_value
#
#     """
#     ******************************************************************
#       The following cases are used to test `set_config` function
#     ******************************************************************
#     """
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_metric_invalid_child_key(self, connect, collection):
#         '''
#         target: set invalid child key
#         method: call set_config with invalid child_key
#         expected: status not ok
#         '''
#         with pytest.raises(Exception) as e:
#             relpy = connect.set_config("metric.child_key", 19530)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_enable_valid(self, connect, collection):
#         '''
#         target: set enable
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         for valid_enable in ["false", "true"]:
#             relpy = connect.set_config("metric.enable", valid_enable)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_address_valid(self, connect, collection):
#         '''
#         target: set address
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         relpy = connect.set_config("metric.address", '127.0.0.1')
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_port_valid(self, connect, collection):
#         '''
#         target: set port
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         for valid_port in [1025, 65534, "19530", "9091"]:
#             relpy = connect.set_config("metric.port", valid_port)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_port_invalid(self, connect, collection):
#         '''
#         target: set port
#         method: call set_config with port number out of range(1024, 65535), or same as http.port number
#         expected: status not ok
#         '''
#         for invalid_port in [1024, 65535, "0", "True", "100000"]:
#             with pytest.raises(Exception) as e:
#                 relpy = connect.set_config("metric.port", invalid_port)
#
#
# class TestWALConfig:
#     """
#     ******************************************************************
#       The following cases are used to test `get_config` function
#     ******************************************************************
#     """
#     @pytest.fixture(scope="function", autouse=True)
#     def skip_http_check(self, args):
#         if args["handler"] == "HTTP":
#             pytest.skip("skip in http mode")
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_enable_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: enable
#         expected: status not ok
#         '''
#         invalid_configs = ["enabled", "Enab_le", "enable_"]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("wal."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_enable_valid(self, connect, collection):
#         '''
#         target: get enable
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("wal.enable")
#         assert config_value
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_recovery_error_ignore_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: recovery_error_ignore
#         expected: status not ok
#         '''
#         invalid_configs = ["recovery-error-ignore", "Recovery error_ignore", "recoveryxerror_ignore "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("wal."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_recovery_error_ignore_valid(self, connect, collection):
#         '''
#         target: get recovery_error_ignore
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("wal.recovery_error_ignore")
#         assert config_value
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_buffer_size_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: buffer_size
#         expected: status not ok
#         '''
#         invalid_configs = ["buffersize", "Buffer size", "buffer size "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("wal."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_buffer_size_valid(self, connect, collection):
#         '''
#         target: get buffer_size
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("wal.buffer_size")
#         assert config_value
#
#     @pytest.mark.tags(CaseLabel.L2)
#     def test_get_wal_path_invalid_child_key(self, connect, collection):
#         '''
#         target: get invalid child key
#         method: call get_config without child_key: wal_path
#         expected: status not ok
#         '''
#         invalid_configs = ["wal", "Wal_path", "wal_path "]
#         for config in invalid_configs:
#             with pytest.raises(Exception) as e:
#                 config_value = connect.get_config("wal."+config)
#
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_get_wal_path_valid(self, connect, collection):
#         '''
#         target: get wal_path
#         method: call get_config correctly
#         expected: status ok
#         '''
#         config_value = connect.get_config("wal.path")
#         assert config_value
#
#     """
#     ******************************************************************
#       The following cases are used to test `set_config` function
#     ******************************************************************
#     """
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_wal_invalid_child_key(self, connect, collection):
#         '''
#         target: set invalid child key
#         method: call set_config with invalid child_key
#         expected: status not ok
#         '''
#         with pytest.raises(Exception) as e:
#             relpy = connect.set_config("wal.child_key", 256)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_enable_valid(self, connect, collection):
#         '''
#         target: set enable
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         for valid_enable in ["false", "true"]:
#             relpy = connect.set_config("wal.enable", valid_enable)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_recovery_error_ignore_valid(self, connect, collection):
#         '''
#         target: set recovery_error_ignore
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         for valid_recovery_error_ignore in ["false", "true"]:
#             relpy = connect.set_config("wal.recovery_error_ignore", valid_recovery_error_ignore)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     def test_set_buffer_size_valid_A(self, connect, collection):
#         '''
#         target: set buffer_size
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         for valid_buffer_size in ["64MB", "128MB", "4096MB", "1000MB", "256MB"]:
#             relpy = connect.set_config("wal.buffer_size", valid_buffer_size)
#
#     @pytest.mark.skip(reason="overwrite config file is not supported in ci yet.")
#     @pytest.mark.timeout(CONFIG_TIMEOUT)
#     def test_set_wal_path_valid(self, connect, collection, args):
#         '''
#         target: set wal_path
#         method: call set_config correctly
#         expected: status ok, set successfully
#         '''
#         relpy = connect.set_config("wal.path", "/var/lib/milvus/wal")

