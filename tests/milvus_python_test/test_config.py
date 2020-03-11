import time
import random
import pdb
import threading
import logging
from multiprocessing import Pool, Process
import pytest
from milvus import IndexType, MetricType
from utils import *
import ujson


dim = 128
index_file_size = 10
CONFIG_TIMEOUT = 80
nprobe = 1
top_k = 1
tag = "1970-01-01"
nb = 6000


class TestCacheConfig:
    """
    ******************************************************************
      The following cases are used to test `get_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def reset_configs(self, connect):
        '''
        reset configs so the tests are stable
        '''
        status, reply = connect.set_config("cache_config", "cpu_cache_capacity", 4)
        assert status.OK()
        status, config_value = connect.get_config("cache_config", "cpu_cache_capacity")
        assert config_value == '4'
        status, reply = connect.set_config("cache_config", "insert_buffer_size", 1)
        assert status.OK()
        status, config_value = connect.get_config("cache_config", "insert_buffer_size")
        assert config_value == '1'

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_cpu_cache_capacity_invalid_parent_key(self, connect, collection):
        '''
        target: get invalid parent key
        method: call get_config without parent_key: cache_config
        expected: status not ok
        '''
        invalid_configs = gen_invalid_cache_config()
        invalid_configs.extend(["Cache_config", "cache config", "cache_Config", "cacheconfig", "cache_config\n", "cache_config\t"])
        for config in invalid_configs:
            status, config_value = connect.get_config(config, "cpu_cache_capacity")
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_cpu_cache_capacity_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: cpu_cache_capacity
        expected: status not ok
        '''
        invalid_configs = gen_invalid_cache_config()
        invalid_configs.extend(["Cpu_cache_capacity", "cpu cache_capacity", "cpucachecapacity", "cpu_cache_capacity\n", "cpu_cache_capacity\t"])
        for config in invalid_configs:
            status, config_value = connect.get_config("cache_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_cpu_cache_capacity_valid(self, connect, collection):
        '''
        target: get cpu_cache_capacity
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("cache_config", "cpu_cache_capacity")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_insert_buffer_size_invalid_parent_key(self, connect, collection):
        '''
        target: get invalid parent key
        method: call get_config without parent_key: cache_config
        expected: status not ok
        '''
        invalid_configs = gen_invalid_cache_config()
        invalid_configs.extend(["Cache_config", "cache config", "cache_Config", "cacheconfig", "cache_config\n", "cache_config\t"])
        for config in invalid_configs:
            status, config_value = connect.get_config(config, "insert_buffer_size")
            assert not status.OK()

    @pytest.mark.level(2)
    def test_get_insert_buffer_size_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: insert_buffer_size
        expected: status not ok
        '''
        invalid_configs = gen_invalid_cache_config()
        invalid_configs.extend(["Insert_buffer_size", "insert buffer_size", "insertbuffersize", "insert_buffer_size\n", "insert_buffer_size\t"])
        for config in invalid_configs:
            status, config_value = connect.get_config("cache_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_insert_buffer_size_valid(self, connect, collection):
        '''
        target: get insert_buffer_size
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("cache_config", "insert_buffer_size")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_cache_insert_data_invalid_parent_key(self, connect, collection):
        '''
        target: get invalid parent key
        method: call get_config without parent_key: cache_config
        expected: status not ok
        '''
        invalid_configs = gen_invalid_cache_config()
        invalid_configs.extend(["Cache_config", "cache config", "cache_Config", "cacheconfig", "cache_config\n", "cache_config\t"])
        for config in invalid_configs:
            status, config_value = connect.get_config(config, "cache_insert_data")
            assert not status.OK()

    @pytest.mark.level(2)
    def test_get_cache_insert_data_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: cache_insert_data
        expected: status not ok
        '''
        invalid_configs = gen_invalid_cache_config()
        invalid_configs.extend(["Cache_insert_data", "cacheinsertdata", " cache_insert_data"])
        for config in invalid_configs:
            status, config_value = connect.get_config("cache_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_cache_insert_data_valid(self, connect, collection):
        '''
        target: get cache_insert_data
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("cache_config", "cache_insert_data")
        assert status.OK()


    """
    ******************************************************************
      The following cases are used to test `set_config` function
    ******************************************************************
    """
    def get_memory_available(self, connect):
        _, info = connect._cmd("get_system_info")
        mem_info = ujson.loads(info)
        mem_total = int(mem_info["memory_total"])
        mem_used = int(mem_info["memory_used"])
        logging.getLogger().info(mem_total)
        logging.getLogger().info(mem_used)
        mem_available = mem_total - mem_used
        return int(mem_available / 1024 / 1024 / 1024)

    def get_memory_total(self, connect):
        _, info = connect._cmd("get_system_info")
        mem_info = ujson.loads(info)
        mem_total = int(mem_info["memory_total"])
        return int(mem_total / 1024 / 1024 / 1024)

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_cpu_cache_capacity_invalid_parent_key(self, connect, collection):
        '''
        target: set invalid parent key
        method: call set_config without parent_key: cache_config
        expected: status not ok
        '''
        self.reset_configs(connect)
        invalid_configs = gen_invalid_cache_config()
        invalid_configs.extend(["Cache_config", "cache config", "cache_Config", "cacheconfig", "cache_config\n", "cache_config\t"])
        for config in invalid_configs:
            status, reply = connect.set_config(config, "cpu_cache_capacity", 4)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_cache_config_invalid_child_key(self, connect, collection):
        '''
        target: set invalid child key
        method: call set_config with invalid child_key
        expected: status not ok
        '''
        self.reset_configs(connect)
        invalid_configs = gen_invalid_cache_config()
        for config in invalid_configs:
            status, reply = connect.set_config("cache_config", config, 4)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_cpu_cache_capacity_valid(self, connect, collection):
        '''
        target: set cpu_cache_capacity
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        self.reset_configs(connect)
        status, reply = connect.set_config("cache_config", "cpu_cache_capacity", 8)
        assert status.OK()
        status, config_value = connect.get_config("cache_config", "cpu_cache_capacity")
        assert status.OK()
        assert config_value == '8'

    def test_set_cpu_cache_capacity_valid_multiple_times(self, connect, collection):
        '''
        target: set cpu_cache_capacity
        method: call set_config correctly and repeatedly
        expected: status ok
        '''
        self.reset_configs(connect)
        for i in range(20):
            status, reply = connect.set_config("cache_config", "cpu_cache_capacity", 4)
            assert status.OK()
            status, config_value = connect.get_config("cache_config", "cpu_cache_capacity")
            assert status.OK()
            assert config_value == '4'
        for i in range(20):
            status, reply = connect.set_config("cache_config", "cpu_cache_capacity", 8)
            assert status.OK()
            status, config_value = connect.get_config("cache_config", "cpu_cache_capacity")
            assert status.OK()
            assert config_value == '8'

    @pytest.mark.level(2)
    def test_set_insert_buffer_size_invalid_parent_key(self, connect, collection):
        '''
        target: set invalid parent key
        method: call set_config without parent_key: cache_config
        expected: status not ok
        '''
        self.reset_configs(connect)
        invalid_configs = gen_invalid_cache_config()
        invalid_configs.extend(["Cache_config", "cache config", "cache_Config", "cacheconfig", "cache_config\n", "cache_config\t"])
        for config in invalid_configs:
            status, reply = connect.set_config(config, "insert_buffer_size", 1)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_insert_buffer_size_valid(self, connect, collection):
        '''
        target: set insert_buffer_size
        method: call get_config correctly
        expected: status ok, set successfully
        '''
        self.reset_configs(connect)
        status, reply = connect.set_config("cache_config", "insert_buffer_size", 2)
        assert status.OK()
        status, config_value = connect.get_config("cache_config", "insert_buffer_size")
        assert status.OK()
        assert config_value == '2'

    @pytest.mark.level(2)
    def test_set_insert_buffer_size_valid_multiple_times(self, connect, collection):
        '''
        target: set insert_buffer_size
        method: call get_config correctly and repeatedly
        expected: status ok
        '''
        self.reset_configs(connect)
        for i in range(20):
            status, reply = connect.set_config("cache_config", "insert_buffer_size", 1)
            assert status.OK()
            status, config_value = connect.get_config("cache_config", "insert_buffer_size")
            assert status.OK()
            assert config_value == '1'
        for i in range(20):
            status, reply = connect.set_config("cache_config", "insert_buffer_size", 2)
            assert status.OK()
            status, config_value = connect.get_config("cache_config", "insert_buffer_size")
            assert status.OK()
            assert config_value == '2'

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_cache_config_out_of_memory_value_A(self, connect, collection):
        '''
        target: set cpu_cache_capacity / insert_buffer_size to be out-of-memory
        method: call set_config with child values bigger than current system memory
        expected: status not ok (cpu_cache_capacity + insert_buffer_size < system memory)
        '''
        self.reset_configs(connect)
        mem_total = self.get_memory_total(connect)
        logging.getLogger().info(mem_total)
        status, reply = connect.set_config("cache_config", "cpu_cache_capacity", mem_total + 1)
        assert not status.OK()
        status, reply = connect.set_config("cache_config", "insert_buffer_size", mem_total + 1)
        assert not status.OK()

    @pytest.mark.skip(reason="Still needs discussion")
    def test_set_cache_config_out_of_memory_value_B(self, connect, collection):
        '''
        target: set cpu_cache_capacity / insert_buffer_size to be out-of-memory
        method: call set_config with invalid values
        expected: status not ok (cpu_cache_capacity + insert_buffer_size < system memory)
        '''
        self.reset_configs(connect)
        mem_available = self.get_memory_available(connect)
        logging.getLogger().info(mem_available)
        status, cpu_cache_capacity = connect.get_config("cache_config", "cpu_cache_capacity")
        assert status.OK()
        logging.getLogger().info(cpu_cache_capacity)
        status, insert_buffer_size = connect.get_config("cache_config", "insert_buffer_size")
        assert status.OK()
        logging.getLogger().info(insert_buffer_size)
        status, reply = connect.set_config("cache_config", "cpu_cache_capacity", mem_available - int(insert_buffer_size) + 1)
        assert not status.OK()
        status, reply = connect.set_config("cache_config", "insert_buffer_size", mem_available - int(cpu_cache_capacity) + 1)
        assert not status.OK()

    @pytest.mark.skip(reason="Still needs discussion")
    def test_set_cache_config_out_of_memory_value_C(self, connect, collection):
        '''
        target: set cpu_cache_capacity / insert_buffer_size to be out-of-memory
        method: call set_config multiple times
        expected: status not ok (cpu_cache_capacity + insert_buffer_size < system memory)
        '''
        self.reset_configs(connect)
        mem_available = self.get_memory_available(connect)
        logging.getLogger().info(mem_available)
        status, insert_buffer_size = connect.get_config("cache_config", "insert_buffer_size")
        assert status.OK()
        if mem_available >= int(insert_buffer_size) + 20:
            status, reply = connect.set_config("cache_config", "cpu_cache_capacity", mem_available - int(insert_buffer_size) - 10)
            assert status.OK()
            status, reply = connect.set_config("cache_config", "insert_buffer_size", int(insert_buffer_size) + 20)
            assert not status.OK()
            status, reply = connect.set_config("cache_config", "insert_buffer_size", int(insert_buffer_size) + 1)
            assert status.OK()
            status, insert_buffer_size_new = connect.get_config("cache_config", "insert_buffer_size")
            assert int(insert_buffer_size_new) == int(insert_buffer_size) + 1
        self.reset_configs(connect)

    @pytest.mark.level(2)
    def test_set_cache_insert_data_invalid_parent_key(self, connect, collection):
        '''
        target: set invalid parent key
        method: call set_config without parent_key: cache_config
        expected: status not ok
        '''
        self.reset_configs(connect)
        invalid_configs = gen_invalid_cache_config()
        invalid_configs.extend(["Cache_config", "cache config", "cache_Config", "cacheconfig", "cache_config\n", "cache_config\t"])
        for config in invalid_configs:
            status, reply = connect.set_config(config, "cache_insert_data", "1")
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_cache_insert_data_valid(self, connect, collection):
        '''
        target: set cache_insert_data
        method: call get_config correctly
        expected: status ok, set successfully
        '''
        self.reset_configs(connect)
        # On/Off true/false 1/0 YES/NO
        valid_configs = ["Off", "false", 0, "NO", "On", "true", "1", "YES"]
        for config in valid_configs:
            status, reply = connect.set_config("cache_config", "cache_insert_data", config)
            assert status.OK()
            status, config_value = connect.get_config("cache_config", "cache_insert_data")
            assert status.OK()
            assert config_value == str(config)


class TestEngineConfig:
    """
    ******************************************************************
      The following cases are used to test `get_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_use_blas_threshold_invalid_parent_key(self, connect, collection):
        '''
        target: get invalid parent key
        method: call get_config without parent_key: engine_config
        expected: status not ok
        '''
        invalid_configs = gen_invalid_engine_config()
        invalid_configs.extend(["Engine_config", "engine config", "engine_config\n", "engine_config\t"])
        for config in invalid_configs:
            status, config_value = connect.get_config(config, "use_blas_threshold")
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_use_blas_threshold_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: use_blas_threshold
        expected: status not ok
        '''
        invalid_configs = gen_invalid_engine_config()
        invalid_configs.extend(["Use_blas_threshold", "use blas threshold", "use_blas_threshold\n", "use_blas_threshold\t"])
        for config in invalid_configs:
            status, config_value = connect.get_config("engine_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_use_blas_threshold_valid(self, connect, collection):
        '''
        target: get use_blas_threshold
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("engine_config", "use_blas_threshold")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_gpu_search_threshold_invalid_parent_key(self, connect, collection):
        '''
        target: get invalid parent key
        method: call get_config without parent_key: engine_config
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = gen_invalid_engine_config()
        invalid_configs.extend(["Engine_config", "engine config", "engine_config\n", "engine_config\t"])
        for config in invalid_configs:
            status, config_value = connect.get_config(config, "gpu_search_threshold")
            assert not status.OK()

    @pytest.mark.level(2)
    def test_get_gpu_search_threshold_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: gpu_search_threshold
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = gen_invalid_engine_config()
        invalid_configs.extend(["Gpu_search_threshold", "gpusearchthreshold", "gpu_search_threshold\n", "gpu_search_threshold\t"])
        for config in invalid_configs:
            status, config_value = connect.get_config("engine_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_gpu_search_threshold_valid(self, connect, collection):
        '''
        target: get gpu_search_threshold
        method: call get_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        status, config_value = connect.get_config("engine_config", "gpu_search_threshold")
        assert status.OK()

    
    """
    ******************************************************************
      The following cases are used to test `set_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_use_blas_threshold_invalid_parent_key(self, connect, collection):
        '''
        target: set invalid parent key
        method: call set_config without parent_key: engine_config
        expected: status not ok
        '''
        invalid_configs = gen_invalid_engine_config()
        invalid_configs.extend(["Engine_config", "engine config", "engine_config\n", "engine_config\t"])
        for config in invalid_configs:
            status, reply = connect.set_config(config, "use_blas_threshold", 1000)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_engine_config_invalid_child_key(self, connect, collection):
        '''
        target: set invalid child key
        method: call set_config with invalid child_key
        expected: status not ok
        '''
        invalid_configs = gen_invalid_engine_config()
        for config in invalid_configs:
            status, reply = connect.set_config("engine_config", config, 1000)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_use_blas_threshold_valid(self, connect, collection):
        '''
        target: set use_blas_threshold
        method: call set_config correctly
        expected: status ok
        '''
        status, reply = connect.set_config("engine_config", "use_blas_threshold", '2000')
        assert status.OK()
        status, config_value = connect.get_config("engine_config", "use_blas_threshold")
        assert status.OK()
        assert config_value == '2000'

    @pytest.mark.level(2)
    def test_set_use_blas_threshold_valid_multiple_times(self, connect, collection):
        '''
        target: set use_blas_threshold
        method: call set_config correctly and repeatedly
        expected: status ok
        '''
        for i in range(1, 100):
            status, reply = connect.set_config("engine_config", "use_blas_threshold", i * 100)
            assert status.OK()
            status, config_value = connect.get_config("engine_config", "use_blas_threshold")
            assert status.OK()
            assert config_value == str(i * 100)
        # reset
        status, reply = connect.set_config("engine_config", "use_blas_threshold", 1100)
        assert status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_gpu_search_threshold_invalid_parent_key(self, connect, collection):
        '''
        target: set invalid parent key
        method: call set_config without parent_key: engine_config
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = gen_invalid_engine_config()
        invalid_configs.extend(["Engine_config", "engine config", "engine_config\n", "engine_config\t"])
        for config in invalid_configs:
            status, reply = connect.set_config(config, "gpu_search_threshold", 1000)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_gpu_search_threshold_valid(self, connect, collection):
        '''
        target: set gpu_search_threshold
        method: call set_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        status, reply = connect.set_config("engine_config", "gpu_search_threshold", 2000)
        assert status.OK()
        status, config_value = connect.get_config("engine_config", "gpu_search_threshold")
        assert status.OK()
        assert config_value == '2000'

    @pytest.mark.level(2)
    def test_set_gpu_search_threshold_valid_multiple_times(self, connect, collection):
        '''
        target: set gpu_search_threshold
        method: call set_config correctly and repeatedly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        for i in range(1, 100):
            status, reply = connect.set_config("engine_config", "gpu_search_threshold", i * 100)
            assert status.OK()
        # reset config
        status, reply = connect.set_config("engine_config", "use_blas_threshold", 1100)
        assert status.OK()
        if str(connect._cmd("mode")[1]) == "GPU":
            status, reply = connect.set_config("engine_config", "gpu_search_threshold", 1000)
            assert status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_engine_config_invalid_values(self, connect, collection):
        '''
        target: set engine_config
        method: call set_config with invalid child values
        expected: status not ok
        '''
        for i in [-1, "1000\n", "1000\t", "1000.0", 1000.35]:
            status, reply = connect.set_config("engine_config", "use_blas_threshold", i)
            assert not status.OK()
            if str(connect._cmd("mode")[1]) == "GPU":
                status, reply = connect.set_config("engine_config", "gpu_search_threshold", i)
                assert not status.OK()


class TestGPUResourceConfig:
    """
    ******************************************************************
      The following cases are used to test `get_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def reset_configs(self, connect):
        '''
        reset configs so the tests are stable
        '''
        status, reply = connect.set_config("gpu_resource_config", "enable", "true")
        assert status.OK()
        status, config_value = connect.get_config("gpu_resource_config", "enable")
        assert config_value == "true"
        status, reply = connect.set_config("gpu_resource_config", "cache_capacity", 1)
        assert status.OK()
        status, config_value = connect.get_config("gpu_resource_config", "cache_capacity")
        assert config_value == '1'
        status, reply = connect.set_config("gpu_resource_config", "search_resources", "gpu0")
        assert status.OK()
        status, config_value = connect.get_config("gpu_resource_config", "search_resources")
        assert config_value == 'gpu0'
        status, reply = connect.set_config("gpu_resource_config", "build_index_resources", "gpu0")
        assert status.OK()
        status, config_value = connect.get_config("gpu_resource_config", "build_index_resources")
        assert config_value == 'gpu0'

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_gpu_enable_invalid_parent_key(self, connect, collection):
        '''
        target: get invalid parent key
        method: call get_config without parent_key: gpu_resource_config
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Gpu_resource_config", "gpu resource config", \
            "gpu_resource", "gpu_resource_config\n", "gpu_resource_config\t"]
        for config in invalid_configs:
            status, config_value = connect.get_config(config, "enable")
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_gpu_enable_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: enable
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Enable", "enable ", "disable", "true", "enable\t"]
        for config in invalid_configs:
            status, config_value = connect.get_config("gpu_resource_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_gpu_enable_valid(self, connect, collection):
        '''
        target: get enable status
        method: call get_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        status, config_value = connect.get_config("gpu_resource_config", "enable")
        assert status.OK()
        assert config_value == "true" or config_value == "false"

    @pytest.mark.level(2)
    def test_get_cache_capacity_invalid_parent_key(self, connect, collection):
        '''
        target: get invalid parent key
        method: call get_config without parent_key: gpu_resource_config
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Gpu_resource_config", "gpu resource config", \
            "gpu_resource", "gpu_resource_config\n", "gpu_resource_config\t"]
        for config in invalid_configs:
            status, config_value = connect.get_config(config, "cache_capacity")
            assert not status.OK()

    @pytest.mark.level(2)
    def test_get_cache_capacity_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: cache_capacity
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Cache_capacity", "cachecapacity", "cache_capacity\n", "cache_capacity\t"]
        for config in invalid_configs:
            status, config_value = connect.get_config("gpu_resource_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_cache_capacity_valid(self, connect, collection):
        '''
        target: get cache_capacity
        method: call get_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        status, config_value = connect.get_config("gpu_resource_config", "cache_capacity")
        assert status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_search_resources_invalid_parent_key(self, connect, collection):
        '''
        target: get invalid parent key
        method: call get_config without parent_key: gpu_resource_config
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Gpu_resource_config", "gpu resource config", \
            "gpu_resource", "gpu_resource_config\n", "gpu_resource_config\t"]
        for config in invalid_configs:
            status, config_value = connect.get_config(config, "search_resources")
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_search_resources_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: search_resources
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Search_resources", "search_resources\n", "search_resources\t"]
        for config in invalid_configs:
            status, config_value = connect.get_config("gpu_resource_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_search_resources_valid(self, connect, collection):
        '''
        target: get search_resources
        method: call get_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        status, config_value = connect.get_config("gpu_resource_config", "search_resources")
        logging.getLogger().info(config_value)
        assert status.OK()
    
    @pytest.mark.level(2)
    def test_get_build_index_resources_invalid_parent_key(self, connect, collection):
        '''
        target: get invalid parent key
        method: call get_config without parent_key: gpu_resource_config
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Gpu_resource_config", "gpu resource config", \
            "gpu_resource", "gpu_resource_config\n", "gpu_resource_config\t"]
        for config in invalid_configs:
            status, config_value = connect.get_config(config, "build_index_resources")
            assert not status.OK()

    @pytest.mark.level(2)
    def test_get_build_index_resources_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: build_index_resources
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Build_index_resources", "build_index_resources\n", "build_index_resources\t"]
        for config in invalid_configs:
            status, config_value = connect.get_config("gpu_resource_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_build_index_resources_valid(self, connect, collection):
        '''
        target: get build_index_resources
        method: call get_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        status, config_value = connect.get_config("gpu_resource_config", "build_index_resources")
        logging.getLogger().info(config_value)
        assert status.OK()

    
    """
    ******************************************************************
      The following cases are used to test `set_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_gpu_enable_invalid_parent_key(self, connect, collection):
        '''
        target: set invalid parent key
        method: call set_config without parent_key: gpu_resource_config
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Gpu_resource_config", "gpu resource config", \
            "gpu_resource", "gpu_resource_config\n", "gpu_resource_config\t"]
        for config in invalid_configs:
            status, reply = connect.set_config(config, "enable", "true")
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_gpu_resource_config_invalid_child_key(self, connect, collection):
        '''
        target: set invalid child key
        method: call set_config with invalid child_key
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Gpu_resource_config", "gpu resource config", \
            "gpu_resource", "gpu_resource_config\n", "gpu_resource_config\t"]
        for config in invalid_configs:
            status, reply = connect.set_config("gpu_resource_config", config, "true")
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_gpu_enable_invalid_values(self, connect, collection):
        '''
        target: set "enable" param
        method: call set_config with invalid child values
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        for i in [-1, -2, 100]:
            status, reply = connect.set_config("gpu_resource_config", "enable", i)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_gpu_enable_valid(self, connect, collection):
        '''
        target: set "enable" param
        method: call set_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        valid_configs = ["off", "False", "0", "nO", "on", "True", 1, "yES"]
        for config in valid_configs:
            status, reply = connect.set_config("gpu_resource_config", "enable", config)
            assert status.OK()
            status, config_value = connect.get_config("gpu_resource_config", "enable")
            assert status.OK()
            assert config_value == str(config)

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_cache_capacity_invalid_parent_key(self, connect, collection):
        '''
        target: set invalid parent key
        method: call set_config without parent_key: gpu_resource_config
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Gpu_resource_config", "gpu resource config", \
            "gpu_resource", "gpu_resource_config\n", "gpu_resource_config\t"]
        for config in invalid_configs:
            status, reply = connect.set_config(config, "cache_capacity", 2)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_cache_capacity_valid(self, connect, collection):
        '''
        target: set cache_capacity
        method: call set_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        status, reply = connect.set_config("gpu_resource_config", "cache_capacity", 2)
        assert status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_cache_capacity_invalid_values(self, connect, collection):
        '''
        target: set cache_capacity
        method: call set_config with invalid child values
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        self.reset_configs(connect)
        for i in [-1, "10", "1\n", "1\t"]:
            status, reply = connect.set_config("gpu_resource_config", "cache_capacity", i)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_search_resources_invalid_parent_key(self, connect, collection):
        '''
        target: set invalid parent key
        method: call set_config without parent_key: gpu_resource_config
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Gpu_resource_config", "gpu resource config", \
            "gpu_resource", "gpu_resource_config\n", "gpu_resource_config\t"]
        for config in invalid_configs:
            status, reply = connect.set_config(config, "search_resources", "gpu0")
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_search_resources_valid(self, connect, collection):
        '''
        target: set search_resources
        method: call set_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        status, reply = connect.set_config("gpu_resource_config", "search_resources", "gpu0")
        assert status.OK()
        status, config_value = connect.get_config("gpu_resource_config", "search_resources")
        assert config_value == "gpu0"

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_search_resources_invalid_values(self, connect, collection):
        '''
        target: set search_resources
        method: call set_config with invalid child values
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        for i in [-1, "10", "gpu-1", "gpu0, gpu1", "gpu22,gpu44","gpu10000","gpu 0","-gpu0"]:
            status, reply = connect.set_config("gpu_resource_config", "search_resources", i)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_build_index_resources_invalid_parent_key(self, connect, collection):
        '''
        target: set invalid parent key
        method: call set_config without parent_key: gpu_resource_config
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        invalid_configs = ["Gpu_resource_config", "gpu resource config", \
            "gpu_resource", "gpu_resource_config\n", "gpu_resource_config\t"]
        for config in invalid_configs:
            status, reply = connect.set_config(config, "build_index_resources", "gpu0")
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_build_index_resources_valid(self, connect, collection):
        '''
        target: set build_index_resources
        method: call set_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        status, reply = connect.set_config("gpu_resource_config", "build_index_resources", "gpu0")
        assert status.OK()
        status, config_value = connect.get_config("gpu_resource_config", "build_index_resources")
        assert config_value == "gpu0"

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_build_index_resources_invalid_values(self, connect, collection):
        '''
        target: set build_index_resources
        method: call set_config with invalid child values
        expected: status not ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        for i in [-1, "10", "gpu-1", "gpu0, gpu1", "gpu22,gpu44","gpu10000","gpu 0","-gpu0"]:
            status, reply = connect.set_config("gpu_resource_config", "build_index_resources", i)
            assert not status.OK()
        self.reset_configs(connect)


class TestServerConfig:
    """
    ******************************************************************
      The following cases are used to test `get_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_address_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: address
        expected: status not ok
        '''
        invalid_configs = ["Address", "addresses", "address "]
        for config in invalid_configs:
            status, config_value = connect.get_config("server_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_address_valid(self, connect, collection):
        '''
        target: get address
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("server_config", "address")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_port_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: port
        expected: status not ok
        '''
        invalid_configs = ["Port", "PORT", "port "]
        for config in invalid_configs:
            status, config_value = connect.get_config("server_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_port_valid(self, connect, collection):
        '''
        target: get port
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("server_config", "port")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_deploy_mode_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: deploy_mode
        expected: status not ok
        '''
        invalid_configs = ["Deploy_mode", "deploymode", "deploy_mode "]
        for config in invalid_configs:
            status, config_value = connect.get_config("server_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_deploy_mode_valid(self, connect, collection):
        '''
        target: get deploy_mode
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("server_config", "deploy_mode")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_time_zone_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: time_zone
        expected: status not ok
        '''
        invalid_configs = ["time", "timezone", "time_zone "]
        for config in invalid_configs:
            status, config_value = connect.get_config("server_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_time_zone_valid(self, connect, collection):
        '''
        target: get time_zone
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("server_config", "time_zone")
        assert status.OK()
        assert "UTC" in config_value

    @pytest.mark.level(2)
    def test_get_web_port_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: web_port
        expected: status not ok
        '''
        invalid_configs = ["webport", "Web_port", "web_port "]
        for config in invalid_configs:
            status, config_value = connect.get_config("server_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_web_port_valid(self, connect, collection):
        '''
        target: get web_port
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("server_config", "web_port")
        assert status.OK()


    """
    ******************************************************************
      The following cases are used to test `set_config` function
    ******************************************************************
    """
    def gen_valid_timezones(self):
        time_zones = []
        for i in range(0, 13):
            time_zones.append("UTC+" + str(i))
            time_zones.append("UTC-" + str(i))
        time_zones.extend(["UTC+13", "UTC+14"])
        return time_zones

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_server_config_invalid_child_key(self, connect, collection):
        '''
        target: set invalid child key
        method: call set_config with invalid child_key
        expected: status not ok
        '''
        status, reply = connect.set_config("server_config", "child_key", 19530)
        assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_address_valid(self, connect, collection):
        '''
        target: set address
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        status, reply = connect.set_config("server_config", "address", '0.0.0.0')
        assert status.OK()
        status, config_value = connect.get_config("server_config", "address")
        assert status.OK()
        assert config_value == '0.0.0.0'

    def test_set_port_valid(self, connect, collection):
        '''
        target: set port
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        for valid_port in [1025, 65534, 12345, "19530"]:
            status, reply = connect.set_config("server_config", "port", valid_port)
            assert status.OK()
            status, config_value = connect.get_config("server_config", "port")
            assert status.OK()
            assert config_value == str(valid_port)
    
    def test_set_port_invalid(self, connect, collection):
        '''
        target: set port
        method: call set_config with port number out of range(1024, 65535)
        expected: status not ok
        '''
        for invalid_port in [1024, 65535, "0", "True", "19530 ", "100000"]:
            logging.getLogger().info(invalid_port)
            status, reply = connect.set_config("server_config", "port", invalid_port)
            assert not status.OK()

    def test_set_deploy_mode_valid(self, connect, collection):
        '''
        target: set deploy_mode
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        for valid_deploy_mode in ["cluster_readonly", "cluster_writable", "single"]:
            status, reply = connect.set_config("server_config", "deploy_mode", valid_deploy_mode)
            assert status.OK()
            status, config_value = connect.get_config("server_config", "deploy_mode")
            assert status.OK()
            assert config_value == valid_deploy_mode
    
    def test_set_deploy_mode_invalid(self, connect, collection):
        '''
        target: set deploy_mode
        method: call set_config with invalid deploy_mode
        expected: status not ok
        '''
        for invalid_deploy_mode in [65535, "0", "Single", "cluster", "cluster-readonly"]:
            status, reply = connect.set_config("server_config", "deploy_mode", invalid_deploy_mode)
            assert not status.OK()

    def test_set_time_zone_valid(self, connect, collection):
        '''
        target: set time_zone
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        for valid_time_zone in self.gen_valid_timezones():
            status, reply = connect.set_config("server_config", "time_zone", valid_time_zone)
            assert status.OK()
            status, config_value = connect.get_config("server_config", "time_zone")
            assert status.OK()
            assert config_value == valid_time_zone
        # reset to default
        status, reply = connect.set_config("server_config", "time_zone", "UTC+8")
        assert status.OK()
    
    def test_set_time_zone_invalid(self, connect, collection):
        '''
        target: set time_zone
        method: call set_config with invalid time_zone
        expected: status not ok
        '''
        for invalid_time_zone in ["utc+8", "UTC++8", "GMT+8"]:
            logging.getLogger().info(invalid_time_zone)
            status, reply = connect.set_config("server_config", "time_zone", invalid_time_zone)
            assert not status.OK()

    def test_set_web_port_valid(self, connect, collection):
        '''
        target: set web_port
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        for valid_web_port in [1025, 65534, "12345", 19121]:
            status, reply = connect.set_config("server_config", "web_port", valid_web_port)
            assert status.OK()
            status, config_value = connect.get_config("server_config", "web_port")
            assert status.OK()
            assert config_value == str(valid_web_port)
    
    def test_set_web_port_invalid(self, connect, collection):
        '''
        target: set web_port
        method: call set_config with web_port number out of range(1024, 65535)
        expected: status not ok
        '''
        for invalid_web_port in [1024, 65535, "0", "True", "19530 ", "1000000"]:
            status, reply = connect.set_config("server_config", "web_port", invalid_web_port)
            assert not status.OK()


class TestDBConfig:
    """
    ******************************************************************
      The following cases are used to test `get_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_backend_url_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: backend_url
        expected: status not ok
        '''
        invalid_configs = ["backend_Url", "backend-url", "backend_url "]
        for config in invalid_configs:
            status, config_value = connect.get_config("db_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_backend_url_valid(self, connect, collection):
        '''
        target: get backend_url
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("db_config", "backend_url")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_preload_table_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: preload_table
        expected: status not ok
        '''
        invalid_configs = ["preloadtable", "preload_table "]
        for config in invalid_configs:
            status, config_value = connect.get_config("db_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_preload_table_valid(self, connect, collection):
        '''
        target: get preload_table
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("db_config", "preload_table")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_auto_flush_interval_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: auto_flush_interval
        expected: status not ok
        '''
        invalid_configs = ["autoFlushInterval", "auto_flush", "auto_flush_interval "]
        for config in invalid_configs:
            status, config_value = connect.get_config("db_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_auto_flush_interval_valid(self, connect, collection):
        '''
        target: get auto_flush_interval
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("db_config", "auto_flush_interval")
        assert status.OK()


    """
    ******************************************************************
      The following cases are used to test `set_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_db_config_invalid_child_key(self, connect, collection):
        '''
        target: set invalid child key
        method: call set_config with invalid child_key
        expected: status not ok
        '''
        status, reply = connect.set_config("db_config", "child_key", 1)
        assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_backend_url_valid(self, connect, collection):
        '''
        target: set backend_url
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        status, reply = connect.set_config("db_config", "backend_url", 'sqlite://:@:/')
        assert status.OK()
        status, config_value = connect.get_config("db_config", "backend_url")
        assert status.OK()
        assert config_value == 'sqlite://:@:/'

    def test_set_preload_table_valid(self, connect, collection):
        '''
        target: set preload_table
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        status, reply = connect.set_config("db_config", "preload_table", "")
        assert status.OK()
        status, config_value = connect.get_config("db_config", "preload_table")
        assert status.OK()
        assert config_value == ""

    def test_set_auto_flush_interval_valid(self, connect, collection):
        '''
        target: set auto_flush_interval
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        for valid_auto_flush_interval in [0, 15, "3", 1]:
            status, reply = connect.set_config("db_config", "auto_flush_interval", valid_auto_flush_interval)
            assert status.OK()
            status, config_value = connect.get_config("db_config", "auto_flush_interval")
            assert status.OK()
            assert config_value == str(valid_auto_flush_interval)
    
    def test_set_auto_flush_interval_invalid(self, connect, collection):
        '''
        target: set auto_flush_interval
        method: call set_config with invalid auto_flush_interval
        expected: status not ok
        '''
        for invalid_auto_flush_interval in [-1, "1.5", "invalid", "1+2"]:
            status, reply = connect.set_config("db_config", "auto_flush_interval", invalid_auto_flush_interval)
            assert not status.OK()


class TestStorageConfig:
    """
    ******************************************************************
      The following cases are used to test `get_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_primary_path_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: primary_path
        expected: status not ok
        '''
        invalid_configs = ["Primary_path", "primarypath", "primary_path "]
        for config in invalid_configs:
            status, config_value = connect.get_config("storage_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_primary_path_valid(self, connect, collection):
        '''
        target: get primary_path
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("storage_config", "primary_path")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_secondary_path_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: secondary_path
        expected: status not ok
        '''
        invalid_configs = ["secondarypath", "secondary_path "]
        for config in invalid_configs:
            status, config_value = connect.get_config("storage_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_secondary_path_valid(self, connect, collection):
        '''
        target: get secondary_path
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("storage_config", "secondary_path")
        assert status.OK()


    """
    ******************************************************************
      The following cases are used to test `set_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_storage_config_invalid_child_key(self, connect, collection):
        '''
        target: set invalid child key
        method: call set_config with invalid child_key
        expected: status not ok
        '''
        status, reply = connect.set_config("storage_config", "child_key", "")
        assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_primary_path_valid(self, connect, collection):
        '''
        target: set primary_path
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        status, reply = connect.set_config("storage_config", "primary_path", '/var/lib/milvus')
        assert status.OK()
        status, config_value = connect.get_config("storage_config", "primary_path")
        assert status.OK()
        assert config_value == '/var/lib/milvus'

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_secondary_path_valid(self, connect, collection):
        '''
        target: set secondary_path
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        status, reply = connect.set_config("storage_config", "secondary_path", "")
        assert status.OK()
        status, config_value = connect.get_config("storage_config", "secondary_path")
        assert status.OK()
        assert config_value == ""


class TestMetricConfig:
    """
    ******************************************************************
      The following cases are used to test `get_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_enable_monitor_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: enable_monitor
        expected: status not ok
        '''
        invalid_configs = ["enablemonitor", "Enable_monitor", "enable_monitor "]
        for config in invalid_configs:
            status, config_value = connect.get_config("metric_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_enable_monitor_valid(self, connect, collection):
        '''
        target: get enable_monitor
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("metric_config", "enable_monitor")
        assert status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_address_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: address
        expected: status not ok
        '''
        invalid_configs = ["Address", "addresses", "address "]
        for config in invalid_configs:
            status, config_value = connect.get_config("metric_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_address_valid(self, connect, collection):
        '''
        target: get address
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("metric_config", "address")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_port_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: port
        expected: status not ok
        '''
        invalid_configs = ["Port", "PORT", "port "]
        for config in invalid_configs:
            status, config_value = connect.get_config("metric_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_port_valid(self, connect, collection):
        '''
        target: get port
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("metric_config", "port")
        assert status.OK()


    """
    ******************************************************************
      The following cases are used to test `set_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_metric_config_invalid_child_key(self, connect, collection):
        '''
        target: set invalid child key
        method: call set_config with invalid child_key
        expected: status not ok
        '''
        status, reply = connect.set_config("metric_config", "child_key", 19530)
        assert not status.OK()

    def test_set_enable_monitor_valid(self, connect, collection):
        '''
        target: set enable_monitor
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        for valid_enable_monitor in ["Off", "false", 0, "yes", "On", "true", "1", "NO"]:
            status, reply = connect.set_config("metric_config", "enable_monitor", valid_enable_monitor)
            assert status.OK()
            status, config_value = connect.get_config("metric_config", "enable_monitor")
            assert status.OK()
            assert config_value == str(valid_enable_monitor)

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_address_valid(self, connect, collection):
        '''
        target: set address
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        status, reply = connect.set_config("metric_config", "address", '127.0.0.1')
        assert status.OK()
        status, config_value = connect.get_config("metric_config", "address")
        assert status.OK()
        assert config_value == '127.0.0.1'

    def test_set_port_valid(self, connect, collection):
        '''
        target: set port
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        for valid_port in [1025, 65534, "19530", "9091"]:
            status, reply = connect.set_config("metric_config", "port", valid_port)
            assert status.OK()
            status, config_value = connect.get_config("metric_config", "port")
            assert status.OK()
            assert config_value == str(valid_port)
    
    def test_set_port_invalid(self, connect, collection):
        '''
        target: set port
        method: call set_config with port number out of range(1024, 65535), or same as web_port number
        expected: status not ok
        '''
        for invalid_port in [1024, 65535, "0", "True", "19530 ", "100000"]:
            status, reply = connect.set_config("metric_config", "port", invalid_port)
            assert not status.OK()


class TestTracingConfig:
    """
    ******************************************************************
      The following cases are used to test `get_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_json_config_path_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: json_config_path
        expected: status not ok
        '''
        invalid_configs = ["json_config", "jsonconfigpath", "json_config_path "]
        for config in invalid_configs:
            status, config_value = connect.get_config("tracing_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_json_config_path_valid(self, connect, collection):
        '''
        target: get json_config_path
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("tracing_config", "json_config_path")
        assert status.OK()


    """
    ******************************************************************
      The following cases are used to test `set_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_tracing_config_invalid_child_key(self, connect, collection):
        '''
        target: set invalid child key
        method: call set_config with invalid child_key
        expected: status not ok
        '''
        status, reply = connect.set_config("tracing_config", "child_key", "")
        assert not status.OK()

    @pytest.mark.skip(reason="Currently not supported")
    def test_set_json_config_path_valid(self, connect, collection):
        '''
        target: set json_config_path
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        status, reply = connect.set_config("tracing_config", "json_config_path", "")
        assert status.OK()
        status, config_value = connect.get_config("tracing_config", "json_config_path")
        assert status.OK()
        assert config_value == ""


class TestWALConfig:
    """
    ******************************************************************
      The following cases are used to test `get_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_enable_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: enable
        expected: status not ok
        '''
        invalid_configs = ["enabled", "Enable", "enable "]
        for config in invalid_configs:
            status, config_value = connect.get_config("wal_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_enable_valid(self, connect, collection):
        '''
        target: get enable
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("wal_config", "enable")
        assert status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_recovery_error_ignore_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: recovery_error_ignore
        expected: status not ok
        '''
        invalid_configs = ["recovery-error-ignore", "Recovery_error_ignore", "recovery_error_ignore "]
        for config in invalid_configs:
            status, config_value = connect.get_config("wal_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_recovery_error_ignore_valid(self, connect, collection):
        '''
        target: get recovery_error_ignore
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("wal_config", "recovery_error_ignore")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_buffer_size_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: buffer_size
        expected: status not ok
        '''
        invalid_configs = ["buffersize", "Buffer_size", "buffer_size "]
        for config in invalid_configs:
            status, config_value = connect.get_config("wal_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_buffer_size_valid(self, connect, collection):
        '''
        target: get buffer_size
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("wal_config", "buffer_size")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_wal_path_invalid_child_key(self, connect, collection):
        '''
        target: get invalid child key
        method: call get_config without child_key: wal_path
        expected: status not ok
        '''
        invalid_configs = ["wal", "Wal_path", "wal_path "]
        for config in invalid_configs:
            status, config_value = connect.get_config("wal_config", config)
            assert not status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_wal_path_valid(self, connect, collection):
        '''
        target: get wal_path
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("wal_config", "wal_path")
        assert status.OK()


    """
    ******************************************************************
      The following cases are used to test `set_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_wal_config_invalid_child_key(self, connect, collection):
        '''
        target: set invalid child key
        method: call set_config with invalid child_key
        expected: status not ok
        '''
        status, reply = connect.set_config("wal_config", "child_key", 256)
        assert not status.OK()

    def test_set_enable_valid(self, connect, collection):
        '''
        target: set enable
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        for valid_enable in ["Off", "false", 0, "no", "On", "true", "1", "YES"]:
            status, reply = connect.set_config("wal_config", "enable", valid_enable)
            assert status.OK()
            status, config_value = connect.get_config("wal_config", "enable")
            assert status.OK()
            assert config_value == str(valid_enable)

    def test_set_recovery_error_ignore_valid(self, connect, collection):
        '''
        target: set recovery_error_ignore
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        for valid_recovery_error_ignore in ["Off", "false", "0", "no", "On", "true", "1", "YES"]:
            status, reply = connect.set_config("wal_config", "recovery_error_ignore", valid_recovery_error_ignore)
            assert status.OK()
            status, config_value = connect.get_config("wal_config", "recovery_error_ignore")
            assert status.OK()
            assert config_value == valid_recovery_error_ignore

    def test_set_buffer_size_valid_A(self, connect, collection):
        '''
        target: set buffer_size
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        for valid_buffer_size in [64, 128, "4096", 1000, "256"]:
            status, reply = connect.set_config("wal_config", "buffer_size", valid_buffer_size)
            assert status.OK()
            status, config_value = connect.get_config("wal_config", "buffer_size")
            assert status.OK()
            assert config_value == str(valid_buffer_size)
        
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_wal_path_valid(self, connect, collection):
        '''
        target: set wal_path
        method: call set_config correctly
        expected: status ok, set successfully
        '''
        status, reply = connect.set_config("wal_config", "wal_path", "/var/lib/milvus/wal")
        assert status.OK()
        status, config_value = connect.get_config("wal_config", "wal_path")
        assert status.OK()
        assert config_value == "/var/lib/milvus/wal"
