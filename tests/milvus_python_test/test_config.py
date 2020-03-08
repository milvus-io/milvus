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
    def test_get_cpu_cache_capacity_invalid_parent_key(self, connect, table):
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
    def test_get_cpu_cache_capacity_invalid_child_key(self, connect, table):
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
    def test_get_cpu_cache_capacity_valid(self, connect, table):
        '''
        target: get cpu_cache_capacity
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("cache_config", "cpu_cache_capacity")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_insert_buffer_size_invalid_parent_key(self, connect, table):
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
    def test_get_insert_buffer_size_invalid_child_key(self, connect, table):
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
    def test_get_insert_buffer_size_valid(self, connect, table):
        '''
        target: get insert_buffer_size
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("cache_config", "insert_buffer_size")
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

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_cpu_cache_capacity_invalid_parent_key(self, connect, table):
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
    def test_set_cache_config_invalid_child_key(self, connect, table):
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
    def test_set_cpu_cache_capacity_valid(self, connect, table):
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

    def test_set_cpu_cache_capacity_valid_multiple_times(self, connect, table):
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
    def test_set_insert_buffer_size_invalid_parent_key(self, connect, table):
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
    def test_set_insert_buffer_size_valid(self, connect, table):
        '''
        target: set insert_buffer_size
        method: call get_config correctly
        expected: status ok
        '''
        self.reset_configs(connect)
        status, reply = connect.set_config("cache_config", "insert_buffer_size", 2)
        status, config_value = connect.get_config("cache_config", "insert_buffer_size")
        assert status.OK()
        assert config_value == '2'

    @pytest.mark.level(2)
    def test_set_insert_buffer_size_valid_multiple_times(self, connect, table):
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
    def test_set_cache_config_out_of_memory_value_A(self, connect, table):
        '''
        target: set cpu_cache_capacity / insert_buffer_size to be out-of-memory
        method: call set_config with child values bigger than current system memory
        expected: status not ok (cpu_cache_capacity + insert_buffer_size < system memory)
        '''
        self.reset_configs(connect)
        mem_available = self.get_memory_available(connect)
        logging.getLogger().info(mem_available)
        status, reply = connect.set_config("cache_config", "cpu_cache_capacity", mem_available + 1)
        assert not status.OK()
        status, reply = connect.set_config("cache_config", "insert_buffer_size", mem_available + 1)
        assert not status.OK()

    # TODO: CI FAIL
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def _test_set_cache_config_out_of_memory_value_B(self, connect, table):
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
        status, insert_buffer_size = connect.get_config("cache_config", "insert_buffer_size")
        assert status.OK()
        # status, reply = connect.set_config("cache_config", "cpu_cache_capacity", mem_available - int(insert_buffer_size) + 1)
        # assert not status.OK()
        # status, reply = connect.set_config("cache_config", "insert_buffer_size", mem_available - int(cpu_cache_capacity) + 1)
        # assert not status.OK()

    def test_set_cache_config_out_of_memory_value_C(self, connect, table):
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


class TestEngineConfig:
    """
    ******************************************************************
      The following cases are used to test `get_config` function
    ******************************************************************
    """
    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_get_use_blas_threshold_invalid_parent_key(self, connect, table):
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
    def test_get_use_blas_threshold_invalid_child_key(self, connect, table):
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
    def test_get_use_blas_threshold_valid(self, connect, table):
        '''
        target: get use_blas_threshold
        method: call get_config correctly
        expected: status ok
        '''
        status, config_value = connect.get_config("engine_config", "use_blas_threshold")
        assert status.OK()

    @pytest.mark.level(2)
    def test_get_gpu_search_threshold_invalid_parent_key(self, connect, table):
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
    def test_get_gpu_search_threshold_invalid_child_key(self, connect, table):
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
    def test_get_gpu_search_threshold_valid(self, connect, table):
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
    def test_set_use_blas_threshold_invalid_parent_key(self, connect, table):
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
    def test_set_engine_config_invalid_child_key(self, connect, table):
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
    def test_set_use_blas_threshold_valid(self, connect, table):
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
    def test_set_use_blas_threshold_valid_multiple_times(self, connect, table):
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

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_gpu_search_threshold_invalid_parent_key(self, connect, table):
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
    def test_set_gpu_search_threshold_valid(self, connect, table):
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
    def test_set_gpu_search_threshold_valid_multiple_times(self, connect, table):
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
    def test_set_engine_config_invalid_values(self, connect, table):
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
    def test_get_gpu_enable_invalid_parent_key(self, connect, table):
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
    def test_get_gpu_enable_invalid_child_key(self, connect, table):
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
    def test_get_gpu_enable_valid(self, connect, table):
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
    def test_get_cache_capacity_invalid_parent_key(self, connect, table):
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
    def test_get_cache_capacity_invalid_child_key(self, connect, table):
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
    def test_get_cache_capacity_valid(self, connect, table):
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
    def test_get_search_resources_invalid_parent_key(self, connect, table):
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
    def test_get_search_resources_invalid_child_key(self, connect, table):
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
    def test_get_search_resources_valid(self, connect, table):
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
    def test_get_build_index_resources_invalid_parent_key(self, connect, table):
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
    def test_get_build_index_resources_invalid_child_key(self, connect, table):
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
    def test_get_build_index_resources_valid(self, connect, table):
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
    def test_set_gpu_enable_invalid_parent_key(self, connect, table):
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
    def test_set_gpu_resource_config_invalid_child_key(self, connect, table):
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
    def test_set_gpu_enable_invalid_values(self, connect, table):
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
    def test_set_gpu_enable_valid(self, connect, table):
        '''
        target: set "enable" param
        method: call set_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        status, reply = connect.set_config("gpu_resource_config", "enable", "false")
        assert status.OK()
        status, config_value = connect.get_config("gpu_resource_config", "enable")
        assert config_value == "false"
        status, reply = connect.set_config("gpu_resource_config", "enable", "true")
        assert status.OK()
        status, config_value = connect.get_config("gpu_resource_config", "enable")
        assert config_value == "true"
        status, reply = connect.set_config("gpu_resource_config", "enable", 0)
        assert status.OK()
        status, config_value = connect.get_config("gpu_resource_config", "enable")
        assert config_value == "0"
        status, reply = connect.set_config("gpu_resource_config", "enable", 1)
        assert status.OK()
        status, config_value = connect.get_config("gpu_resource_config", "enable")
        assert config_value == "1"
        status, reply = connect.set_config("gpu_resource_config", "enable", "off")
        assert status.OK()
        status, config_value = connect.get_config("gpu_resource_config", "enable")
        assert config_value == "off"
        status, reply = connect.set_config("gpu_resource_config", "enable", "ON")
        assert status.OK()
        status, config_value = connect.get_config("gpu_resource_config", "enable")
        assert config_value == "ON"

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_cache_capacity_invalid_parent_key(self, connect, table):
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
    def test_set_cache_capacity_valid(self, connect, table):
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
    def test_set_cache_capacity_invalid_values(self, connect, table):
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
    def test_set_search_resources_invalid_parent_key(self, connect, table):
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
    def test_set_search_resources_valid(self, connect, table):
        '''
        target: set search_resources
        method: call set_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        for i in ["gpu0"]:
            status, reply = connect.set_config("gpu_resource_config", "search_resources", i)
            assert status.OK()
            status, config_value = connect.get_config("gpu_resource_config", "search_resources")
            assert config_value == i

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_search_resources_invalid_values(self, connect, table):
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
    def test_set_build_index_resources_invalid_parent_key(self, connect, table):
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
    def test_set_build_index_resources_valid(self, connect, table):
        '''
        target: set build_index_resources
        method: call set_config correctly
        expected: status ok
        '''
        if str(connect._cmd("mode")[1]) == "CPU":
            pytest.skip("Only support GPU mode")
        for i in ["gpu0"]:
            status, reply = connect.set_config("gpu_resource_config", "build_index_resources", i)
            assert status.OK()

    @pytest.mark.timeout(CONFIG_TIMEOUT)
    def test_set_build_index_resources_invalid_values(self, connect, table):
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
