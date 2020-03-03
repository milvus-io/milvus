import random
import pdb
import pytest
import logging
import itertools
import numpy
from time import sleep
from multiprocessing import Process
from milvus import IndexType, MetricType
from utils import *

dim = 128
delete_table_interval_time = 3
index_file_size = 10
vectors = gen_vectors(100, dim)


class TestTable:

    """
    ******************************************************************
      The following cases are used to test `create_table` function
    ******************************************************************
    """

    def test_create_table(self, connect):
        '''
        target: test create normal table 
        method: create table with corrent params
        expected: create status return ok
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        assert status.OK()

    def test_create_table_ip(self, connect):
        '''
        target: test create normal table 
        method: create table with corrent params
        expected: create status return ok
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.IP}
        status = connect.create_table(param)
        assert status.OK()

    def test_create_table_jaccard(self, connect):
        '''
        target: test create normal table 
        method: create table with corrent params
        expected: create status return ok
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.JACCARD}
        status = connect.create_table(param)
        assert status.OK()

    def test_create_table_hamming(self, connect):
        '''
        target: test create normal table
        method: create table with corrent params
        expected: create status return ok
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.HAMMING}
        status = connect.create_table(param)
        assert status.OK()

    @pytest.mark.level(2)
    def test_create_table_without_connection(self, dis_connect):
        '''
        target: test create table, without connection
        method: create table with correct params, with a disconnected instance
        expected: create raise exception
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.L2}
        with pytest.raises(Exception) as e:
            status = dis_connect.create_table(param)

    def test_create_table_existed(self, connect):
        '''
        target: test create table but the table name have already existed
        method: create table with the same table_name
        expected: create status return not ok
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        status = connect.create_table(param)
        assert not status.OK()

    @pytest.mark.level(2)
    def test_create_table_existed_ip(self, connect):
        '''
        target: test create table but the table name have already existed
        method: create table with the same table_name
        expected: create status return not ok
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.IP}
        status = connect.create_table(param)
        status = connect.create_table(param)
        assert not status.OK()

    def test_create_table_None(self, connect):
        '''
        target: test create table but the table name is None
        method: create table, param table_name is None
        expected: create raise error
        '''
        param = {'table_name': None,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.L2}
        with pytest.raises(Exception) as e:
            status = connect.create_table(param)

    def test_create_table_no_dimension(self, connect):
        '''
        target: test create table with no dimension params
        method: create table with corrent params
        expected: create status return ok
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        with pytest.raises(Exception) as e:
            status = connect.create_table(param)

    def test_create_table_no_file_size(self, connect):
        '''
        target: test create table with no index_file_size params
        method: create table with corrent params
        expected: create status return ok, use default 1024
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        logging.getLogger().info(status)
        status, result = connect.describe_table(table_name)
        logging.getLogger().info(result)
        assert result.index_file_size == 1024

    def test_create_table_no_metric_type(self, connect):
        '''
        target: test create table with no metric_type params
        method: create table with corrent params
        expected: create status return ok, use default L2
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size}
        status = connect.create_table(param)
        status, result = connect.describe_table(table_name)
        logging.getLogger().info(result)
        assert result.metric_type == MetricType.L2

    """
    ******************************************************************
      The following cases are used to test `describe_table` function
    ******************************************************************
    """

    def test_table_describe_result(self, connect):
        '''
        target: test describe table created with correct params 
        method: create table, assert the value returned by describe method
        expected: table_name equals with the table name created
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        connect.create_table(param)
        status, res = connect.describe_table(table_name)
        assert res.table_name == table_name
        assert res.metric_type == MetricType.L2

    @pytest.mark.level(2)
    def test_table_describe_table_name_ip(self, connect):
        '''
        target: test describe table created with correct params 
        method: create table, assert the value returned by describe method
        expected: table_name equals with the table name created
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.IP}
        connect.create_table(param)
        status, res = connect.describe_table(table_name)
        assert res.table_name == table_name
        assert res.metric_type == MetricType.IP

    @pytest.mark.level(2)
    def test_table_describe_table_name_jaccard(self, connect):
        '''
        target: test describe table created with correct params 
        method: create table, assert the value returned by describe method
        expected: table_name equals with the table name created
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.JACCARD}
        connect.create_table(param)
        status, res = connect.describe_table(table_name)
        assert res.table_name == table_name
        assert res.metric_type == MetricType.JACCARD

    @pytest.mark.level(2)
    def test_table_describe_table_name_hamming(self, connect):
        '''
        target: test describe table created with correct params
        method: create table, assert the value returned by describe method
        expected: table_name equals with the table name created
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.HAMMING}
        connect.create_table(param)
        status, res = connect.describe_table(table_name)
        assert res.table_name == table_name
        assert res.metric_type == MetricType.HAMMING

    # TODO: enable
    @pytest.mark.level(2)
    def _test_table_describe_table_name_multiprocessing(self, connect, args):
        '''
        target: test describe table created with multiprocess 
        method: create table, assert the value returned by describe method
        expected: table_name equals with the table name created
        '''
        table_name = gen_unique_str("test_table")
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size, 
                 'metric_type': MetricType.L2}
        connect.create_table(param)

        def describetable(milvus):
            status, res = milvus.describe_table(table_name)
            assert res.table_name == table_name

        process_num = 4
        processes = []
        for i in range(process_num):
            milvus = get_milvus(args["handler"])
            milvus.connect(uri=uri)
            p = Process(target=describetable, args=(milvus,))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()
    
    @pytest.mark.level(2)
    def test_table_describe_without_connection(self, table, dis_connect):
        '''
        target: test describe table, without connection
        method: describe table with correct params, with a disconnected instance
        expected: describe raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.describe_table(table)

    def test_table_describe_dimension(self, connect):
        '''
        target: test describe table created with correct params 
        method: create table, assert the dimention value returned by describe method
        expected: dimention equals with dimention when created
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim+1,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        connect.create_table(param)
        status, res = connect.describe_table(table_name)
        assert res.dimension == dim+1

    """
    ******************************************************************
      The following cases are used to test `delete_table` function
    ******************************************************************
    """

    def test_delete_table(self, connect, table):
        '''
        target: test delete table created with correct params 
        method: create table and then delete, 
            assert the value returned by delete method
        expected: status ok, and no table in tables
        '''
        status = connect.delete_table(table)
        assert not assert_has_table(connect, table)

    @pytest.mark.level(2)
    def test_delete_table_ip(self, connect, ip_table):
        '''
        target: test delete table created with correct params 
        method: create table and then delete, 
            assert the value returned by delete method
        expected: status ok, and no table in tables
        '''
        status = connect.delete_table(ip_table)
        assert not assert_has_table(connect, ip_table)

    @pytest.mark.level(2)
    def test_delete_table_jaccard(self, connect, jac_table):
        '''
        target: test delete table created with correct params 
        method: create table and then delete, 
            assert the value returned by delete method
        expected: status ok, and no table in tables
        '''
        status = connect.delete_table(jac_table)
        assert not assert_has_table(connect, jac_table)

    @pytest.mark.level(2)
    def test_delete_table_hamming(self, connect, ham_table):
        '''
        target: test delete table created with correct params
        method: create table and then delete,
            assert the value returned by delete method
        expected: status ok, and no table in tables
        '''
        status = connect.delete_table(ham_table)
        assert not assert_has_table(connect, ham_table)

    @pytest.mark.level(2)
    def test_table_delete_without_connection(self, table, dis_connect):
        '''
        target: test describe table, without connection
        method: describe table with correct params, with a disconnected instance
        expected: describe raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.delete_table(table)

    def test_delete_table_not_existed(self, connect):
        '''
        target: test delete table not in index
        method: delete all tables, and delete table again, 
            assert the value returned by delete method
        expected: status not ok
        '''
        table_name = gen_unique_str("test_table")
        status = connect.delete_table(table_name)
        assert not status.OK()

    def test_delete_table_repeatedly(self, connect):
        '''
        target: test delete table created with correct params 
        method: create table and delete new table repeatedly, 
            assert the value returned by delete method
        expected: create ok and delete ok
        '''
        loops = 1
        for i in range(loops):
            table_name = gen_unique_str("test_table")
            param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
            connect.create_table(param)
            status = connect.delete_table(table_name)
            time.sleep(1)
            assert not assert_has_table(connect, table_name)

    def test_delete_create_table_repeatedly(self, connect):
        '''
        target: test delete and create the same table repeatedly
        method: try to create the same table and delete repeatedly,
            assert the value returned by delete method
        expected: create ok and delete ok
        '''
        loops = 5
        for i in range(loops):
            table_name = "test_table"
            param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
            connect.create_table(param)
            status = connect.delete_table(table_name)
            time.sleep(2)
            assert status.OK()

    def test_delete_create_table_repeatedly_ip(self, connect):
        '''
        target: test delete and create the same table repeatedly
        method: try to create the same table and delete repeatedly,
            assert the value returned by delete method
        expected: create ok and delete ok
        '''
        loops = 5
        for i in range(loops):
            table_name = "test_table"
            param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.IP}
            connect.create_table(param)
            status = connect.delete_table(table_name)
            time.sleep(2)
            assert status.OK()

    # TODO: enable
    @pytest.mark.level(2)
    def _test_delete_table_multiprocessing(self, args):
        '''
        target: test delete table with multiprocess 
        method: create table and then delete, 
            assert the value returned by delete method
        expected: status ok, and no table in tables
        '''
        process_num = 6
        processes = []
        uri = "tcp://%s:%s" % (args["ip"], args["port"])

        def deletetable(milvus):
            status = milvus.delete_table(table)
            # assert not status.code==0
            assert assert_has_table(milvus, table)
            assert status.OK()

        for i in range(process_num):
            milvus = get_milvus(args["handler"])
            milvus.connect(uri=uri)
            p = Process(target=deletetable, args=(milvus,))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

    # TODO: enable
    @pytest.mark.level(2)
    def _test_delete_table_multiprocessing_multitable(self, connect):
        '''
        target: test delete table with multiprocess 
        method: create table and then delete, 
            assert the value returned by delete method
        expected: status ok, and no table in tables
        '''
        process_num = 5
        loop_num = 2
        processes = []

        table = []
        j = 0
        while j < (process_num*loop_num):
            table_name = gen_unique_str("test_delete_table_with_multiprocessing")
            table.append(table_name)
            param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
            connect.create_table(param)
            j = j + 1

        def delete(connect,ids):
            i = 0
            while i < loop_num:
                status = connect.delete_table(table[ids*process_num+i])
                time.sleep(2)
                assert status.OK()
                assert not assert_has_table(connect, table[ids*process_num+i])
                i = i + 1

        for i in range(process_num):
            ids = i
            p = Process(target=delete, args=(connect,ids))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

    """
    ******************************************************************
      The following cases are used to test `has_table` function
    ******************************************************************
    """

    def test_has_table(self, connect):
        '''
        target: test if the created table existed
        method: create table, assert the value returned by has_table method
        expected: True
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        connect.create_table(param)
        assert assert_has_table(connect, table_name)

    def test_has_table_ip(self, connect):
        '''
        target: test if the created table existed
        method: create table, assert the value returned by has_table method
        expected: True
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.IP}
        connect.create_table(param)
        assert assert_has_table(connect, table_name)

    def test_has_table_jaccard(self, connect):
        '''
        target: test if the created table existed
        method: create table, assert the value returned by has_table method
        expected: True
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.JACCARD}
        connect.create_table(param)
        assert assert_has_table(connect, table_name)

    def test_has_table_hamming(self, connect):
        '''
        target: test if the created table existed
        method: create table, assert the value returned by has_table method
        expected: True
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.HAMMING}
        connect.create_table(param)
        assert assert_has_table(connect, table_name)

    @pytest.mark.level(2)
    def test_has_table_without_connection(self, table, dis_connect):
        '''
        target: test has table, without connection
        method: calling has table with correct params, with a disconnected instance
        expected: has table raise exception
        '''
        with pytest.raises(Exception) as e:
            assert_has_table(dis_connect, table)

    def test_has_table_not_existed(self, connect):
        '''
        target: test if table not created
        method: random a table name, which not existed in db, 
            assert the value returned by has_table method
        expected: False
        '''
        table_name = gen_unique_str("test_table")
        assert not assert_has_table(connect, table_name)

    """
    ******************************************************************
      The following cases are used to test `show_tables` function
    ******************************************************************
    """

    def test_show_tables(self, connect):
        '''
        target: test show tables is correct or not, if table created
        method: create table, assert the value returned by show_tables method is equal to 0
        expected: table_name in show tables   
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        connect.create_table(param)    
        status, result = connect.show_tables()
        assert status.OK()
        assert table_name in result

    def test_show_tables_ip(self, connect):
        '''
        target: test show tables is correct or not, if table created
        method: create table, assert the value returned by show_tables method is equal to 0
        expected: table_name in show tables   
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.IP}
        connect.create_table(param)    
        status, result = connect.show_tables()
        assert status.OK()
        assert table_name in result

    def test_show_tables_jaccard(self, connect):
        '''
        target: test show tables is correct or not, if table created
        method: create table, assert the value returned by show_tables method is equal to 0
        expected: table_name in show tables   
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.JACCARD}
        connect.create_table(param)    
        status, result = connect.show_tables()
        assert status.OK()
        assert table_name in result

    def test_show_tables_hamming(self, connect):
        '''
        target: test show tables is correct or not, if table created
        method: create table, assert the value returned by show_tables method is equal to 0
        expected: table_name in show tables
        '''
        table_name = gen_unique_str("test_table")
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.HAMMING}
        connect.create_table(param)
        status, result = connect.show_tables()
        assert status.OK()
        assert table_name in result

    @pytest.mark.level(2)
    def test_show_tables_without_connection(self, dis_connect):
        '''
        target: test show_tables, without connection
        method: calling show_tables with correct params, with a disconnected instance
        expected: show_tables raise exception
        '''
        with pytest.raises(Exception) as e:
            status = dis_connect.show_tables()

    def test_show_tables_no_table(self, connect):
        '''
        target: test show tables is correct or not, if no table in db
        method: delete all tables,
            assert the value returned by show_tables method is equal to []
        expected: the status is ok, and the result is equal to []      
        '''
        status, result = connect.show_tables()
        if result:
            for table_name in result:
                connect.delete_table(table_name)
        time.sleep(delete_table_interval_time)
        status, result = connect.show_tables()
        assert status.OK()
        assert len(result) == 0

    # TODO: enable
    @pytest.mark.level(2)
    def _test_show_tables_multiprocessing(self, connect, args):
        '''
        target: test show tables is correct or not with processes
        method: create table, assert the value returned by show_tables method is equal to 0
        expected: table_name in show tables
        '''
        table_name = gen_unique_str("test_table")
        uri = "tcp://%s:%s" % (args["ip"], args["port"])
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        connect.create_table(param)

        def showtables(milvus):
            status, result = milvus.show_tables()
            assert status.OK()
            assert table_name in result

        process_num = 8
        processes = []

        for i in range(process_num):
            milvus = get_milvus(args["handler"])
            milvus.connect(uri=uri)
            p = Process(target=showtables, args=(milvus,))
            processes.append(p)
            p.start()
        for p in processes:
            p.join()

    """
    ******************************************************************
      The following cases are used to test `preload_table` function
    ******************************************************************
    """

    """
    generate valid create_index params
    """
    @pytest.fixture(
        scope="function",
        params=gen_simple_index_params()
    )
    def get_simple_index_params(self, request, connect):
        if str(connect._cmd("mode")[1]) == "CPU":
            if request.param["index_type"] == IndexType.IVF_SQ8H:
                pytest.skip("sq8h not support in open source")
        if request.param["index_type"] == IndexType.IVF_PQ:
            pytest.skip("Skip PQ Temporary")
        return request.param

    @pytest.mark.level(1)
    def test_preload_table(self, connect, table, get_simple_index_params):
        index_params = get_simple_index_params
        status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_params)
        status = connect.preload_table(table)
        assert status.OK()

    @pytest.mark.level(1)
    def test_preload_table_ip(self, connect, ip_table, get_simple_index_params):
        index_params = get_simple_index_params
        status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_params)
        status = connect.preload_table(ip_table)
        assert status.OK()

    @pytest.mark.level(1)
    def test_preload_table_jaccard(self, connect, jac_table, get_simple_index_params):
        index_params = get_simple_index_params
        status, ids = connect.add_vectors(jac_table, vectors)
        status = connect.create_index(jac_table, index_params)
        status = connect.preload_table(jac_table)
        assert status.OK()

    @pytest.mark.level(1)
    def test_preload_table_hamming(self, connect, ham_table, get_simple_index_params):
        index_params = get_simple_index_params
        status, ids = connect.add_vectors(ham_table, vectors)
        status = connect.create_index(ham_table, index_params)
        status = connect.preload_table(ham_table)
        assert status.OK()

    @pytest.mark.level(1)
    def test_preload_table_not_existed(self, connect, table):
        table_name = gen_unique_str()
        nlist = 16384
        index_param = {"index_type": IndexType.IVF_SQ8, "nlist": nlist}
        status, ids = connect.add_vectors(table, vectors)
        status = connect.create_index(table, index_param)
        status = connect.preload_table(table_name)
        assert not status.OK()

    @pytest.mark.level(2)
    def test_preload_table_not_existed_ip(self, connect, ip_table):
        table_name = gen_unique_str()
        nlist = 16384
        index_param = {"index_type": IndexType.IVF_SQ8, "nlist": nlist}
        status, ids = connect.add_vectors(ip_table, vectors)
        status = connect.create_index(ip_table, index_param)
        status = connect.preload_table(table_name)
        assert not status.OK()

    @pytest.mark.level(1)
    def test_preload_table_no_vectors(self, connect, table):
        status = connect.preload_table(table)
        assert status.OK()

    @pytest.mark.level(2)
    def test_preload_table_no_vectors_ip(self, connect, ip_table):
        status = connect.preload_table(ip_table)
        assert status.OK()

    # TODO: psutils get memory usage
    @pytest.mark.level(1)
    def test_preload_table_memory_usage(self, connect, table):
        pass


class TestTableInvalid(object):
    """
    Test creating table with invalid table names
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_table_names()
    )
    def get_table_name(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_create_table_with_invalid_tablename(self, connect, get_table_name):
        table_name = get_table_name
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        status = connect.create_table(param)
        assert not status.OK()

    def test_create_table_with_empty_tablename(self, connect):
        table_name = ''
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        with pytest.raises(Exception) as e:
            status = connect.create_table(param)

    def test_preload_table_with_invalid_tablename(self, connect):
        table_name = ''
        with pytest.raises(Exception) as e:
            status = connect.preload_table(table_name)


class TestCreateTableDimInvalid(object):
    """
    Test creating table with invalid dimension
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_dims()
    )
    def get_dim(self, request):
        yield request.param

    @pytest.mark.level(2)
    @pytest.mark.timeout(5)
    def test_create_table_with_invalid_dimension(self, connect, get_dim):
        dimension = get_dim
        table = gen_unique_str("test_create_table_with_invalid_dimension")
        param = {'table_name': table,
                 'dimension': dimension,
                 'index_file_size': index_file_size,
                 'metric_type': MetricType.L2}
        if isinstance(dimension, int):
            status = connect.create_table(param)
            assert not status.OK()
        else:
            with pytest.raises(Exception) as e:
                status = connect.create_table(param)
            

# TODO: max / min index file size
class TestCreateTableIndexSizeInvalid(object):
    """
    Test creating tables with invalid index_file_size
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_file_sizes()
    )
    def get_file_size(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_create_table_with_invalid_file_size(self, connect, table, get_file_size):
        file_size = get_file_size
        param = {'table_name': table,
                 'dimension': dim,
                 'index_file_size': file_size,
                 'metric_type': MetricType.L2}
        if isinstance(file_size, int):
            status = connect.create_table(param)
            assert not status.OK()
        else:
            with pytest.raises(Exception) as e:
                status = connect.create_table(param)


class TestCreateMetricTypeInvalid(object):
    """
    Test creating tables with invalid metric_type
    """
    @pytest.fixture(
        scope="function",
        params=gen_invalid_metric_types()
    )
    def get_metric_type(self, request):
        yield request.param

    @pytest.mark.level(2)
    def test_create_table_with_invalid_file_size(self, connect, table, get_metric_type):
        metric_type = get_metric_type
        param = {'table_name': table,
                 'dimension': dim,
                 'index_file_size': 10,
                 'metric_type': metric_type}
        with pytest.raises(Exception) as e:
            status = connect.create_table(param)


def create_table(connect, **params):
    param = {'table_name': params["table_name"],
             'dimension': params["dimension"],
             'index_file_size': index_file_size,
             'metric_type': MetricType.L2}
    status = connect.create_table(param)
    return status

def search_table(connect, **params):
    status, result = connect.search_vectors(
        params["table_name"], 
        params["top_k"], 
        params["nprobe"],
        params["query_vectors"])
    return status

def preload_table(connect, **params):
    status = connect.preload_table(params["table_name"])
    return status

def has(connect, **params):
    status, result = connect.has_table(params["table_name"])
    return status

def show(connect, **params):
    status, result = connect.show_tables()
    return status

def delete(connect, **params):
    status = connect.delete_table(params["table_name"])
    return status

def describe(connect, **params):
    status, result = connect.describe_table(params["table_name"])
    return status

def rowcount(connect, **params):
    status, result = connect.get_table_row_count(params["table_name"])
    return status

def create_index(connect, **params):
    status = connect.create_index(params["table_name"], params["index_params"])
    return status

func_map = { 
    # 0:has, 
    1:show,
    10:create_table, 
    11:describe,
    12:rowcount,
    13:search_table,
    14:preload_table,
    15:create_index,
    30:delete
}

def gen_sequence():
    raw_seq = func_map.keys()
    result = itertools.permutations(raw_seq)
    for x in result:
        yield x

class TestTableLogic(object):

    @pytest.mark.parametrize("logic_seq", gen_sequence())
    @pytest.mark.level(2)
    def test_logic(self, connect, logic_seq):
        if self.is_right(logic_seq):
            self.execute(logic_seq, connect)
        else:
            self.execute_with_error(logic_seq, connect)

    def is_right(self, seq):
        if sorted(seq) == seq:
            return True

        not_created = True
        has_deleted = False
        for i in range(len(seq)):
            if seq[i] > 10 and not_created:
                return False
            elif seq [i] > 10 and has_deleted:
                return False
            elif seq[i] == 10:
                not_created = False
            elif seq[i] == 30:
                has_deleted = True

        return True

    def execute(self, logic_seq, connect):
        basic_params = self.gen_params()
        for i in range(len(logic_seq)):
            # logging.getLogger().info(logic_seq[i])
            f = func_map[logic_seq[i]]
            status = f(connect, **basic_params)
            assert status.OK()

    def execute_with_error(self, logic_seq, connect):
        basic_params = self.gen_params()

        error_flag = False
        for i in range(len(logic_seq)):
            f = func_map[logic_seq[i]]
            status = f(connect, **basic_params)
            if not status.OK():
                # logging.getLogger().info(logic_seq[i])
                error_flag = True
                break
        assert error_flag == True

    def gen_params(self):
        table_name = gen_unique_str("test_table")
        top_k = 1
        vectors = gen_vectors(2, dim)
        param = {'table_name': table_name,
                 'dimension': dim,
                 'index_type': IndexType.IVFLAT,
                 'metric_type': MetricType.L2,
                 'nprobe': 1,
                 'top_k': top_k,
                 'index_params': {
                        'index_type': IndexType.IVF_SQ8,
                        'nlist': 16384
                 },
                 'query_vectors': vectors}
        return param
