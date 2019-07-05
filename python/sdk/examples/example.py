from client.Client import Milvus, Prepare, IndexType
import random
import struct
from pprint import pprint


def main():
    # Get client version
    milvus = Milvus()
    print('# Client version: {}'.format(milvus.client_version()))

    # Connect
    # Please change HOST and PORT to correct one
    param = {'host': 'HOST', 'port': 'PORT'}
    cnn_status = milvus.connect(**param)
    print('# Connect Status: {}'.format(cnn_status))

    # Check if connected
    is_connected = milvus.connected
    print('# Is connected: {}'.format(is_connected))

    # Get server version
    print('# Server version: {}'.format(milvus.server_version()))

    # Describe table
    # Check if `test01` exists, if not, create a table test01
    table_name = 'test01'
    res_status, table = milvus.describe_table(table_name)
    print('# Describe table status: {}'.format(res_status))
    print('# Describe table:{}'.format(table))

    # Create table
    #   01.Prepare data
    if not table:
        param = {
            'table_name': 'test01',
            'dimension': 256,
            'index_type': IndexType.IDMAP,
            'store_raw_vector': False
        }

        #   02.Create table
        res_status = milvus.create_table(Prepare.table_schema(**param))
        print('# Create table status: {}'.format(res_status))

    # # Create table Optional
    # #   01.Prepare data
    # param = {
    #     'table_name': 'test'+ str(random.randint(22,999)),
    #     'dimension': 256,
    #     'index_type': IndexType.IDMAP,
    #     'store_raw_vector': False
    # }
    #
    # #   02.Create table
    # res_status = milvus.create_table(Prepare.table_schema(**param))
    # print('# Create table status: {}'.format(res_status))

    # Show tables and their description
    status, tables = milvus.show_tables()
    print('# Show tables: {}'.format(tables))

    # Add vectors to table 'test01'
    #   01. Prepare data
    dim = 256
    # list of binary vectors
    vectors = [Prepare.row_record(struct.pack(str(dim)+'d',
                                              *[random.random()for _ in range(dim)]))
               for _ in range(20)]
    #   02. Add vectors
    status, ids = milvus.add_vectors(table_name=table_name, records=vectors)
    print('# Add vector status: {}'.format(status))
    pprint(ids)

    # Search vectors
    # When adding vectors for the first time, server will take at least 5s to
    # persist vector data, so we have wait for 6s to search correctly
    import time
    print('Waiting for 6s...')
    time.sleep(6)  # Wait for server persist vector data

    q_records = [Prepare.row_record(struct.pack(str(dim) + 'd',
                                                *[random.random() for _ in range(dim)]))
                 for _ in range(5)]
    param = {
        'table_name': 'test01',
        'query_records': q_records,
        'top_k': 10,
        # 'query_ranges':   # Optional
    }
    sta, results = milvus.search_vectors(**param)
    print('# Search vectors status: {}'.format(sta))
    pprint(results)

    # Get table row count
    sta, result = milvus.get_table_row_count(table_name)
    print('# Status: {}'.format(sta))
    print('# Count: {}'.format(result))

    # Disconnect
    discnn_status = milvus.disconnect()
    print('# Disconnect Status: {}'.format(discnn_status))


if __name__ == '__main__':
    main()
