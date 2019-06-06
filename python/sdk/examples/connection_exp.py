from client.Client import MegaSearch, Prepare, IndexType, ColumnType
from client.Status import Status


def main():
    mega = MegaSearch()

    # Connect
    param = {'host': '192.168.1.129', 'port': '33001'}
    cnn_status = mega.connect(**param)
    print('Connect Status: {}'.format(cnn_status))

    is_connected = mega.connected
    print('Connect status: {}'.format(is_connected))

    # # Create table with 1 vector column, 1 attribute column and 1 partition column
    # # 1. prepare table_schema
    # vector_column = {
    #     'name': 'fake_vec_name01',
    #     'store_raw_vector': True,
    #     'dimension': 10
    # }
    # attribute_column = {
    #     'name': 'fake_attri_name01',
    #     'type': ColumnType.DATE,
    # }
    #
    # table = {
    #     'table_name': 'fake_table_name01',
    #     'vector_columns': [Prepare.vector_column(**vector_column)],
    #     'attribute_columns': [Prepare.column(**attribute_column)],
    #     'partition_column_names': ['fake_attri_name01']
    # }
    # table_schema = Prepare.table_schema(**table)
    #
    # # 2. Create Table
    # create_status = mega.create_table(table_schema)
    # print('Create table status: {}'.format(create_status))

    mega.server_status('ok!')

    # Disconnect
    discnn_status = mega.disconnect()
    print('Disconnect Status{}'.format(discnn_status))


if __name__ == '__main__':
    main()