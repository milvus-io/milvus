from client.Client import MegaSearch, Prepare, IndexType, ColumnType
from client.Status import Status
import time

from megasearch.thrift import MegasearchService, ttypes


def main():
    mega = MegaSearch()

    # Connect
    param = {'host': '192.168.1.129', 'port': '33001'}
    cnn_status = mega.connect(**param)
    print('Connect Status: {}'.format(cnn_status))

    is_connected = mega.connected
    print('Connect status: {}'.format(is_connected))

    # Create table with 1 vector column, 1 attribute column and 1 partition column
    # 1. prepare table_schema

    # table_schema = Prepare.table_schema(
    #     table_name='fake_table_name' + time.strftime('%H%M%S'),
    #
    #     vector_columns=[Prepare.vector_column(
    #         name='fake_vector_name' + time.strftime('%H%M%S'),
    #         store_raw_vector=False,
    #         dimension=256)],
    #
    #     attribute_columns=[],
    #
    #     partition_column_names=[]
    # )

    # get server version
    print(mega.server_status('version'))

    # show tables and their description
    statu, tables = mega.show_tables()
    print(tables)

    for table in tables:
        s,t = mega.describe_table(table)
        print('table: {}'.format(t))

    # Create table
    # 1. create table schema
    table_schema_full = MegasearchService.TableSchema(
        table_name='fake' + time.strftime('%H%M%S'),

        vector_column_array=[MegasearchService.VectorColumn(
            base=MegasearchService.Column(
                name='111',
                type=ttypes.TType.I32
            ),
            dimension=256,
        )],

        attribute_column_array=[],

        partition_column_name_array=None
    )

    table_schema_empty = MegasearchService.TableSchema(
        table_name='fake' + time.strftime('%H%M%S'),

        vector_column_array=[MegasearchService.VectorColumn()],

        attribute_column_array=[],

        partition_column_name_array=None
    )
    # 2. Create Table
    create_status = mega.create_table(table_schema_full)
    print('Create table status: {}'.format(create_status))

    # add_vector

    # Disconnect
    discnn_status = mega.disconnect()
    print('Disconnect Status{}'.format(discnn_status))


if __name__ == '__main__':
    main()