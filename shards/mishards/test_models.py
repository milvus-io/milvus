import logging
import pytest
from mishards.factories import TableFiles, Tables, TableFilesFactory, TablesFactory
from mishards import db, create_app, settings
from mishards.factories import (
    Tables, TableFiles,
    TablesFactory, TableFilesFactory
)

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures('app')
class TestModels:
    def test_files_to_search(self):
        table = TablesFactory()
        new_files_cnt = 5
        to_index_cnt = 10
        raw_cnt = 20
        backup_cnt = 12
        to_delete_cnt = 9
        index_cnt = 8
        new_index_cnt = 6
        new_merge_cnt = 11

        new_files = TableFilesFactory.create_batch(new_files_cnt, table=table, file_type=TableFiles.FILE_TYPE_NEW, date=110)
        to_index_files = TableFilesFactory.create_batch(to_index_cnt, table=table, file_type=TableFiles.FILE_TYPE_TO_INDEX, date=110)
        raw_files = TableFilesFactory.create_batch(raw_cnt, table=table, file_type=TableFiles.FILE_TYPE_RAW, date=120)
        backup_files = TableFilesFactory.create_batch(backup_cnt, table=table, file_type=TableFiles.FILE_TYPE_BACKUP, date=110)
        index_files = TableFilesFactory.create_batch(index_cnt, table=table, file_type=TableFiles.FILE_TYPE_INDEX, date=110)
        new_index_files = TableFilesFactory.create_batch(new_index_cnt, table=table, file_type=TableFiles.FILE_TYPE_NEW_INDEX, date=110)
        new_merge_files = TableFilesFactory.create_batch(new_merge_cnt, table=table, file_type=TableFiles.FILE_TYPE_NEW_MERGE, date=110)
        to_delete_files = TableFilesFactory.create_batch(to_delete_cnt, table=table, file_type=TableFiles.FILE_TYPE_TO_DELETE, date=110)
        assert table.files_to_search().count() == raw_cnt + index_cnt + to_index_cnt

        assert table.files_to_search([(100, 115)]).count() == index_cnt + to_index_cnt
        assert table.files_to_search([(111, 120)]).count() == 0
        assert table.files_to_search([(111, 121)]).count() == raw_cnt
        assert table.files_to_search([(110, 121)]).count() == raw_cnt + index_cnt + to_index_cnt
