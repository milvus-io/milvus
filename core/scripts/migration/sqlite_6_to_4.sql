CREATE TABLE 'TempTables' AS SELECT id, table_id, state, dimension, created_on, flag, index_file_size, engine_type, nlist, metric_type FROM Tables;

DROP TABLE Tables;

ALTER TABLE TempTables RENAME TO Tables;
