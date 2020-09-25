CREATE TABLE 'TempTables' ( 'id' INTEGER PRIMARY KEY NOT NULL , 'table_id' TEXT UNIQUE NOT NULL , 'state' INTEGER NOT NULL , 'dimension' INTEGER NOT NULL , 'created_on' INTEGER NOT NULL , 'flag' INTEGER DEFAULT 0 NOT NULL , 'index_file_size' INTEGER NOT NULL , 'engine_type' INTEGER NOT NULL , 'nlist' INTEGER NOT NULL , 'metric_type' INTEGER NOT NULL);

INSERT INTO TempTables SELECT id, table_id, state, dimension, created_on, flag, index_file_size, engine_type, nlist, metric_type FROM Tables;

DROP TABLE Tables;

ALTER TABLE TempTables RENAME TO Tables;
