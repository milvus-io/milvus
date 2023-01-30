/*
MySQL (Positive Technologies) grammar
The MIT License (MIT).
Copyright (c) 2015-2017, Ivan Kochurkin (kvanttt@gmail.com), Positive Technologies.
Copyright (c) 2017, Ivan Khudyashev (IHudyashov@ptsecurity.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

lexer grammar MySqlLexer;

options { caseInsensitive = true; }

channels { MYSQLCOMMENT, ERRORCHANNEL }

// SKIP

SPACE:                               [ \t\r\n]+    -> channel(HIDDEN);
SPEC_MYSQL_COMMENT:                  '/*!' .+? '*/' -> channel(MYSQLCOMMENT);
COMMENT_INPUT:                       '/*' .*? '*/' -> channel(HIDDEN);
LINE_COMMENT:                        (
                                       ('--' [ \t]* | '#') ~[\r\n]* ('\r'? '\n' | EOF)
                                       | '--' ('\r'? '\n' | EOF)
                                     ) -> channel(HIDDEN);


// Keywords
// Common Keywords

ADD:                                 'ADD';
ALL:                                 'ALL';
ALTER:                               'ALTER';
ALWAYS:                              'ALWAYS';
ANALYZE:                             'ANALYZE';
AND:                                 'AND';
ARRAY:                               'ARRAY';
AS:                                  'AS';
ASC:                                 'ASC';
ATTRIBUTE:                           'ATTRIBUTE';
BEFORE:                              'BEFORE';
BETWEEN:                             'BETWEEN';
BOTH:                                'BOTH';
BUCKETS:                             'BUCKETS';
BY:                                  'BY';
CALL:                                'CALL';
CASCADE:                             'CASCADE';
CASE:                                'CASE';
CAST:                                'CAST';
CHANGE:                              'CHANGE';
CHARACTER:                           'CHARACTER';
CHECK:                               'CHECK';
COLLATE:                             'COLLATE';
COLUMN:                              'COLUMN';
CONDITION:                           'CONDITION';
CONSTRAINT:                          'CONSTRAINT';
CONTINUE:                            'CONTINUE';
CONVERT:                             'CONVERT';
CREATE:                              'CREATE';
CROSS:                               'CROSS';
CURRENT:                             'CURRENT';
CURRENT_ROLE:                        'CURRENT_ROLE';
CURRENT_USER:                        'CURRENT_USER';
CURSOR:                              'CURSOR';
DATABASE:                            'DATABASE';
DATABASES:                           'DATABASES';
DECLARE:                             'DECLARE';
DEFAULT:                             'DEFAULT';
DELAYED:                             'DELAYED';
DELETE:                              'DELETE';
DESC:                                'DESC';
DESCRIBE:                            'DESCRIBE';
DETERMINISTIC:                       'DETERMINISTIC';
DIAGNOSTICS:                         'DIAGNOSTICS';
DISTINCT:                            'DISTINCT';
DISTINCTROW:                         'DISTINCTROW';
DROP:                                'DROP';
EACH:                                'EACH';
ELSE:                                'ELSE';
ELSEIF:                              'ELSEIF';
EMPTY:                               'EMPTY';
ENCLOSED:                            'ENCLOSED';
ESCAPED:                             'ESCAPED';
EXCEPT:                              'EXCEPT';
EXISTS:                              'EXISTS';
EXIT:                                'EXIT';
EXPLAIN:                             'EXPLAIN';
FALSE:                               'FALSE';
FETCH:                               'FETCH';
FOR:                                 'FOR';
FORCE:                               'FORCE';
FOREIGN:                             'FOREIGN';
FROM:                                'FROM';
FULLTEXT:                            'FULLTEXT';
GENERATED:                           'GENERATED';
GET:                                 'GET';
GRANT:                               'GRANT';
GROUP:                               'GROUP';
HAVING:                              'HAVING';
HIGH_PRIORITY:                       'HIGH_PRIORITY';
HISTOGRAM:                           'HISTOGRAM';
IF:                                  'IF';
IGNORE:                              'IGNORE';
IGNORED:                             'IGNORED';
IN:                                  'IN';
INDEX:                               'INDEX';
INFILE:                              'INFILE';
INNER:                               'INNER';
INOUT:                               'INOUT';
INSERT:                              'INSERT';
INTERVAL:                            'INTERVAL';
INTO:                                'INTO';
IS:                                  'IS';
ITERATE:                             'ITERATE';
JOIN:                                'JOIN';
KEY:                                 'KEY';
KEYS:                                'KEYS';
KILL:                                'KILL';
LATERAL:                             'LATERAL';
LEADING:                             'LEADING';
LEAVE:                               'LEAVE';
LEFT:                                'LEFT';
LIKE:                                'LIKE';
LIMIT:                               'LIMIT';
LINEAR:                              'LINEAR';
LINES:                               'LINES';
LOAD:                                'LOAD';
LOCK:                                'LOCK';
LOCKED:                              'LOCKED';
LOOP:                                'LOOP';
LOW_PRIORITY:                        'LOW_PRIORITY';
MASTER_BIND:                         'MASTER_BIND';
MASTER_SSL_VERIFY_SERVER_CERT:       'MASTER_SSL_VERIFY_SERVER_CERT';
MATCH:                               'MATCH';
MAXVALUE:                            'MAXVALUE';
MINVALUE:                            'MINVALUE';
MODIFIES:                            'MODIFIES';
NATURAL:                             'NATURAL';
NOT:                                 'NOT';
NO_WRITE_TO_BINLOG:                  'NO_WRITE_TO_BINLOG';
NULL_LITERAL:                        'NULL';
NUMBER:                              'NUMBER';
ON:                                  'ON';
OPTIMIZE:                            'OPTIMIZE';
OPTION:                              'OPTION';
OPTIONAL:                            'OPTIONAL';
OPTIONALLY:                          'OPTIONALLY';
OR:                                  'OR';
ORDER:                               'ORDER';
OUT:                                 'OUT';
OUTER:                               'OUTER';
OUTFILE:                             'OUTFILE';
OVER:                                'OVER';
PARTITION:                           'PARTITION';
PRIMARY:                             'PRIMARY';
PROCEDURE:                           'PROCEDURE';
PURGE:                               'PURGE';
RANGE:                               'RANGE';
READ:                                'READ';
READS:                               'READS';
REFERENCES:                          'REFERENCES';
REGEXP:                              'REGEXP';
RELEASE:                             'RELEASE';
RENAME:                              'RENAME';
REPEAT:                              'REPEAT';
REPLACE:                             'REPLACE';
REQUIRE:                             'REQUIRE';
RESIGNAL:                            'RESIGNAL';
RESTRICT:                            'RESTRICT';
RETAIN:                              'RETAIN';
RETURN:                              'RETURN';
REVOKE:                              'REVOKE';
RIGHT:                               'RIGHT';
RLIKE:                               'RLIKE';
SCHEMA:                              'SCHEMA';
SCHEMAS:                             'SCHEMAS';
SELECT:                              'SELECT';
SET:                                 'SET';
SEPARATOR:                           'SEPARATOR';
SHOW:                                'SHOW';
SIGNAL:                              'SIGNAL';
SKIP_:                               'SKIP';
SPATIAL:                             'SPATIAL';
SQL:                                 'SQL';
SQLEXCEPTION:                        'SQLEXCEPTION';
SQLSTATE:                            'SQLSTATE';
SQLWARNING:                          'SQLWARNING';
SQL_BIG_RESULT:                      'SQL_BIG_RESULT';
SQL_CALC_FOUND_ROWS:                 'SQL_CALC_FOUND_ROWS';
SQL_SMALL_RESULT:                    'SQL_SMALL_RESULT';
SSL:                                 'SSL';
STACKED:                             'STACKED';
STARTING:                            'STARTING';
STATEMENT:                           'STATEMENT';
STRAIGHT_JOIN:                       'STRAIGHT_JOIN';
TABLE:                               'TABLE';
TERMINATED:                          'TERMINATED';
THEN:                                'THEN';
TO:                                  'TO';
TRAILING:                            'TRAILING';
TRIGGER:                             'TRIGGER';
TRUE:                                'TRUE';
UNDO:                                'UNDO';
UNION:                               'UNION';
UNIQUE:                              'UNIQUE';
UNLOCK:                              'UNLOCK';
UNSIGNED:                            'UNSIGNED';
UPDATE:                              'UPDATE';
USAGE:                               'USAGE';
USE:                                 'USE';
USING:                               'USING';
VALUES:                              'VALUES';
WHEN:                                'WHEN';
WHERE:                               'WHERE';
WHILE:                               'WHILE';
WITH:                                'WITH';
WRITE:                               'WRITE';
XOR:                                 'XOR';
ZEROFILL:                            'ZEROFILL';

// DATA TYPE Keywords

TINYINT:                             'TINYINT';
SMALLINT:                            'SMALLINT';
MEDIUMINT:                           'MEDIUMINT';
MIDDLEINT:                           'MIDDLEINT';
INT:                                 'INT';
INT1:                                'INT1';
INT2:                                'INT2';
INT3:                                'INT3';
INT4:                                'INT4';
INT8:                                'INT8';
INTEGER:                             'INTEGER';
BIGINT:                              'BIGINT';
REAL:                                'REAL';
DOUBLE:                              'DOUBLE';
PRECISION:                           'PRECISION';
FLOAT:                               'FLOAT';
FLOAT4:                              'FLOAT4';
FLOAT8:                              'FLOAT8';
DECIMAL:                             'DECIMAL';
DEC:                                 'DEC';
NUMERIC:                             'NUMERIC';
DATE:                                'DATE';
TIME:                                'TIME';
TIMESTAMP:                           'TIMESTAMP';
DATETIME:                            'DATETIME';
YEAR:                                'YEAR';
CHAR:                                'CHAR';
VARCHAR:                             'VARCHAR';
NVARCHAR:                            'NVARCHAR';
NATIONAL:                            'NATIONAL';
BINARY:                              'BINARY';
VARBINARY:                           'VARBINARY';
TINYBLOB:                            'TINYBLOB';
BLOB:                                'BLOB';
MEDIUMBLOB:                          'MEDIUMBLOB';
LONG:                                'LONG';
LONGBLOB:                            'LONGBLOB';
TINYTEXT:                            'TINYTEXT';
TEXT:                                'TEXT';
MEDIUMTEXT:                          'MEDIUMTEXT';
LONGTEXT:                            'LONGTEXT';
ENUM:                                'ENUM';
VARYING:                             'VARYING';
SERIAL:                              'SERIAL';


// Interval type Keywords

YEAR_MONTH:                          'YEAR_MONTH';
DAY_HOUR:                            'DAY_HOUR';
DAY_MINUTE:                          'DAY_MINUTE';
DAY_SECOND:                          'DAY_SECOND';
HOUR_MINUTE:                         'HOUR_MINUTE';
HOUR_SECOND:                         'HOUR_SECOND';
MINUTE_SECOND:                       'MINUTE_SECOND';
SECOND_MICROSECOND:                  'SECOND_MICROSECOND';
MINUTE_MICROSECOND:                  'MINUTE_MICROSECOND';
HOUR_MICROSECOND:                    'HOUR_MICROSECOND';
DAY_MICROSECOND:                     'DAY_MICROSECOND';

// JSON keywords
JSON_ARRAY:                          'JSON_ARRAY';
JSON_ARRAYAGG:                       'JSON_ARRAYAGG';
JSON_ARRAY_APPEND:                   'JSON_ARRAY_APPEND';
JSON_ARRAY_INSERT:                   'JSON_ARRAY_INSERT';
JSON_CONTAINS:                       'JSON_CONTAINS';
JSON_CONTAINS_PATH:                  'JSON_CONTAINS_PATH';
JSON_DEPTH:                          'JSON_DEPTH';
JSON_EXTRACT:                        'JSON_EXTRACT';
JSON_INSERT:                         'JSON_INSERT';
JSON_KEYS:                           'JSON_KEYS';
JSON_LENGTH:                         'JSON_LENGTH';
JSON_MERGE:                          'JSON_MERGE';
JSON_MERGE_PATCH:                    'JSON_MERGE_PATCH';
JSON_MERGE_PRESERVE:                 'JSON_MERGE_PRESERVE';
JSON_OBJECT:                         'JSON_OBJECT';
JSON_OBJECTAGG:                      'JSON_OBJECTAGG';
JSON_OVERLAPS:                       'JSON_OVERLAPS';
JSON_PRETTY:                         'JSON_PRETTY';
JSON_QUOTE:                          'JSON_QUOTE';
JSON_REMOVE:                         'JSON_REMOVE';
JSON_REPLACE:                        'JSON_REPLACE';
JSON_SCHEMA_VALID:                   'JSON_SCHEMA_VALID';
JSON_SCHEMA_VALIDATION_REPORT:       'JSON_SCHEMA_VALIDATION_REPORT';
JSON_SEARCH:                         'JSON_SEARCH';
JSON_SET:                            'JSON_SET';
JSON_STORAGE_FREE:                   'JSON_STORAGE_FREE';
JSON_STORAGE_SIZE:                   'JSON_STORAGE_SIZE';
JSON_TABLE:                          'JSON_TABLE';
JSON_TYPE:                           'JSON_TYPE';
JSON_UNQUOTE:                        'JSON_UNQUOTE';
JSON_VALID:                          'JSON_VALID';
JSON_VALUE:                          'JSON_VALUE';
NESTED:                              'NESTED';
ORDINALITY:                          'ORDINALITY';
PATH:                                'PATH';

// Group function Keywords

AVG:                                 'AVG';
BIT_AND:                             'BIT_AND';
BIT_OR:                              'BIT_OR';
BIT_XOR:                             'BIT_XOR';
COUNT:                               'COUNT';
CUME_DIST:                           'CUME_DIST';
DENSE_RANK:                          'DENSE_RANK';
FIRST_VALUE:                         'FIRST_VALUE';
GROUP_CONCAT:                        'GROUP_CONCAT';
LAG:                                 'LAG';
LAST_VALUE:                          'LAST_VALUE';
LEAD:                                'LEAD';
MAX:                                 'MAX';
MIN:                                 'MIN';
NTILE:                               'NTILE';
NTH_VALUE:                           'NTH_VALUE';
PERCENT_RANK:                        'PERCENT_RANK';
RANK:                                'RANK';
ROW_NUMBER:                          'ROW_NUMBER';
STD:                                 'STD';
STDDEV:                              'STDDEV';
STDDEV_POP:                          'STDDEV_POP';
STDDEV_SAMP:                         'STDDEV_SAMP';
SUM:                                 'SUM';
VAR_POP:                             'VAR_POP';
VAR_SAMP:                            'VAR_SAMP';
VARIANCE:                            'VARIANCE';

// Common function Keywords

CURRENT_DATE:                        'CURRENT_DATE';
CURRENT_TIME:                        'CURRENT_TIME';
CURRENT_TIMESTAMP:                   'CURRENT_TIMESTAMP';
LOCALTIME:                           'LOCALTIME';
CURDATE:                             'CURDATE';
CURTIME:                             'CURTIME';
DATE_ADD:                            'DATE_ADD';
DATE_SUB:                            'DATE_SUB';
EXTRACT:                             'EXTRACT';
LOCALTIMESTAMP:                      'LOCALTIMESTAMP';
NOW:                                 'NOW';
POSITION:                            'POSITION';
SUBSTR:                              'SUBSTR';
SUBSTRING:                           'SUBSTRING';
SYSDATE:                             'SYSDATE';
TRIM:                                'TRIM';
UTC_DATE:                            'UTC_DATE';
UTC_TIME:                            'UTC_TIME';
UTC_TIMESTAMP:                       'UTC_TIMESTAMP';

// Keywords, but can be ID
// Common Keywords, but can be ID

ACCOUNT:                             'ACCOUNT';
ACTION:                              'ACTION';
AFTER:                               'AFTER';
AGGREGATE:                           'AGGREGATE';
ALGORITHM:                           'ALGORITHM';
ANY:                                 'ANY';
AT:                                  'AT';
AUTHORS:                             'AUTHORS';
AUTOCOMMIT:                          'AUTOCOMMIT';
AUTOEXTEND_SIZE:                     'AUTOEXTEND_SIZE';
AUTO_INCREMENT:                      'AUTO_INCREMENT';
AVG_ROW_LENGTH:                      'AVG_ROW_LENGTH';
BEGIN:                               'BEGIN';
BINLOG:                              'BINLOG';
BIT:                                 'BIT';
BLOCK:                               'BLOCK';
BOOL:                                'BOOL';
BOOLEAN:                             'BOOLEAN';
BTREE:                               'BTREE';
CACHE:                               'CACHE';
CASCADED:                            'CASCADED';
CHAIN:                               'CHAIN';
CHANGED:                             'CHANGED';
CHANNEL:                             'CHANNEL';
CHECKSUM:                            'CHECKSUM';
PAGE_CHECKSUM:                       'PAGE_CHECKSUM';
CIPHER:                              'CIPHER';
CLASS_ORIGIN:                        'CLASS_ORIGIN';
CLIENT:                              'CLIENT';
CLOSE:                               'CLOSE';
CLUSTERING:                          'CLUSTERING';
COALESCE:                            'COALESCE';
CODE:                                'CODE';
COLUMNS:                             'COLUMNS';
COLUMN_FORMAT:                       'COLUMN_FORMAT';
COLUMN_NAME:                         'COLUMN_NAME';
COMMENT:                             'COMMENT';
COMMIT:                              'COMMIT';
COMPACT:                             'COMPACT';
COMPLETION:                          'COMPLETION';
COMPRESSED:                          'COMPRESSED';
COMPRESSION:                         'COMPRESSION' | QUOTE_SYMB? 'COMPRESSION' QUOTE_SYMB?;
CONCURRENT:                          'CONCURRENT';
CONNECT:                             'CONNECT';
CONNECTION:                          'CONNECTION';
CONSISTENT:                          'CONSISTENT';
CONSTRAINT_CATALOG:                  'CONSTRAINT_CATALOG';
CONSTRAINT_SCHEMA:                   'CONSTRAINT_SCHEMA';
CONSTRAINT_NAME:                     'CONSTRAINT_NAME';
CONTAINS:                            'CONTAINS';
CONTEXT:                             'CONTEXT';
CONTRIBUTORS:                        'CONTRIBUTORS';
COPY:                                'COPY';
CPU:                                 'CPU';
CYCLE:                               'CYCLE';
CURSOR_NAME:                         'CURSOR_NAME';
DATA:                                'DATA';
DATAFILE:                            'DATAFILE';
DEALLOCATE:                          'DEALLOCATE';
DEFAULT_AUTH:                        'DEFAULT_AUTH';
DEFINER:                             'DEFINER';
DELAY_KEY_WRITE:                     'DELAY_KEY_WRITE';
DES_KEY_FILE:                        'DES_KEY_FILE';
DIRECTORY:                           'DIRECTORY';
DISABLE:                             'DISABLE';
DISCARD:                             'DISCARD';
DISK:                                'DISK';
DO:                                  'DO';
DUMPFILE:                            'DUMPFILE';
DUPLICATE:                           'DUPLICATE';
DYNAMIC:                             'DYNAMIC';
ENABLE:                              'ENABLE';
ENCRYPTED:                           'ENCRYPTED';
ENCRYPTION:                          'ENCRYPTION';
ENCRYPTION_KEY_ID:                   'ENCRYPTION_KEY_ID';
END:                                 'END';
ENDS:                                'ENDS';
ENGINE:                              'ENGINE';
ENGINES:                             'ENGINES';
ERROR:                               'ERROR';
ERRORS:                              'ERRORS';
ESCAPE:                              'ESCAPE';
EVEN:                                'EVEN';
EVENT:                               'EVENT';
EVENTS:                              'EVENTS';
EVERY:                               'EVERY';
EXCHANGE:                            'EXCHANGE';
EXCLUSIVE:                           'EXCLUSIVE';
EXPIRE:                              'EXPIRE';
EXPORT:                              'EXPORT';
EXTENDED:                            'EXTENDED';
EXTENT_SIZE:                         'EXTENT_SIZE';
FAILED_LOGIN_ATTEMPTS:               'FAILED_LOGIN_ATTEMPTS';
FAST:                                'FAST';
FAULTS:                              'FAULTS';
FIELDS:                              'FIELDS';
FILE_BLOCK_SIZE:                     'FILE_BLOCK_SIZE';
FILTER:                              'FILTER';
FIRST:                               'FIRST';
FIXED:                               'FIXED';
FLUSH:                               'FLUSH';
FOLLOWING:                           'FOLLOWING';
FOLLOWS:                             'FOLLOWS';
FOUND:                               'FOUND';
FULL:                                'FULL';
FUNCTION:                            'FUNCTION';
GENERAL:                             'GENERAL';
GLOBAL:                              'GLOBAL';
GRANTS:                              'GRANTS';
GROUP_REPLICATION:                   'GROUP_REPLICATION';
HANDLER:                             'HANDLER';
HASH:                                'HASH';
HELP:                                'HELP';
HISTORY:                             'HISTORY';
HOST:                                'HOST';
HOSTS:                               'HOSTS';
IDENTIFIED:                          'IDENTIFIED';
IGNORE_SERVER_IDS:                   'IGNORE_SERVER_IDS';
IMPORT:                              'IMPORT';
INCREMENT:                           'INCREMENT';
INDEXES:                             'INDEXES';
INITIAL_SIZE:                        'INITIAL_SIZE';
INPLACE:                             'INPLACE';
INSERT_METHOD:                       'INSERT_METHOD';
INSTALL:                             'INSTALL';
INSTANCE:                            'INSTANCE';
INSTANT:                             'INSTANT';
INVISIBLE:                           'INVISIBLE';
INVOKER:                             'INVOKER';
IO:                                  'IO';
IO_THREAD:                           'IO_THREAD';
IPC:                                 'IPC';
ISOLATION:                           'ISOLATION';
ISSUER:                              'ISSUER';
JSON:                                'JSON';
KEY_BLOCK_SIZE:                      'KEY_BLOCK_SIZE';
LANGUAGE:                            'LANGUAGE';
LAST:                                'LAST';
LEAVES:                              'LEAVES';
LESS:                                'LESS';
LEVEL:                               'LEVEL';
LIST:                                'LIST';
LOCAL:                               'LOCAL';
LOGFILE:                             'LOGFILE';
LOGS:                                'LOGS';
MASTER:                              'MASTER';
MASTER_AUTO_POSITION:                'MASTER_AUTO_POSITION';
MASTER_CONNECT_RETRY:                'MASTER_CONNECT_RETRY';
MASTER_DELAY:                        'MASTER_DELAY';
MASTER_HEARTBEAT_PERIOD:             'MASTER_HEARTBEAT_PERIOD';
MASTER_HOST:                         'MASTER_HOST';
MASTER_LOG_FILE:                     'MASTER_LOG_FILE';
MASTER_LOG_POS:                      'MASTER_LOG_POS';
MASTER_PASSWORD:                     'MASTER_PASSWORD';
MASTER_PORT:                         'MASTER_PORT';
MASTER_RETRY_COUNT:                  'MASTER_RETRY_COUNT';
MASTER_SSL:                          'MASTER_SSL';
MASTER_SSL_CA:                       'MASTER_SSL_CA';
MASTER_SSL_CAPATH:                   'MASTER_SSL_CAPATH';
MASTER_SSL_CERT:                     'MASTER_SSL_CERT';
MASTER_SSL_CIPHER:                   'MASTER_SSL_CIPHER';
MASTER_SSL_CRL:                      'MASTER_SSL_CRL';
MASTER_SSL_CRLPATH:                  'MASTER_SSL_CRLPATH';
MASTER_SSL_KEY:                      'MASTER_SSL_KEY';
MASTER_TLS_VERSION:                  'MASTER_TLS_VERSION';
MASTER_USER:                         'MASTER_USER';
MAX_CONNECTIONS_PER_HOUR:            'MAX_CONNECTIONS_PER_HOUR';
MAX_QUERIES_PER_HOUR:                'MAX_QUERIES_PER_HOUR';
MAX_ROWS:                            'MAX_ROWS';
MAX_SIZE:                            'MAX_SIZE';
MAX_UPDATES_PER_HOUR:                'MAX_UPDATES_PER_HOUR';
MAX_USER_CONNECTIONS:                'MAX_USER_CONNECTIONS';
MEDIUM:                              'MEDIUM';
MEMBER:                              'MEMBER';
MERGE:                               'MERGE';
MESSAGE_TEXT:                        'MESSAGE_TEXT';
MID:                                 'MID';
MIGRATE:                             'MIGRATE';
MIN_ROWS:                            'MIN_ROWS';
MODE:                                'MODE';
MODIFY:                              'MODIFY';
MUTEX:                               'MUTEX';
MYSQL:                               'MYSQL';
MYSQL_ERRNO:                         'MYSQL_ERRNO';
NAME:                                'NAME';
NAMES:                               'NAMES';
NCHAR:                               'NCHAR';
NEVER:                               'NEVER';
NEXT:                                'NEXT';
NO:                                  'NO';
NOCACHE:                             'NOCACHE';
NOCOPY:                              'NOCOPY';
NOCYCLE:                             'NOCYCLE';
NOMAXVALUE:                          'NOMAXVALUE';
NOMINVALUE:                          'NOMINVALUE';
NOWAIT:                              'NOWAIT';
NODEGROUP:                           'NODEGROUP';
NONE:                                'NONE';
ODBC:                                'ODBC';
OFFLINE:                             'OFFLINE';
OFFSET:                              'OFFSET';
OF:                                  'OF';
OJ:                                  'OJ';
OLD_PASSWORD:                        'OLD_PASSWORD';
ONE:                                 'ONE';
ONLINE:                              'ONLINE';
ONLY:                                'ONLY';
OPEN:                                'OPEN';
OPTIMIZER_COSTS:                     'OPTIMIZER_COSTS';
OPTIONS:                             'OPTIONS';
OWNER:                               'OWNER';
PACK_KEYS:                           'PACK_KEYS';
PAGE:                                'PAGE';
PAGE_COMPRESSED:                     'PAGE_COMPRESSED';
PAGE_COMPRESSION_LEVEL:              'PAGE_COMPRESSION_LEVEL';
PARSER:                              'PARSER';
PARTIAL:                             'PARTIAL';
PARTITIONING:                        'PARTITIONING';
PARTITIONS:                          'PARTITIONS';
PASSWORD:                            'PASSWORD';
PASSWORD_LOCK_TIME:                  'PASSWORD_LOCK_TIME';
PHASE:                               'PHASE';
PLUGIN:                              'PLUGIN';
PLUGIN_DIR:                          'PLUGIN_DIR';
PLUGINS:                             'PLUGINS';
PORT:                                'PORT';
PRECEDES:                            'PRECEDES';
PRECEDING:                           'PRECEDING';
PREPARE:                             'PREPARE';
PRESERVE:                            'PRESERVE';
PREV:                                'PREV';
PROCESSLIST:                         'PROCESSLIST';
PROFILE:                             'PROFILE';
PROFILES:                            'PROFILES';
PROXY:                               'PROXY';
QUERY:                               'QUERY';
QUICK:                               'QUICK';
REBUILD:                             'REBUILD';
RECOVER:                             'RECOVER';
RECURSIVE:                           'RECURSIVE';
REDO_BUFFER_SIZE:                    'REDO_BUFFER_SIZE';
REDUNDANT:                           'REDUNDANT';
RELAY:                               'RELAY';
RELAY_LOG_FILE:                      'RELAY_LOG_FILE';
RELAY_LOG_POS:                       'RELAY_LOG_POS';
RELAYLOG:                            'RELAYLOG';
REMOVE:                              'REMOVE';
REORGANIZE:                          'REORGANIZE';
REPAIR:                              'REPAIR';
REPLICATE_DO_DB:                     'REPLICATE_DO_DB';
REPLICATE_DO_TABLE:                  'REPLICATE_DO_TABLE';
REPLICATE_IGNORE_DB:                 'REPLICATE_IGNORE_DB';
REPLICATE_IGNORE_TABLE:              'REPLICATE_IGNORE_TABLE';
REPLICATE_REWRITE_DB:                'REPLICATE_REWRITE_DB';
REPLICATE_WILD_DO_TABLE:             'REPLICATE_WILD_DO_TABLE';
REPLICATE_WILD_IGNORE_TABLE:         'REPLICATE_WILD_IGNORE_TABLE';
REPLICATION:                         'REPLICATION';
RESET:                               'RESET';
RESTART:                             'RESTART';
RESUME:                              'RESUME';
RETURNED_SQLSTATE:                   'RETURNED_SQLSTATE';
RETURNING:                           'RETURNING';
RETURNS:                             'RETURNS';
REUSE:                               'REUSE';
ROLE:                                'ROLE';
ROLLBACK:                            'ROLLBACK';
ROLLUP:                              'ROLLUP';
ROTATE:                              'ROTATE';
ROW:                                 'ROW';
ROWS:                                'ROWS';
ROW_FORMAT:                          'ROW_FORMAT';
RTREE:                               'RTREE';
SAVEPOINT:                           'SAVEPOINT';
SCHEDULE:                            'SCHEDULE';
SECURITY:                            'SECURITY';
SEQUENCE:                            'SEQUENCE';
SERVER:                              'SERVER';
SESSION:                             'SESSION';
SHARE:                               'SHARE';
SHARED:                              'SHARED';
SIGNED:                              'SIGNED';
SIMPLE:                              'SIMPLE';
SLAVE:                               'SLAVE';
SLOW:                                'SLOW';
SNAPSHOT:                            'SNAPSHOT';
SOCKET:                              'SOCKET';
SOME:                                'SOME';
SONAME:                              'SONAME';
SOUNDS:                              'SOUNDS';
SOURCE:                              'SOURCE';
SQL_AFTER_GTIDS:                     'SQL_AFTER_GTIDS';
SQL_AFTER_MTS_GAPS:                  'SQL_AFTER_MTS_GAPS';
SQL_BEFORE_GTIDS:                    'SQL_BEFORE_GTIDS';
SQL_BUFFER_RESULT:                   'SQL_BUFFER_RESULT';
SQL_CACHE:                           'SQL_CACHE';
SQL_NO_CACHE:                        'SQL_NO_CACHE';
SQL_THREAD:                          'SQL_THREAD';
START:                               'START';
STARTS:                              'STARTS';
STATS_AUTO_RECALC:                   'STATS_AUTO_RECALC';
STATS_PERSISTENT:                    'STATS_PERSISTENT';
STATS_SAMPLE_PAGES:                  'STATS_SAMPLE_PAGES';
STATUS:                              'STATUS';
STOP:                                'STOP';
STORAGE:                             'STORAGE';
STORED:                              'STORED';
STRING:                              'STRING';
SUBCLASS_ORIGIN:                     'SUBCLASS_ORIGIN';
SUBJECT:                             'SUBJECT';
SUBPARTITION:                        'SUBPARTITION';
SUBPARTITIONS:                       'SUBPARTITIONS';
SUSPEND:                             'SUSPEND';
SWAPS:                               'SWAPS';
SWITCHES:                            'SWITCHES';
TABLE_NAME:                          'TABLE_NAME';
TABLESPACE:                          'TABLESPACE';
TABLE_TYPE:                          'TABLE_TYPE';
TEMPORARY:                           'TEMPORARY';
TEMPTABLE:                           'TEMPTABLE';
THAN:                                'THAN';
TRADITIONAL:                         'TRADITIONAL';
TRANSACTION:                         'TRANSACTION';
TRANSACTIONAL:                       'TRANSACTIONAL';
TRIGGERS:                            'TRIGGERS';
TRUNCATE:                            'TRUNCATE';
UNBOUNDED:                           'UNBOUNDED';
UNDEFINED:                           'UNDEFINED';
UNDOFILE:                            'UNDOFILE';
UNDO_BUFFER_SIZE:                    'UNDO_BUFFER_SIZE';
UNINSTALL:                           'UNINSTALL';
UNKNOWN:                             'UNKNOWN';
UNTIL:                               'UNTIL';
UPGRADE:                             'UPGRADE';
USER:                                'USER';
USE_FRM:                             'USE_FRM';
USER_RESOURCES:                      'USER_RESOURCES';
VALIDATION:                          'VALIDATION';
VALUE:                               'VALUE';
VARIABLES:                           'VARIABLES';
VIEW:                                'VIEW';
VIRTUAL:                             'VIRTUAL';
VISIBLE:                             'VISIBLE';
WAIT:                                'WAIT';
WARNINGS:                            'WARNINGS';
WINDOW:                              'WINDOW';
WITHOUT:                             'WITHOUT';
WORK:                                'WORK';
WRAPPER:                             'WRAPPER';
X509:                                'X509';
XA:                                  'XA';
XML:                                 'XML';
YES:                                 'YES';

// Date format Keywords

EUR:                                 'EUR';
USA:                                 'USA';
JIS:                                 'JIS';
ISO:                                 'ISO';
INTERNAL:                            'INTERNAL';


// Interval type Keywords

QUARTER:                             'QUARTER';
MONTH:                               'MONTH';
DAY:                                 'DAY';
HOUR:                                'HOUR';
MINUTE:                              'MINUTE';
WEEK:                                'WEEK';
SECOND:                              'SECOND';
MICROSECOND:                         'MICROSECOND';


// PRIVILEGES

ADMIN:                               'ADMIN';
APPLICATION_PASSWORD_ADMIN:          'APPLICATION_PASSWORD_ADMIN';
AUDIT_ADMIN:                         'AUDIT_ADMIN';
BACKUP_ADMIN:                        'BACKUP_ADMIN';
BINLOG_ADMIN:                        'BINLOG_ADMIN';
BINLOG_ENCRYPTION_ADMIN:             'BINLOG_ENCRYPTION_ADMIN';
CLONE_ADMIN:                         'CLONE_ADMIN';
CONNECTION_ADMIN:                    'CONNECTION_ADMIN';
ENCRYPTION_KEY_ADMIN:                'ENCRYPTION_KEY_ADMIN';
EXECUTE:                             'EXECUTE';
FILE:                                'FILE';
FIREWALL_ADMIN:                      'FIREWALL_ADMIN';
FIREWALL_USER:                       'FIREWALL_USER';
FLUSH_OPTIMIZER_COSTS:               'FLUSH_OPTIMIZER_COSTS';
FLUSH_STATUS:                        'FLUSH_STATUS';
FLUSH_TABLES:                        'FLUSH_TABLES';
FLUSH_USER_RESOURCES:                'FLUSH_USER_RESOURCES';
GROUP_REPLICATION_ADMIN:             'GROUP_REPLICATION_ADMIN';
INNODB_REDO_LOG_ARCHIVE:             'INNODB_REDO_LOG_ARCHIVE';
INNODB_REDO_LOG_ENABLE:              'INNODB_REDO_LOG_ENABLE';
INVOKE:                              'INVOKE';
LAMBDA:                              'LAMBDA';
NDB_STORED_USER:                     'NDB_STORED_USER';
PASSWORDLESS_USER_ADMIN:             'PASSWORDLESS_USER_ADMIN';
PERSIST_RO_VARIABLES_ADMIN:          'PERSIST_RO_VARIABLES_ADMIN';
PRIVILEGES:                          'PRIVILEGES';
PROCESS:                             'PROCESS';
RELOAD:                              'RELOAD';
REPLICATION_APPLIER:                 'REPLICATION_APPLIER';
REPLICATION_SLAVE_ADMIN:             'REPLICATION_SLAVE_ADMIN';
RESOURCE_GROUP_ADMIN:                'RESOURCE_GROUP_ADMIN';
RESOURCE_GROUP_USER:                 'RESOURCE_GROUP_USER';
ROLE_ADMIN:                          'ROLE_ADMIN';
ROUTINE:                             'ROUTINE';
S3:                                  'S3';
SERVICE_CONNECTION_ADMIN:            'SERVICE_CONNECTION_ADMIN';
SESSION_VARIABLES_ADMIN:             QUOTE_SYMB? 'SESSION_VARIABLES_ADMIN' QUOTE_SYMB?;
SET_USER_ID:                         'SET_USER_ID';
SHOW_ROUTINE:                        'SHOW_ROUTINE';
SHUTDOWN:                            'SHUTDOWN';
SUPER:                               'SUPER';
SYSTEM_VARIABLES_ADMIN:              'SYSTEM_VARIABLES_ADMIN';
TABLES:                              'TABLES';
TABLE_ENCRYPTION_ADMIN:              'TABLE_ENCRYPTION_ADMIN';
VERSION_TOKEN_ADMIN:                 'VERSION_TOKEN_ADMIN';
XA_RECOVER_ADMIN:                    'XA_RECOVER_ADMIN';


// Charsets

ARMSCII8:                            'ARMSCII8';
ASCII:                               'ASCII';
BIG5:                                'BIG5';
CP1250:                              'CP1250';
CP1251:                              'CP1251';
CP1256:                              'CP1256';
CP1257:                              'CP1257';
CP850:                               'CP850';
CP852:                               'CP852';
CP866:                               'CP866';
CP932:                               'CP932';
DEC8:                                'DEC8';
EUCJPMS:                             'EUCJPMS';
EUCKR:                               'EUCKR';
GB18030:                             'GB18030';
GB2312:                              'GB2312';
GBK:                                 'GBK';
GEOSTD8:                             'GEOSTD8';
GREEK:                               'GREEK';
HEBREW:                              'HEBREW';
HP8:                                 'HP8';
KEYBCS2:                             'KEYBCS2';
KOI8R:                               'KOI8R';
KOI8U:                               'KOI8U';
LATIN1:                              'LATIN1';
LATIN2:                              'LATIN2';
LATIN5:                              'LATIN5';
LATIN7:                              'LATIN7';
MACCE:                               'MACCE';
MACROMAN:                            'MACROMAN';
SJIS:                                'SJIS';
SWE7:                                'SWE7';
TIS620:                              'TIS620';
UCS2:                                'UCS2';
UJIS:                                'UJIS';
UTF16:                               'UTF16';
UTF16LE:                             'UTF16LE';
UTF32:                               'UTF32';
UTF8:                                'UTF8';
UTF8MB3:                             'UTF8MB3';
UTF8MB4:                             'UTF8MB4';


// DB Engines

ARCHIVE:                             'ARCHIVE';
BLACKHOLE:                           'BLACKHOLE';
CSV:                                 'CSV';
FEDERATED:                           'FEDERATED';
INNODB:                              'INNODB';
MEMORY:                              'MEMORY';
MRG_MYISAM:                          'MRG_MYISAM';
MYISAM:                              'MYISAM';
NDB:                                 'NDB';
NDBCLUSTER:                          'NDBCLUSTER';
PERFORMANCE_SCHEMA:                  'PERFORMANCE_SCHEMA';
TOKUDB:                              'TOKUDB';


// Transaction Levels

REPEATABLE:                          'REPEATABLE';
COMMITTED:                           'COMMITTED';
UNCOMMITTED:                         'UNCOMMITTED';
SERIALIZABLE:                        'SERIALIZABLE';


// Spatial data types

GEOMETRYCOLLECTION:                  'GEOMETRYCOLLECTION';
GEOMCOLLECTION:                      'GEOMCOLLECTION';
GEOMETRY:                            'GEOMETRY';
LINESTRING:                          'LINESTRING';
MULTILINESTRING:                     'MULTILINESTRING';
MULTIPOINT:                          'MULTIPOINT';
MULTIPOLYGON:                        'MULTIPOLYGON';
POINT:                               'POINT';
POLYGON:                             'POLYGON';


// Common function names

ABS:                                 'ABS';
ACOS:                                'ACOS';
ADDDATE:                             'ADDDATE';
ADDTIME:                             'ADDTIME';
AES_DECRYPT:                         'AES_DECRYPT';
AES_ENCRYPT:                         'AES_ENCRYPT';
AREA:                                'AREA';
ASBINARY:                            'ASBINARY';
ASIN:                                'ASIN';
ASTEXT:                              'ASTEXT';
ASWKB:                               'ASWKB';
ASWKT:                               'ASWKT';
ASYMMETRIC_DECRYPT:                  'ASYMMETRIC_DECRYPT';
ASYMMETRIC_DERIVE:                   'ASYMMETRIC_DERIVE';
ASYMMETRIC_ENCRYPT:                  'ASYMMETRIC_ENCRYPT';
ASYMMETRIC_SIGN:                     'ASYMMETRIC_SIGN';
ASYMMETRIC_VERIFY:                   'ASYMMETRIC_VERIFY';
ATAN:                                'ATAN';
ATAN2:                               'ATAN2';
BENCHMARK:                           'BENCHMARK';
BIN:                                 'BIN';
BIT_COUNT:                           'BIT_COUNT';
BIT_LENGTH:                          'BIT_LENGTH';
BUFFER:                              'BUFFER';
CATALOG_NAME:                        'CATALOG_NAME';
CEIL:                                'CEIL';
CEILING:                             'CEILING';
CENTROID:                            'CENTROID';
CHARACTER_LENGTH:                    'CHARACTER_LENGTH';
CHARSET:                             'CHARSET';
CHAR_LENGTH:                         'CHAR_LENGTH';
COERCIBILITY:                        'COERCIBILITY';
COLLATION:                           'COLLATION';
COMPRESS:                            'COMPRESS';
CONCAT:                              'CONCAT';
CONCAT_WS:                           'CONCAT_WS';
CONNECTION_ID:                       'CONNECTION_ID';
CONV:                                'CONV';
CONVERT_TZ:                          'CONVERT_TZ';
COS:                                 'COS';
COT:                                 'COT';
CRC32:                               'CRC32';
CREATE_ASYMMETRIC_PRIV_KEY:          'CREATE_ASYMMETRIC_PRIV_KEY';
CREATE_ASYMMETRIC_PUB_KEY:           'CREATE_ASYMMETRIC_PUB_KEY';
CREATE_DH_PARAMETERS:                'CREATE_DH_PARAMETERS';
CREATE_DIGEST:                       'CREATE_DIGEST';
CROSSES:                             'CROSSES';
DATEDIFF:                            'DATEDIFF';
DATE_FORMAT:                         'DATE_FORMAT';
DAYNAME:                             'DAYNAME';
DAYOFMONTH:                          'DAYOFMONTH';
DAYOFWEEK:                           'DAYOFWEEK';
DAYOFYEAR:                           'DAYOFYEAR';
DECODE:                              'DECODE';
DEGREES:                             'DEGREES';
DES_DECRYPT:                         'DES_DECRYPT';
DES_ENCRYPT:                         'DES_ENCRYPT';
DIMENSION:                           'DIMENSION';
DISJOINT:                            'DISJOINT';
ELT:                                 'ELT';
ENCODE:                              'ENCODE';
ENCRYPT:                             'ENCRYPT';
ENDPOINT:                            'ENDPOINT';
ENGINE_ATTRIBUTE:                    'ENGINE_ATTRIBUTE';
ENVELOPE:                            'ENVELOPE';
EQUALS:                              'EQUALS';
EXP:                                 'EXP';
EXPORT_SET:                          'EXPORT_SET';
EXTERIORRING:                        'EXTERIORRING';
EXTRACTVALUE:                        'EXTRACTVALUE';
FIELD:                               'FIELD';
FIND_IN_SET:                         'FIND_IN_SET';
FLOOR:                               'FLOOR';
FORMAT:                              'FORMAT';
FOUND_ROWS:                          'FOUND_ROWS';
FROM_BASE64:                         'FROM_BASE64';
FROM_DAYS:                           'FROM_DAYS';
FROM_UNIXTIME:                       'FROM_UNIXTIME';
GEOMCOLLFROMTEXT:                    'GEOMCOLLFROMTEXT';
GEOMCOLLFROMWKB:                     'GEOMCOLLFROMWKB';
GEOMETRYCOLLECTIONFROMTEXT:          'GEOMETRYCOLLECTIONFROMTEXT';
GEOMETRYCOLLECTIONFROMWKB:           'GEOMETRYCOLLECTIONFROMWKB';
GEOMETRYFROMTEXT:                    'GEOMETRYFROMTEXT';
GEOMETRYFROMWKB:                     'GEOMETRYFROMWKB';
GEOMETRYN:                           'GEOMETRYN';
GEOMETRYTYPE:                        'GEOMETRYTYPE';
GEOMFROMTEXT:                        'GEOMFROMTEXT';
GEOMFROMWKB:                         'GEOMFROMWKB';
GET_FORMAT:                          'GET_FORMAT';
GET_LOCK:                            'GET_LOCK';
GLENGTH:                             'GLENGTH';
GREATEST:                            'GREATEST';
GTID_SUBSET:                         'GTID_SUBSET';
GTID_SUBTRACT:                       'GTID_SUBTRACT';
HEX:                                 'HEX';
IFNULL:                              'IFNULL';
INET6_ATON:                          'INET6_ATON';
INET6_NTOA:                          'INET6_NTOA';
INET_ATON:                           'INET_ATON';
INET_NTOA:                           'INET_NTOA';
INSTR:                               'INSTR';
INTERIORRINGN:                       'INTERIORRINGN';
INTERSECTS:                          'INTERSECTS';
ISCLOSED:                            'ISCLOSED';
ISEMPTY:                             'ISEMPTY';
ISNULL:                              'ISNULL';
ISSIMPLE:                            'ISSIMPLE';
IS_FREE_LOCK:                        'IS_FREE_LOCK';
IS_IPV4:                             'IS_IPV4';
IS_IPV4_COMPAT:                      'IS_IPV4_COMPAT';
IS_IPV4_MAPPED:                      'IS_IPV4_MAPPED';
IS_IPV6:                             'IS_IPV6';
IS_USED_LOCK:                        'IS_USED_LOCK';
LAST_INSERT_ID:                      'LAST_INSERT_ID';
LCASE:                               'LCASE';
LEAST:                               'LEAST';
LENGTH:                              'LENGTH';
LINEFROMTEXT:                        'LINEFROMTEXT';
LINEFROMWKB:                         'LINEFROMWKB';
LINESTRINGFROMTEXT:                  'LINESTRINGFROMTEXT';
LINESTRINGFROMWKB:                   'LINESTRINGFROMWKB';
LN:                                  'LN';
LOAD_FILE:                           'LOAD_FILE';
LOCATE:                              'LOCATE';
LOG:                                 'LOG';
LOG10:                               'LOG10';
LOG2:                                'LOG2';
LOWER:                               'LOWER';
LPAD:                                'LPAD';
LTRIM:                               'LTRIM';
MAKEDATE:                            'MAKEDATE';
MAKETIME:                            'MAKETIME';
MAKE_SET:                            'MAKE_SET';
MASTER_POS_WAIT:                     'MASTER_POS_WAIT';
MBRCONTAINS:                         'MBRCONTAINS';
MBRDISJOINT:                         'MBRDISJOINT';
MBREQUAL:                            'MBREQUAL';
MBRINTERSECTS:                       'MBRINTERSECTS';
MBROVERLAPS:                         'MBROVERLAPS';
MBRTOUCHES:                          'MBRTOUCHES';
MBRWITHIN:                           'MBRWITHIN';
MD5:                                 'MD5';
MLINEFROMTEXT:                       'MLINEFROMTEXT';
MLINEFROMWKB:                        'MLINEFROMWKB';
MONTHNAME:                           'MONTHNAME';
MPOINTFROMTEXT:                      'MPOINTFROMTEXT';
MPOINTFROMWKB:                       'MPOINTFROMWKB';
MPOLYFROMTEXT:                       'MPOLYFROMTEXT';
MPOLYFROMWKB:                        'MPOLYFROMWKB';
MULTILINESTRINGFROMTEXT:             'MULTILINESTRINGFROMTEXT';
MULTILINESTRINGFROMWKB:              'MULTILINESTRINGFROMWKB';
MULTIPOINTFROMTEXT:                  'MULTIPOINTFROMTEXT';
MULTIPOINTFROMWKB:                   'MULTIPOINTFROMWKB';
MULTIPOLYGONFROMTEXT:                'MULTIPOLYGONFROMTEXT';
MULTIPOLYGONFROMWKB:                 'MULTIPOLYGONFROMWKB';
NAME_CONST:                          'NAME_CONST';
NULLIF:                              'NULLIF';
NUMGEOMETRIES:                       'NUMGEOMETRIES';
NUMINTERIORRINGS:                    'NUMINTERIORRINGS';
NUMPOINTS:                           'NUMPOINTS';
OCT:                                 'OCT';
OCTET_LENGTH:                        'OCTET_LENGTH';
ORD:                                 'ORD';
OVERLAPS:                            'OVERLAPS';
PERIOD_ADD:                          'PERIOD_ADD';
PERIOD_DIFF:                         'PERIOD_DIFF';
PI:                                  'PI';
POINTFROMTEXT:                       'POINTFROMTEXT';
POINTFROMWKB:                        'POINTFROMWKB';
POINTN:                              'POINTN';
POLYFROMTEXT:                        'POLYFROMTEXT';
POLYFROMWKB:                         'POLYFROMWKB';
POLYGONFROMTEXT:                     'POLYGONFROMTEXT';
POLYGONFROMWKB:                      'POLYGONFROMWKB';
POW:                                 'POW';
POWER:                               'POWER';
QUOTE:                               'QUOTE';
RADIANS:                             'RADIANS';
RAND:                                'RAND';
RANDOM_BYTES:                        'RANDOM_BYTES';
RELEASE_LOCK:                        'RELEASE_LOCK';
REVERSE:                             'REVERSE';
ROUND:                               'ROUND';
ROW_COUNT:                           'ROW_COUNT';
RPAD:                                'RPAD';
RTRIM:                               'RTRIM';
SEC_TO_TIME:                         'SEC_TO_TIME';
SECONDARY_ENGINE_ATTRIBUTE:          'SECONDARY_ENGINE_ATTRIBUTE';
SESSION_USER:                        'SESSION_USER';
SHA:                                 'SHA';
SHA1:                                'SHA1';
SHA2:                                'SHA2';
SCHEMA_NAME:                         'SCHEMA_NAME';
SIGN:                                'SIGN';
SIN:                                 'SIN';
SLEEP:                               'SLEEP';
SOUNDEX:                             'SOUNDEX';
SQL_THREAD_WAIT_AFTER_GTIDS:         'SQL_THREAD_WAIT_AFTER_GTIDS';
SQRT:                                'SQRT';
SRID:                                'SRID';
STARTPOINT:                          'STARTPOINT';
STRCMP:                              'STRCMP';
STR_TO_DATE:                         'STR_TO_DATE';
ST_AREA:                             'ST_AREA';
ST_ASBINARY:                         'ST_ASBINARY';
ST_ASTEXT:                           'ST_ASTEXT';
ST_ASWKB:                            'ST_ASWKB';
ST_ASWKT:                            'ST_ASWKT';
ST_BUFFER:                           'ST_BUFFER';
ST_CENTROID:                         'ST_CENTROID';
ST_CONTAINS:                         'ST_CONTAINS';
ST_CROSSES:                          'ST_CROSSES';
ST_DIFFERENCE:                       'ST_DIFFERENCE';
ST_DIMENSION:                        'ST_DIMENSION';
ST_DISJOINT:                         'ST_DISJOINT';
ST_DISTANCE:                         'ST_DISTANCE';
ST_ENDPOINT:                         'ST_ENDPOINT';
ST_ENVELOPE:                         'ST_ENVELOPE';
ST_EQUALS:                           'ST_EQUALS';
ST_EXTERIORRING:                     'ST_EXTERIORRING';
ST_GEOMCOLLFROMTEXT:                 'ST_GEOMCOLLFROMTEXT';
ST_GEOMCOLLFROMTXT:                  'ST_GEOMCOLLFROMTXT';
ST_GEOMCOLLFROMWKB:                  'ST_GEOMCOLLFROMWKB';
ST_GEOMETRYCOLLECTIONFROMTEXT:       'ST_GEOMETRYCOLLECTIONFROMTEXT';
ST_GEOMETRYCOLLECTIONFROMWKB:        'ST_GEOMETRYCOLLECTIONFROMWKB';
ST_GEOMETRYFROMTEXT:                 'ST_GEOMETRYFROMTEXT';
ST_GEOMETRYFROMWKB:                  'ST_GEOMETRYFROMWKB';
ST_GEOMETRYN:                        'ST_GEOMETRYN';
ST_GEOMETRYTYPE:                     'ST_GEOMETRYTYPE';
ST_GEOMFROMTEXT:                     'ST_GEOMFROMTEXT';
ST_GEOMFROMWKB:                      'ST_GEOMFROMWKB';
ST_INTERIORRINGN:                    'ST_INTERIORRINGN';
ST_INTERSECTION:                     'ST_INTERSECTION';
ST_INTERSECTS:                       'ST_INTERSECTS';
ST_ISCLOSED:                         'ST_ISCLOSED';
ST_ISEMPTY:                          'ST_ISEMPTY';
ST_ISSIMPLE:                         'ST_ISSIMPLE';
ST_LINEFROMTEXT:                     'ST_LINEFROMTEXT';
ST_LINEFROMWKB:                      'ST_LINEFROMWKB';
ST_LINESTRINGFROMTEXT:               'ST_LINESTRINGFROMTEXT';
ST_LINESTRINGFROMWKB:                'ST_LINESTRINGFROMWKB';
ST_NUMGEOMETRIES:                    'ST_NUMGEOMETRIES';
ST_NUMINTERIORRING:                  'ST_NUMINTERIORRING';
ST_NUMINTERIORRINGS:                 'ST_NUMINTERIORRINGS';
ST_NUMPOINTS:                        'ST_NUMPOINTS';
ST_OVERLAPS:                         'ST_OVERLAPS';
ST_POINTFROMTEXT:                    'ST_POINTFROMTEXT';
ST_POINTFROMWKB:                     'ST_POINTFROMWKB';
ST_POINTN:                           'ST_POINTN';
ST_POLYFROMTEXT:                     'ST_POLYFROMTEXT';
ST_POLYFROMWKB:                      'ST_POLYFROMWKB';
ST_POLYGONFROMTEXT:                  'ST_POLYGONFROMTEXT';
ST_POLYGONFROMWKB:                   'ST_POLYGONFROMWKB';
ST_SRID:                             'ST_SRID';
ST_STARTPOINT:                       'ST_STARTPOINT';
ST_SYMDIFFERENCE:                    'ST_SYMDIFFERENCE';
ST_TOUCHES:                          'ST_TOUCHES';
ST_UNION:                            'ST_UNION';
ST_WITHIN:                           'ST_WITHIN';
ST_X:                                'ST_X';
ST_Y:                                'ST_Y';
SUBDATE:                             'SUBDATE';
SUBSTRING_INDEX:                     'SUBSTRING_INDEX';
SUBTIME:                             'SUBTIME';
SYSTEM_USER:                         'SYSTEM_USER';
TAN:                                 'TAN';
TIMEDIFF:                            'TIMEDIFF';
TIMESTAMPADD:                        'TIMESTAMPADD';
TIMESTAMPDIFF:                       'TIMESTAMPDIFF';
TIME_FORMAT:                         'TIME_FORMAT';
TIME_TO_SEC:                         'TIME_TO_SEC';
TOUCHES:                             'TOUCHES';
TO_BASE64:                           'TO_BASE64';
TO_DAYS:                             'TO_DAYS';
TO_SECONDS:                          'TO_SECONDS';
UCASE:                               'UCASE';
UNCOMPRESS:                          'UNCOMPRESS';
UNCOMPRESSED_LENGTH:                 'UNCOMPRESSED_LENGTH';
UNHEX:                               'UNHEX';
UNIX_TIMESTAMP:                      'UNIX_TIMESTAMP';
UPDATEXML:                           'UPDATEXML';
UPPER:                               'UPPER';
UUID:                                'UUID';
UUID_SHORT:                          'UUID_SHORT';
VALIDATE_PASSWORD_STRENGTH:          'VALIDATE_PASSWORD_STRENGTH';
VERSION:                             'VERSION';
WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS:   'WAIT_UNTIL_SQL_THREAD_AFTER_GTIDS';
WEEKDAY:                             'WEEKDAY';
WEEKOFYEAR:                          'WEEKOFYEAR';
WEIGHT_STRING:                       'WEIGHT_STRING';
WITHIN:                              'WITHIN';
YEARWEEK:                            'YEARWEEK';
Y_FUNCTION:                          'Y';
X_FUNCTION:                          'X';




// Operators
// Operators. Assigns

VAR_ASSIGN:                          ':=';
PLUS_ASSIGN:                         '+=';
MINUS_ASSIGN:                        '-=';
MULT_ASSIGN:                         '*=';
DIV_ASSIGN:                          '/=';
MOD_ASSIGN:                          '%=';
AND_ASSIGN:                          '&=';
XOR_ASSIGN:                          '^=';
OR_ASSIGN:                           '|=';


// Operators. Arithmetics

STAR:                                '*';
DIVIDE:                              '/';
MODULE:                              '%';
PLUS:                                '+';
MINUS:                               '-';
DIV:                                 'DIV';
MOD:                                 'MOD';


// Operators. Comparation

EQUAL_SYMBOL:                        '=';
GREATER_SYMBOL:                      '>';
LESS_SYMBOL:                         '<';
EXCLAMATION_SYMBOL:                  '!';


// Operators. Bit

BIT_NOT_OP:                          '~';
BIT_OR_OP:                           '|';
BIT_AND_OP:                          '&';
BIT_XOR_OP:                          '^';


// Constructors symbols

DOT:                                 '.';
LR_BRACKET:                          '(';
RR_BRACKET:                          ')';
COMMA:                               ',';
SEMI:                                ';';
AT_SIGN:                             '@';
ZERO_DECIMAL:                        '0';
ONE_DECIMAL:                         '1';
TWO_DECIMAL:                         '2';
SINGLE_QUOTE_SYMB:                   '\'';
DOUBLE_QUOTE_SYMB:                   '"';
REVERSE_QUOTE_SYMB:                  '`';
COLON_SYMB:                          ':';

fragment QUOTE_SYMB
    : SINGLE_QUOTE_SYMB | DOUBLE_QUOTE_SYMB | REVERSE_QUOTE_SYMB
    ;



// Charsets

CHARSET_REVERSE_QOUTE_STRING:        '`' CHARSET_NAME '`';



// File's sizes


FILESIZE_LITERAL:                    DEC_DIGIT+ ('K'|'M'|'G'|'T');



// Literal Primitives


START_NATIONAL_STRING_LITERAL:       'N' SQUOTA_STRING;
STRING_LITERAL:                      DQUOTA_STRING | SQUOTA_STRING | BQUOTA_STRING;
DECIMAL_LITERAL:                     DEC_DIGIT+;
HEXADECIMAL_LITERAL:                 'X' '\'' (HEX_DIGIT HEX_DIGIT)+ '\''
                                     | '0X' HEX_DIGIT+;

REAL_LITERAL:                        (DEC_DIGIT+)? '.' DEC_DIGIT+
                                     | DEC_DIGIT+ '.' EXPONENT_NUM_PART
                                     | (DEC_DIGIT+)? '.' (DEC_DIGIT+ EXPONENT_NUM_PART)
                                     | DEC_DIGIT+ EXPONENT_NUM_PART;
NULL_SPEC_LITERAL:                   '\\' 'N';
BIT_STRING:                          BIT_STRING_L;
STRING_CHARSET_NAME:                 '_' CHARSET_NAME;




// Hack for dotID
// Prevent recognize string:         .123somelatin AS ((.123), FLOAT_LITERAL), ((somelatin), ID)
//  it must recoginze:               .123somelatin AS ((.), DOT), (123somelatin, ID)

DOT_ID:                              '.' ID_LITERAL;



// Identifiers

ID:                                  ID_LITERAL;
// DOUBLE_QUOTE_ID:                  '"' ~'"'+ '"';
REVERSE_QUOTE_ID:                    '`' ~'`'+ '`';
STRING_USER_NAME:                    (
                                       SQUOTA_STRING | DQUOTA_STRING
                                       | BQUOTA_STRING | ID_LITERAL
                                     ) '@'
                                     (
                                       SQUOTA_STRING | DQUOTA_STRING
                                       | BQUOTA_STRING | ID_LITERAL
                                       | IP_ADDRESS
                                     );
IP_ADDRESS:                          (
                                       [0-9]+ '.' [0-9.]+
                                       | [0-9A-F:]+ ':' [0-9A-F:]+
                                     );
LOCAL_ID:                               '@'
                                     (
                                        [A-Z0-9._$]+
                                        | SQUOTA_STRING
                                        | DQUOTA_STRING
                                        | BQUOTA_STRING
                                    );
GLOBAL_ID:                              '@' '@'
                                    (
                                        [A-Z0-9._$]+
                                        | BQUOTA_STRING
                                    );


// Fragments for Literal primitives

fragment CHARSET_NAME:               ARMSCII8 | ASCII | BIG5 | BINARY | CP1250
                                     | CP1251 | CP1256 | CP1257 | CP850
                                     | CP852 | CP866 | CP932 | DEC8 | EUCJPMS
                                     | EUCKR | GB2312 | GBK | GEOSTD8 | GREEK
                                     | HEBREW | HP8 | KEYBCS2 | KOI8R | KOI8U
                                     | LATIN1 | LATIN2 | LATIN5 | LATIN7
                                     | MACCE | MACROMAN | SJIS | SWE7 | TIS620
                                     | UCS2 | UJIS | UTF16 | UTF16LE | UTF32
                                     | UTF8 | UTF8MB3 | UTF8MB4;

fragment EXPONENT_NUM_PART:          'E' [-+]? DEC_DIGIT+;
fragment ID_LITERAL:                 [A-Z_$0-9\u0080-\uFFFF]*?[A-Z_$\u0080-\uFFFF]+?[A-Z_$0-9\u0080-\uFFFF]*;
fragment DQUOTA_STRING:              '"' ( '\\'. | '""' | ~('"'| '\\') )* '"';
fragment SQUOTA_STRING:              '\'' ('\\'. | '\'\'' | ~('\'' | '\\'))* '\'';
fragment BQUOTA_STRING:              '`' ( '\\'. | '``' | ~('`'|'\\'))* '`';
fragment HEX_DIGIT:                  [0-9A-F];
fragment DEC_DIGIT:                  [0-9];
fragment BIT_STRING_L:               'B' '\'' [01]+ '\'';



// Last tokens must generate Errors

ERROR_RECONGNIGION:                  .    -> channel(ERRORCHANNEL);
