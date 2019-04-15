# To do list

`sqlite_orm` is a wonderful library but there are still features that are not implemented. Here you can find a list of them:

* `FOREIGN KEY` - sync_schema fk comparison and ability of two tables to have fk to each other
* rest of core functions(https://sqlite.org/lang_corefunc.html)
* `CASE`
* `HAVING`
* `EXISTS`
* asterisk in raw select: `SELECT rowid, * FROM table`
* `ATTACH`
* blob incremental I/O https://sqlite.org/c3ref/blob_open.html
* reusing of prepared statements - useful for query optimisation
* subselect
* backup API https://www.sqlite.org/backup.html
* busy handler https://sqlite.org/c3ref/busy_handler.html
* CAST https://sqlite.org/lang_expr.html#castexpr
* CREATE VIEW and other view operations https://sqlite.org/lang_createview.html
* triggers

Please feel free to add any feature that isn't listed here and not implemented yet.
