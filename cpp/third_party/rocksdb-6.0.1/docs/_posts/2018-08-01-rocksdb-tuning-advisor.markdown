---
title: Rocksdb Tuning Advisor
layout: post
author: poojam23
category: blog
---

The performance of Rocksdb is contingent on its tuning. However, because
of the complexity of its underlying technology and a large number of
configurable parameters, a good configuration is sometimes hard to obtain. The aim of
the python command-line tool, Rocksdb Advisor, is to automate the process of
suggesting improvements in the configuration based on advice from Rocksdb
experts.

### Overview

Experts share their wisdom as rules comprising of conditions and suggestions in the INI format (refer
[rules.ini](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/rules.ini)).
Users provide the Rocksdb configuration that they want to improve upon (as the
familiar Rocksdb OPTIONS file —
[example](https://github.com/facebook/rocksdb/blob/master/examples/rocksdb_option_file_example.ini))
and the path of the file which contains Rocksdb logs and statistics.
The [Advisor](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/rule_parser_example.py)
creates appropriate DataSource objects (for Rocksdb
[logs](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/db_log_parser.py),
[options](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/db_options_parser.py),
[statistics](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/db_stats_fetcher.py) etc.)
and provides them to the [Rules Engine](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/rule_parser.py).
The Rules uses rules from experts to parse data-sources and trigger appropriate rules.
The Advisor's output gives information about which rules were triggered,
why they were triggered and what each of them suggests. Each suggestion
provided by a triggered rule advises some action on a Rocksdb
configuration option, for example, increase CFOptions.write_buffer_size,
set bloom_bits to 2 etc.

### Usage

An example command to run the tool:

```shell
cd rocksdb/tools/advisor
python3 -m advisor.rule_parser_example --rules_spec=advisor/rules.ini --rocksdb_options=test/input_files/OPTIONS-000005 --log_files_path_prefix=test/input_files/LOG-0 --stats_dump_period_sec=20
```

Sample output where a Rocksdb log-based rule has been triggered :

```shell
Rule: stall-too-many-memtables
LogCondition: stall-too-many-memtables regex: Stopping writes because we have \d+ immutable memtables \(waiting for flush\), max_write_buffer_number is set to \d+
Suggestion: inc-bg-flush option : DBOptions.max_background_flushes action : increase suggested_values : ['2']
Suggestion: inc-write-buffer option : CFOptions.max_write_buffer_number action : increase
scope: col_fam:
{'default'}
```

### Read more

For more information, refer to [advisor](https://github.com/facebook/rocksdb/tree/master/tools/advisor/README.md).
