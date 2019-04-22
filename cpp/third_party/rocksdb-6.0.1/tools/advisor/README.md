# Rocksdb Tuning Advisor

## Motivation

The performance of Rocksdb is contingent on its tuning. However,
because of the complexity of its underlying technology and a large number of
configurable parameters, a good configuration is sometimes hard to obtain. The aim of
the python command-line tool, Rocksdb Advisor, is to automate the process of
suggesting improvements in the configuration based on advice from Rocksdb
experts.

## Overview

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

## Usage

### Prerequisites
The tool needs the following to run:
* python3

### Running the tool
An example command to run the tool:

```shell
cd rocksdb/tools/advisor
python3 -m advisor.rule_parser_example --rules_spec=advisor/rules.ini --rocksdb_options=test/input_files/OPTIONS-000005 --log_files_path_prefix=test/input_files/LOG-0 --stats_dump_period_sec=20
```

### Command-line arguments

Most important amongst all the input that the Advisor needs, are the rules
spec and starting Rocksdb configuration. The configuration is provided as the
familiar Rocksdb Options file (refer [example](https://github.com/facebook/rocksdb/blob/master/examples/rocksdb_option_file_example.ini)).
The Rules spec is written in the INI format (more details in
[rules.ini](https://github.com/facebook/rocksdb/blob/master/tools/advisor/advisor/rules.ini)).

In brief, a Rule is made of conditions and is triggered when all its
constituent conditions are triggered. When triggered, a Rule suggests changes
(increase/decrease/set to a suggested value) to certain Rocksdb options that
aim to improve Rocksdb performance. Every Condition has a 'source' i.e.
the data source that would be checked for triggering that condition.
For example, a log Condition (with 'source=LOG') is triggered if a particular
'regex' is found in the Rocksdb LOG files. As of now the Rules Engine
supports 3 types of Conditions (and consequently data-sources):
LOG, OPTIONS, TIME_SERIES. The TIME_SERIES data can be sourced from the
Rocksdb [statistics](https://github.com/facebook/rocksdb/blob/master/include/rocksdb/statistics.h)
or [perf context](https://github.com/facebook/rocksdb/blob/master/include/rocksdb/perf_context.h).

For more information about the remaining command-line arguments, run:

```shell
cd rocksdb/tools/advisor
python3 -m advisor.rule_parser_example --help
```

### Sample output

Here, a Rocksdb log-based rule has been triggered:

```shell
Rule: stall-too-many-memtables
LogCondition: stall-too-many-memtables regex: Stopping writes because we have \d+ immutable memtables \(waiting for flush\), max_write_buffer_number is set to \d+
Suggestion: inc-bg-flush option : DBOptions.max_background_flushes action : increase suggested_values : ['2']
Suggestion: inc-write-buffer option : CFOptions.max_write_buffer_number action : increase
scope: col_fam:
{'default'}
```

## Running the tests

Tests for the code have been added to the
[test/](https://github.com/facebook/rocksdb/tree/master/tools/advisor/test)
directory. For example, to run the unit tests for db_log_parser.py:

```shell
cd rocksdb/tools/advisor
python3 -m unittest -v test.test_db_log_parser
```
