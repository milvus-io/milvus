"""QueryScan Grep regressions through MilvusClient's public Query path.

Requires PyMilvus 3.1.0rc104 or later with QueryScan support and the grep DSL.
For a local SDK checkout, prepend its repository root to PYTHONPATH.
Tests verify public API results and invalid-parameter errors with Strong
consistency. No external corpus or special server configuration is required.
"""

from concurrent.futures import ThreadPoolExecutor

import pytest
from base.client_v2_base import TestMilvusClientV2Base
from common import common_func as cf
from common.common_type import CaseLabel
from pymilvus import DataType, FunctionChain, FunctionChainStage, MilvusException
from pymilvus.exceptions import ParamError


def _grep_chain(arguments="-F -e timeout", output="matches", export=True):
    return FunctionChain(
        FunctionChainStage.QUERY_SCAN,
        name="scan",
        output_columns=[output] if export else [],
    ).grep("text", arguments=arguments, output=output)


def _literal_details(text, pattern="timeout"):
    """Independent oracle with original line coordinates and UTF-8 byte spans."""
    if not pattern:
        raise ValueError("Empty patterns require explicit zero-width expectations")
    if not text:
        return []
    lines = text.split("\n")
    if text.endswith("\n"):
        lines.pop()
    details = []
    line_start_offset = 0
    for line_number, line in enumerate(lines, 1):
        content = line.encode("utf-8")
        needle = pattern.encode("utf-8")
        starts, ends = [], []
        position = content.find(needle)
        while position >= 0:
            starts.append(position)
            ends.append(position + len(needle))
            position = content.find(needle, ends[-1])
        if starts:
            details.append(
                {
                    "content": line,
                    "line_number": line_number,
                    "line_start_offset": line_start_offset,
                    "starts": starts,
                    "ends": ends,
                }
            )
        line_start_offset += len(content) + 1
    return details


class TestMilvusClientGrep(TestMilvusClientV2Base):
    output_fields = ["id", "text", "revision", "note", "metadata", "numbers", "vector"]

    def _create(self, client, text_type=DataType.VARCHAR, string_pk=False):
        name = cf.gen_unique_str("grep_query")
        schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
        pk_options = {"max_length": 64} if string_pk else {}
        schema.add_field("id", DataType.VARCHAR if string_pk else DataType.INT64, is_primary=True, **pk_options)
        text_options = {"max_length": 65535} if text_type == DataType.VARCHAR else {}
        schema.add_field("text", text_type, nullable=True, **text_options)
        schema.add_field("revision", DataType.INT64)
        schema.add_field("note", DataType.VARCHAR, max_length=128, nullable=True)
        schema.add_field("metadata", DataType.JSON)
        schema.add_field("numbers", DataType.ARRAY, element_type=DataType.INT64, max_capacity=4)
        schema.add_field("vector", DataType.FLOAT_VECTOR, dim=2)
        index = client.prepare_index_params()
        index.add_index(field_name="vector", index_type="FLAT", metric_type="L2")
        self.create_collection(client, name, schema=schema, index_params=index, consistency_level="Strong")
        return name

    @staticmethod
    def _row(pk, text, revision=0):
        return {
            "id": pk,
            "text": text,
            "revision": revision,
            "note": None if revision % 2 == 0 else f"note-{pk}-{revision}",
            "metadata": {"pk": pk, "revision": revision},
            "numbers": [revision, revision + 1],
            "vector": [float(revision), 1.0],
        }

    def _query(self, client, name, *, arguments="-F -e timeout", output="matches", export=True, **kwargs):
        result, _ = self.query(
            client,
            name,
            filter=kwargs.pop("filter", "revision >= 0"),
            output_fields=kwargs.pop("output_fields", self.output_fields),
            function_chains=[_grep_chain(arguments, output, export)],
            consistency_level="Strong",
            **kwargs,
        )
        return result

    def _assert_rows(self, actual, rows, *, pattern="timeout", offset=0, limit=None, output="matches", export=True):
        expected = []
        for row in sorted(rows, key=lambda row: row["id"]):
            details = _literal_details(row["text"], pattern)
            if details:
                value = dict(row)
                if export:
                    value[output] = details
                expected.append(value)
        expected = expected[offset : None if limit is None else offset + limit]
        # Compare full rows in returned order: sorting actual rows would hide
        # pagination bugs, and checking only IDs would miss late-read misalignment.
        assert list(actual) == expected

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("text_type", [DataType.VARCHAR, DataType.TEXT])
    @pytest.mark.parametrize("string_pk", [False, True])
    def test_grep_projection_and_paging(self, text_type, string_pk):
        """Check all projected fields and decoded JSON with Strong consistency."""
        client = self._client()
        name = self._create(client, text_type, string_pk)
        texts = {
            10: None,
            20: "",
            30: "unrelated",
            40: "中文\r\n\n超时 timeout timeout\nsecond timeout\r\n",
            50: "timeout fifty",
            60: "timeout sixty",
            70: "timeout seventy",
        }
        rows = [
            self._row(f"pk-{pk:03}" if string_pk else pk, texts[pk], i)
            for i, pk in enumerate([70, 20, 60, 10, 50, 30, 40])
        ]
        self.insert(client, name, rows)
        for limit, offset in [(1, 0), (2, 1), (2, 3), (2, 20)]:
            result = self._query(client, name, limit=limit, offset=offset)
            self._assert_rows(result, rows, limit=limit, offset=offset)
        for export in [False, True]:
            result = self._query(client, name, output="renamed_hits", export=export)
            self._assert_rows(result, rows, output="renamed_hits", export=export)
        result = self._query(client, name, filter="revision >= 4", limit=20)
        self._assert_rows(result, [row for row in rows if row["revision"] >= 4])
        assert self._query(client, name, filter="revision < 0", limit=20) == []
        assert self._query(client, name, arguments="-F -e absent", limit=20) == []

    @pytest.mark.tags(CaseLabel.L1)
    def test_grep_interleaved_inserts_and_paging(self):
        """Interleaved inserts must page correctly with Strong consistency."""
        client = self._client()
        name = self._create(client)
        rows = []
        for batch in range(3):
            incoming = [
                self._row(pk, f"timeout document-{pk}" if batch != 0 and pk % 5 == 0 else "skip", pk)
                for pk in range(batch, 96, 3)
            ]
            self.insert(client, name, list(reversed(incoming)))
            rows.extend(incoming)
        self._assert_rows(self._query(client, name, limit=100), rows)
        for limit, offset in [(1, 0), (7, 1), (7, 10), (8, 90)]:
            result = self._query(client, name, limit=limit, offset=offset)
            self._assert_rows(result, rows, limit=limit, offset=offset)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("text_type", [DataType.VARCHAR, DataType.TEXT])
    def test_grep_upsert_delete_visibility(self, text_type):
        """New versions change match eligibility, details and ordinary fields together."""
        client = self._client()
        name = self._create(client, text_type)
        initial = [self._row(pk, "skip" if pk == 20 else f"timeout old-{pk}") for pk in [10, 20, 30, 40, 50]]
        self.insert(client, name, initial)
        updates = [
            self._row(10, "no longer matches", 1),
            self._row(20, "timeout newly matching", 2),
            self._row(30, "超时 timeout new details", 3),
        ]
        self.upsert(client, name, updates)
        self.delete(client, name, ids=[40])
        current = updates + [initial[-1]]
        # Establish ordinary Query visibility before checking Grep selection.
        ordinary, _ = self.query(
            client, name, filter="revision >= 0", output_fields=self.output_fields, limit=20, consistency_level="Strong"
        )
        assert sorted(ordinary, key=lambda row: row["id"]) == sorted(current, key=lambda row: row["id"])
        self._assert_rows(self._query(client, name, limit=20), current)
        self._assert_rows(self._query(client, name, limit=1, offset=1), current, limit=1, offset=1)

    @pytest.mark.tags(CaseLabel.L1)
    def test_grep_empty_collection(self):
        client = self._client()
        name = self._create(client)
        assert self._query(client, name, limit=10) == []

    @pytest.mark.tags(CaseLabel.L2)
    def test_grep_concurrent_requests(self):
        """Different patterns, caps and output names must not share request state."""
        client = self._client()
        name = self._create(client)
        rows = [self._row(i, f"{'timeout' if i % 2 else 'warning'} row-{i}", i) for i in range(32)]
        self.insert(client, name, rows)

        def check(index):
            pattern = "timeout" if index % 2 else "warning"
            limit = index % 5 + 1
            output = f"hits_{index}"
            result = client.query(
                name,
                filter="revision >= 0",
                output_fields=self.output_fields,
                function_chains=[_grep_chain(f"-F -e {pattern}", output)],
                limit=limit,
                consistency_level="Strong",
                timeout=120,
            )
            self._assert_rows(result, rows, pattern=pattern, limit=limit, output=output)

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(check, range(32)))

    @pytest.mark.tags(CaseLabel.L2)
    @pytest.mark.parametrize(
        "arguments,reason,code",
        [
            ("-C 1 -e timeout", "unsupported grep option", 1100),
            ("timeout", "requires -e/--regexp", 1100),
            ("-e", "requires a pattern", 1101),
            ("", "requires at least one", 1101),
            ("-m -e timeout", "unsigned decimal integer", 1100),
            ("-e timeout -m", "requires a count", 1101),
            ("-m -1 -e timeout", "unsigned decimal integer", 1100),
            ("--max-count=abc -e timeout", "unsigned decimal integer", 1100),
            ("--max-count=18446744073709551616 -e timeout", "unsigned decimal integer", 1100),
            ('-e "timeout', "unclosed quote", 1100),
            ("-w -x -e timeout", "cannot be combined", 1100),
            ("-e timeout -- file.txt", "positional arguments", 1100),
            ('-e "["', "invalid regex", 2042),
            (r'-e "(?=timeout)"', "invalid regex", 2042),
            ('-e "foo\nbar"', "literal LF", 1100),
            ('-e "foo\x00bar"', "NUL", 1100),
        ],
    )
    def test_grep_invalid_arguments_and_recovery(self, arguments, reason, code):
        client = self._client()
        name = self._create(client)
        rows = [self._row(1, "timeout")]
        self.insert(client, name, rows)
        # Invoke the public API directly so a wrapper cannot treat an error as
        # a successful response. Match server parser diagnostics, not any error.
        with pytest.raises(MilvusException, match=reason) as error:
            client.query(
                name,
                consistency_level="Strong",
                filter="revision >= 0",
                output_fields=["id"],
                function_chains=[_grep_chain(arguments)],
                limit=10,
                timeout=120,
            )
        self._assert_input_error(error.value, code)
        self._assert_rows(self._query(client, name, limit=10), rows)

    @staticmethod
    def _assert_input_error(error, code):
        assert not isinstance(error, ParamError), "Expected a server rejection, not SDK validation"
        assert error.code == code, error
        assert error.is_input_error, error
        assert not error.retriable, error

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("text_type", [DataType.VARCHAR, DataType.TEXT])
    def test_grep_max_count(self, text_type):
        """Max-count caps matching lines per record independently of Query paging."""
        client = self._client()
        name = self._create(client, text_type)
        rows = [
            self._row(0, "skip\ntimeout timeout\ntimeout last"),
            self._row(1, "timeout first\nskip\ntimeout last", 1),
            self._row(2, "skip", 2),
            self._row(3, None, 3),
            self._row(4, "timeout final", 4),
        ]
        self.insert(client, name, rows)
        for option, max_count in [("-m 1", 1), ("--max-count=1", 1), ("-m 2", 2), ("-m 0", 0)]:
            expected = []
            for row in rows:
                details = _literal_details(row["text"])[:max_count]
                if details:
                    expected.append(dict(row, matches=details))
            for limit, offset in [(10, 0), (1, 0), (1, 1), (2, 2)]:
                actual = self._query(client, name, arguments=f"{option} -F -e timeout", limit=limit, offset=offset)
                assert list(actual) == expected[offset : offset + limit], (option, limit, offset)
            actual = self._query(client, name, arguments=f"{option} -F -e timeout", export=False, limit=10)
            assert list(actual) == [{key: value for key, value in row.items() if key != "matches"} for row in expected]

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("text_type", [DataType.VARCHAR, DataType.TEXT])
    def test_grep_matching_semantics(self, text_type):
        """Explicit expectations avoid Python/Rust regex semantic differences."""
        client = self._client()
        name = self._create(client, text_type)
        cases = [
            (r'-e "\d+"', "id=42 code=7", [3, 11], [5, 12]),
            ("-e timeout -e warning", "timeout warning", [0, 8], [7, 15]),
            ("-i -e timeout", "TIMEOUT timeout", [0, 8], [7, 15]),
            ("-i -s -e timeout", "TIMEOUT timeout", [8], [15]),
            ("-s -i -e timeout", "TIMEOUT timeout", [0, 8], [7, 15]),
            ("-w -e foo", "foo food seafood", [0], [3]),
            ("-x -e foo", "foo", [0], [3]),
            ("-x -e foo", "foo food", [], []),
            ("-F -e 'foo.bar'", "foo.bar fooXbar", [0], [7]),
            (r"""-F -e 'a\b "c"' """, 'a\\b "c"', [0], [7]),
            (r'-e "\bfoo\b"', "food foo", [5], [8]),
            ("--ignore-case --regexp=timeout", "TIMEOUT", [0], [7]),
        ]
        rows = [self._row(i, text, i) for i, (_, text, _, _) in enumerate(cases)]
        self.insert(client, name, rows)
        for i, (arguments, text, starts, ends) in enumerate(cases):
            actual = self._query(
                client,
                name,
                arguments=arguments,
                filter=f"revision == {i}",
                output_fields=["id"],
                limit=10,
            )
            expected = (
                [
                    {
                        "id": i,
                        "matches": [
                            {
                                "content": text,
                                "line_number": 1,
                                "line_start_offset": 0,
                                "starts": starts,
                                "ends": ends,
                            }
                        ],
                    }
                ]
                if starts
                else []
            )
            if list(actual) != expected:
                ordinary = client.query(
                    name,
                    filter=f"revision == {i}",
                    output_fields=["id", "text"],
                    consistency_level="Strong",
                    timeout=120,
                )
                pytest.fail(
                    f"arguments={arguments!r}, expected={expected!r}, "
                    f"actual={list(actual)!r}, ordinary={list(ordinary)!r}"
                )

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("text_type", [DataType.VARCHAR, DataType.TEXT])
    def test_grep_empty_and_zero_width_patterns(self, text_type):
        client = self._client()
        name = self._create(client, text_type)
        texts = [None, "", "\n", "é中\n", "\n\n", "x\r\n"]
        self.insert(client, name, [self._row(i, text) for i, text in enumerate(texts)])
        empty_line = {"content": "", "line_number": 1, "line_start_offset": 0, "starts": [0], "ends": [0]}
        second_empty_line = dict(empty_line, line_number=2, line_start_offset=1)
        blank = [{"id": 2, "matches": [empty_line]}, {"id": 4, "matches": [empty_line, second_empty_line]}]
        empty_pattern = [
            blank[0],
            {
                "id": 3,
                "matches": [
                    {"content": "é中", "line_number": 1, "line_start_offset": 0, "starts": [0, 2, 5], "ends": [0, 2, 5]}
                ],
            },
            blank[1],
            {
                "id": 5,
                "matches": [
                    {"content": "x\r", "line_number": 1, "line_start_offset": 0, "starts": [0, 1, 2], "ends": [0, 1, 2]}
                ],
            },
        ]
        for arguments, expected in [('-e ""', empty_pattern), ('-e "^$"', blank)]:
            actual = self._query(client, name, arguments=arguments, output_fields=["id"], limit=10)
            assert list(actual) == expected, (arguments, actual)

    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("text_type", [DataType.VARCHAR, DataType.TEXT])
    def test_grep_sparse_matches_and_paging(self, text_type):
        """Sparse matches must page correctly regardless of internal data layout."""
        client = self._client()
        name = self._create(client, text_type)
        hits = {128, 255, 256, 383, 384}
        rows = [
            self._row(i, f"timeout row-{i}" if i in hits else (None if i % 7 == 0 else "skip"), i) for i in range(388)
        ]
        self.insert(client, name, rows)
        for limit, offset in [(1, 0), (2, 1), (3, 2), (10, 0)]:
            actual = self._query(client, name, limit=limit, offset=offset)
            self._assert_rows(actual, rows, limit=limit, offset=offset)

    @pytest.mark.tags(CaseLabel.L1)
    def test_grep_large_text_and_projection(self):
        """Check large TEXT values and projections through the public Query API."""
        text_size = 65536
        client = self._client()
        name = self._create(client, DataType.TEXT)
        rows = [
            self._row(0, None),
            self._row(1, "timeout short text", 1),
            self._row(2, "x" * (text_size + 1024) + "\n超时 timeout at end", 2),
            self._row(3, "timeout first\n" + "y" * (text_size + 2048) + "\ntimeout last", 3),
            self._row(4, "z" * (text_size + 4096), 4),
        ]
        self.insert(client, name, rows)
        self._assert_rows(self._query(client, name, limit=10), rows)
        for fields in [["id"], ["id", "revision", "note"]]:
            for export in [False, True]:
                actual = self._query(client, name, output_fields=fields, export=export, limit=2, offset=1)
                expected = []
                for row in rows[2:4]:
                    value = {field: row[field] for field in fields}
                    if export:
                        value["matches"] = _literal_details(row["text"])
                    expected.append(value)
                assert list(actual) == expected

    @pytest.mark.tags(CaseLabel.L2)
    def test_grep_invalid_fields_and_chains(self):
        client = self._client()
        name = self._create(client)
        rows = [self._row(1, "timeout")]
        self.insert(client, name, rows)
        cases = []
        for field in ["missing", "revision", "metadata", "vector"]:
            chain = FunctionChain(FunctionChainStage.QUERY_SCAN, name="scan", output_columns=["matches"])
            cases.append((chain.grep(field, arguments="-e timeout"), "must be a schema VarChar or Text field"))
        for output in ["id", "text", "$id"]:
            cases.append((_grep_chain(output=output), "conflicts|overwrite|invalid grep field"))
        cases.append((_grep_chain().grep("text", arguments="-e timeout", output="second"), "exactly one grep"))
        unknown_export = FunctionChain(FunctionChainStage.QUERY_SCAN, name="scan", output_columns=["missing"])
        cases.append((unknown_export.grep("text", arguments="-e timeout"), "not produced"))
        for chain, reason in cases:
            with pytest.raises(MilvusException, match=reason) as error:
                client.query(
                    name,
                    filter="revision >= 0",
                    output_fields=["id"],
                    function_chains=chain,
                    limit=10,
                    timeout=120,
                    consistency_level="Strong",
                )
            self._assert_input_error(error.value, 1100)
        self._assert_rows(self._query(client, name, limit=10), rows)

    @pytest.mark.tags(CaseLabel.L2)
    def test_grep_unsupported_query_combinations(self):
        client = self._client()
        name = self._create(client)
        rows = [self._row(1, "timeout")]
        self.insert(client, name, rows)
        with pytest.raises(MilvusException, match="do not support count") as error:
            client.query(
                name,
                filter="revision >= 0",
                output_fields=["count(*)"],
                function_chains=_grep_chain(),
                timeout=120,
                consistency_level="Strong",
            )
        self._assert_input_error(error.value, 1100)
        with pytest.raises(MilvusException, match="iterator") as error:
            iterator = client.query_iterator(
                name,
                consistency_level="Strong",
                filter="revision >= 0",
                output_fields=["id"],
                function_chains=_grep_chain(),
                batch_size=1,
                timeout=120,
            )
            try:
                iterator.next()
            finally:
                iterator.close()
        self._assert_input_error(error.value, 1100)
        # These are deliberately SDK checks, distinct from server Status errors.
        for chains, reason in [
            ([_grep_chain(), _grep_chain()], "at most one"),
            ([FunctionChain(FunctionChainStage.QUERY_SCAN)], "at least one op"),
            ([FunctionChain(FunctionChainStage.L0_RERANK)], "require QUERY_SCAN"),
        ]:
            with pytest.raises(ParamError, match=reason):
                client.query(
                    name,
                    filter="revision >= 0",
                    output_fields=["id"],
                    function_chains=chains,
                    limit=10,
                    timeout=120,
                    consistency_level="Strong",
                )
        self._assert_rows(self._query(client, name, limit=10), rows)

    @pytest.mark.tags(CaseLabel.L1)
    def test_grep_partitions_and_scalar_filter(self):
        client = self._client()
        name = self._create(client)
        parts = ["grep_left", "grep_right"]
        by_partition = {}
        for part_index, partition in enumerate(parts):
            self.create_partition(client, name, partition)
            rows = [
                self._row(i, f"timeout partition-{partition}" if i % 3 else "skip", i) for i in range(part_index, 32, 2)
            ]
            by_partition[partition] = rows
            self.insert(client, name, rows, partition_name=partition)
        for partitions in [[parts[0]], [parts[1]], parts]:
            selected = [row for partition in partitions for row in by_partition[partition] if row["revision"] >= 10]
            result = self._query(
                client,
                name,
                partition_names=partitions,
                filter="revision >= {lower}",
                filter_params={"lower": 10},
                limit=4,
                offset=1,
            )
            self._assert_rows(result, selected, limit=4, offset=1)
        # Exercise the public convenience API with the same projection and paging.
        result = client.grep(
            name,
            "text",
            "-F -e timeout",
            filter="revision >= 10",
            output_fields=self.output_fields,
            partition_names=[parts[0]],
            match_field="hits",
            limit=4,
            offset=1,
            consistency_level="Strong",
            timeout=120,
        )
        self._assert_rows(
            result, [row for row in by_partition[parts[0]] if row["revision"] >= 10], limit=4, offset=1, output="hits"
        )
