import pytest
from chaos import checker as checker_module


class FakeMilvusClient:
    def __init__(
        self,
        indexed_fields=(),
        create_index_error=None,
        create_index_errors=(),
        list_indexes_error=None,
        collection_exists=True,
    ):
        self.indexed_fields = tuple(indexed_fields)
        self.create_index_error = create_index_error
        self.create_index_errors = list(create_index_errors)
        self.list_indexes_error = list_indexes_error
        self.collection_exists = collection_exists
        self.add_collection_field_calls = 0
        self.create_index_calls = 0
        self.create_index_kwargs = []
        self.create_collection_calls = 0
        self.flush_calls = 0
        self.load_collection_calls = 0

    def has_collection(self, collection_name):
        return self.collection_exists

    def describe_collection(self, collection_name):
        return {"collection_name": collection_name}

    def list_indexes(self, collection_name, **kwargs):
        if self.list_indexes_error is not None:
            raise self.list_indexes_error
        return [f"index_{field_name}" for field_name in self.indexed_fields]

    def describe_index(self, collection_name, index_name, **kwargs):
        return {"field_name": index_name.removeprefix("index_")}

    def create_index(self, **kwargs):
        self.create_index_calls += 1
        self.create_index_kwargs.append(kwargs)
        if self.create_index_errors:
            error = self.create_index_errors.pop(0)
            if error is not None:
                raise error
        if self.create_index_error is not None:
            raise self.create_index_error

    def add_collection_field(self, **kwargs):
        self.add_collection_field_calls += 1

    def create_collection(self, **kwargs):
        self.create_collection_calls += 1
        self.collection_exists = True

    def load_collection(self, **kwargs):
        self.load_collection_calls += 1

    def flush(self, **kwargs):
        self.flush_calls += 1

    def get_collection_stats(self, collection_name):
        return {"row_count": 1}

    def query(self, **kwargs):
        return [{}]


def patch_checker_constructor(monkeypatch, client, *, scalar_fields=(), float_vector_fields=()):
    schema = object()
    monkeypatch.setattr(checker_module, "MilvusClient", lambda **kwargs: client)
    monkeypatch.setattr(checker_module, "MilvusSys", lambda: object())
    monkeypatch.setattr(checker_module.connections, "connect", lambda **kwargs: None)
    monkeypatch.setattr(
        checker_module.CollectionSchema,
        "construct_from_dict",
        staticmethod(lambda collection_info: schema),
    )

    field_helpers = {
        "get_dim_by_schema": 8,
        "get_int64_field_name": "id",
        "get_text_field_name": "text",
        "get_text_match_field_name": [],
        "get_float_vec_field_name": "float_vector",
        "get_scalar_field_name_list": list(scalar_fields),
        "get_json_field_name_list": [],
        "get_geometry_field_name_list": [],
        "get_float_vec_field_name_list": list(float_vector_fields),
        "get_binary_vec_field_name_list": [],
        "get_int8_vec_field_name_list": [],
        "get_bm25_vec_field_name_list": [],
        "get_minhash_vec_field_name_list": [],
        "get_emb_list_field_name_list": [],
    }
    for helper_name, return_value in field_helpers.items():
        monkeypatch.setattr(checker_module.cf, helper_name, lambda *, schema, value=return_value: value)
    return schema


def test_checker_initialization_fails_fast_on_index_creation_error(monkeypatch):
    client = FakeMilvusClient(
        create_index_error=TimeoutError("index creation timed out"),
        collection_exists=False,
    )
    schema = patch_checker_constructor(monkeypatch, client, scalar_fields=("first_scalar", "second_scalar"))

    with pytest.raises(RuntimeError, match="first_scalar"):
        checker_module.Checker(collection_name="new_collection", schema=schema, insert_data=False)

    assert client.create_index_calls == 1


def test_checker_initialization_emits_structured_timing_events(monkeypatch):
    client = FakeMilvusClient()
    patch_checker_constructor(monkeypatch, client)
    messages = []
    monkeypatch.setattr(checker_module.log, "info", messages.append)

    checker = checker_module.Checker(collection_name="existing_collection", insert_data=False)

    assert checker.init_duration_seconds >= 0
    assert any('"event":"checker_init_start"' in message for message in messages)
    assert any('"stage":"collection_resolved"' in message for message in messages)
    assert any('"stage":"index_discovery_complete"' in message for message in messages)
    assert any('"stage":"index_setup_complete"' in message for message in messages)
    assert any('"event":"checker_base_init_complete"' in message for message in messages)


def test_checker_flushes_initial_seed_data_before_other_checkers_reuse_collection(monkeypatch):
    client = FakeMilvusClient(collection_exists=False)
    schema = patch_checker_constructor(monkeypatch, client)
    row_counts = iter((0, 3000))
    monkeypatch.setattr(client, "get_collection_stats", lambda collection_name: {"row_count": next(row_counts)})
    insert_calls = []
    monkeypatch.setattr(
        checker_module.Checker,
        "insert_data",
        lambda self, **kwargs: insert_calls.append(kwargs) or (None, True),
    )

    checker = checker_module.Checker(collection_name="new_collection", schema=schema)

    assert len(insert_calls) == 1
    assert client.flush_calls == 1
    assert checker.initial_entities == 3000


def test_trace_tracks_in_flight_and_last_operation_state():
    checker = object.__new__(checker_module.Checker)
    checker.c_name = "trace_collection"
    checker.rsp_times = []
    checker.average_time = 0
    checker._succ = 0
    checker._fail = 0
    checker.fail_records = []
    seen = {}

    @checker_module.trace(flag=False)
    def operation(self):
        seen["current_operation"] = self.current_operation
        seen["started"] = self.current_operation_started_at
        return None, True

    assert operation(checker) == (None, True)
    assert seen["current_operation"] == "operation"
    assert seen["started"] is not None
    assert checker.current_operation is None
    assert checker.current_operation_started_at is None
    assert checker.last_operation == "operation"
    assert checker.last_operation_result == "success"
    assert checker.last_operation_elapsed >= 0


def test_exception_handler_logs_one_line_without_traceback(monkeypatch):
    checker = object.__new__(checker_module.Checker)
    checker.c_name = "error_collection"
    checker.error_messages = set()
    checker.error_message_samples = {}
    error_messages = []
    exception_messages = []
    monkeypatch.setattr(checker_module, "enable_traceback", False)
    monkeypatch.setattr(checker_module.log, "error", error_messages.append)
    monkeypatch.setattr(checker_module.log, "exception", exception_messages.append)

    @checker_module.exception_handler()
    def operation(self):
        raise RuntimeError("expected test failure")

    _, result = operation(checker)

    assert result is False
    assert len(error_messages) == 1
    assert '"event":"operation_exception"' in error_messages[0]
    assert exception_messages == []


def test_checker_initialization_skips_runtime_added_fields(monkeypatch):
    client = FakeMilvusClient(indexed_fields=("base_scalar", "base_vector"))
    patch_checker_constructor(
        monkeypatch,
        client,
        scalar_fields=("base_scalar", "new_field_generated"),
        float_vector_fields=("base_vector", "new_vec_generated"),
    )

    checker_module.Checker(collection_name="existing_collection", insert_data=False)

    assert client.create_index_calls == 0
    assert client.load_collection_calls == 1


def test_checker_initialization_does_not_recreate_indexes_for_existing_collection(monkeypatch):
    client = FakeMilvusClient()
    patch_checker_constructor(
        monkeypatch,
        client,
        scalar_fields=("base_scalar",),
        float_vector_fields=("base_vector",),
    )

    checker_module.Checker(collection_name="existing_collection", insert_data=False)

    assert client.create_index_calls == 0
    assert client.load_collection_calls == 0


def test_checker_initialization_fails_fast_when_indexes_cannot_be_listed(monkeypatch):
    client = FakeMilvusClient(list_indexes_error=TimeoutError("index discovery timed out"))
    patch_checker_constructor(monkeypatch, client, scalar_fields=("base_scalar",))

    with pytest.raises(RuntimeError, match="Failed to list indexes"):
        checker_module.Checker(collection_name="existing_collection", insert_data=False)

    assert client.create_index_calls == 0


def test_query_checker_reuses_primary_vector_index(monkeypatch):
    client = FakeMilvusClient(indexed_fields=("base_vector",))
    patch_checker_constructor(monkeypatch, client, float_vector_fields=("base_vector",))
    monkeypatch.setattr(checker_module.Checker, "insert_data", lambda self: None)

    checker_module.QueryChecker(collection_name="existing_collection")

    assert client.create_index_calls == 0


def test_add_vector_field_checker_retries_until_first_success(monkeypatch):
    client = FakeMilvusClient(indexed_fields=("base_vector",))
    patch_checker_constructor(monkeypatch, client, float_vector_fields=("base_vector",))
    checker = checker_module.AddVectorFieldChecker(collection_name="existing_collection")
    attempts = []
    attempt_results = iter((("server unavailable", False), (None, True)))

    def attempt_once():
        attempts.append(1)
        return next(attempt_results)

    monkeypatch.setattr(checker, "_add_vector_field_once", attempt_once, raising=False)

    first_result = checker.add_vector_field()
    second_result = checker.add_vector_field()
    third_result = checker.add_vector_field()

    assert attempts == [1, 1]
    assert first_result == ("server unavailable", False)
    assert second_result == third_result == (None, True)


def test_add_vector_field_checker_retries_same_field_after_index_timeout(monkeypatch):
    client = FakeMilvusClient(
        indexed_fields=("base_vector",),
        create_index_errors=(TimeoutError("index creation timed out"), None),
    )
    patch_checker_constructor(monkeypatch, client, float_vector_fields=("base_vector",))
    checker = checker_module.AddVectorFieldChecker(collection_name="existing_collection")
    monkeypatch.setattr(checker_module.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(checker, "insert_data", lambda: (None, True))

    first_result = checker.add_vector_field()
    second_result = checker.add_vector_field()
    third_result = checker.add_vector_field()

    assert first_result[1] is False
    assert second_result == third_result == (None, True)
    assert client.add_collection_field_calls == 1
    assert client.create_index_calls == 2
    index_param = client.create_index_kwargs[-1]["index_params"][0].to_dict()
    assert index_param["index_type"] == "FLAT"
    assert index_param["metric_type"] == "COSINE"
    assert client.create_index_kwargs[-1]["sync"] is False


@pytest.mark.parametrize(
    "checker_class",
    (
        checker_module.FlushChecker,
        checker_module.AddFieldChecker,
        checker_module.AddVectorFieldChecker,
        checker_module.CollectionDropChecker,
        checker_module.SnapshotChecker,
        checker_module.SnapshotRestoreChecker,
    ),
)
def test_heavy_checker_waits_between_operations(monkeypatch, checker_class):
    checker = object.__new__(checker_class)
    checker.c_name = "schedule_test_collection"
    checker._keep_running = True
    checker.configure_operation_schedule(
        interval_seconds=checker_module.HEAVY_OP_WAIT_SECONDS,
        initial_jitter_seconds=checker_module.HEAVY_OP_WAIT_SECONDS,
    )

    def run_once():
        checker._keep_running = False

    checker.run_task = run_once
    wait_calls = []
    monkeypatch.setattr(
        checker_module,
        "_wait_for_next_operation",
        lambda checker, seconds: wait_calls.append(seconds),
    )
    monkeypatch.setattr(checker_module, "_get_initial_operation_jitter", lambda checker, seconds: 17)

    checker.keep_running()

    assert wait_calls == [17, checker_module.HEAVY_OP_WAIT_SECONDS]


def test_collection_drop_checker_uses_bounded_pool(monkeypatch):
    checker = object.__new__(checker_module.CollectionDropChecker)
    checker.c_name = "drop_checker"
    checker.milvus_client = FakeMilvusClient()
    checker.collection_pool = []
    generated_names = iter(f"drop_pool_{index}" for index in range(checker_module.DROP_COLLECTION_POOL_SIZE))
    monkeypatch.setattr(checker_module.cf, "gen_unique_str", lambda prefix: next(generated_names))

    checker.gen_collection_pool(schema=object())

    assert len(checker.collection_pool) == checker_module.DROP_COLLECTION_POOL_SIZE
    assert checker.milvus_client.create_collection_calls == checker_module.DROP_COLLECTION_POOL_SIZE


def test_initial_operation_jitter_is_stable_and_operation_specific():
    add_vector = object.__new__(checker_module.AddVectorFieldChecker)
    add_vector.c_name = "shared_collection"
    flush = object.__new__(checker_module.FlushChecker)
    flush.c_name = "shared_collection"

    first = checker_module._get_initial_operation_jitter(add_vector, 120)
    second = checker_module._get_initial_operation_jitter(add_vector, 120)
    flush_jitter = checker_module._get_initial_operation_jitter(flush, 120)

    assert first == second
    assert 0 <= first < 120
    assert first != flush_jitter


def test_initial_operation_jitter_uses_distinct_xdist_worker_slots(monkeypatch):
    checker = object.__new__(checker_module.AddVectorFieldChecker)
    checker.c_name = "shared_collection"
    monkeypatch.setenv("PYTEST_XDIST_WORKER_COUNT", "5")

    delays = []
    for worker_index in range(5):
        monkeypatch.setenv("PYTEST_XDIST_WORKER", f"gw{worker_index}")
        delays.append(checker_module._get_initial_operation_jitter(checker, 120))

    assert len(set(delays)) == 5
    assert all(right - left == 24 for left, right in zip(delays, delays[1:]))


def test_configure_heavy_operation_schedules():
    checkers = {
        op: object.__new__(checker_class)
        for op, checker_class in (
            (checker_module.Op.flush, checker_module.FlushChecker),
            (checker_module.Op.drop, checker_module.CollectionDropChecker),
            (checker_module.Op.add_field, checker_module.AddFieldChecker),
            (checker_module.Op.snapshot, checker_module.SnapshotChecker),
            (checker_module.Op.restore_snapshot, checker_module.SnapshotRestoreChecker),
            (checker_module.Op.add_vector_field, checker_module.AddVectorFieldChecker),
        )
    }
    insert_checker = object()
    checkers[checker_module.Op.insert] = insert_checker

    checker_module.configure_heavy_operation_schedules(checkers)

    for operation, checker in checkers.items():
        if operation == checker_module.Op.insert:
            assert checker is insert_checker
            continue
        assert checker.operation_interval_seconds == checker_module.HEAVY_OP_WAIT_SECONDS
        assert checker.initial_jitter_seconds == checker_module.HEAVY_OP_WAIT_SECONDS
