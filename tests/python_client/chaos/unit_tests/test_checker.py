import pytest
from chaos import checker as checker_module


class FakeMilvusClient:
    def __init__(
        self,
        indexed_fields=(),
        create_index_error=None,
        list_indexes_error=None,
        collection_exists=True,
    ):
        self.indexed_fields = tuple(indexed_fields)
        self.create_index_error = create_index_error
        self.list_indexes_error = list_indexes_error
        self.collection_exists = collection_exists
        self.create_index_calls = 0
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
        if self.create_index_error is not None:
            raise self.create_index_error

    def create_collection(self, **kwargs):
        self.collection_exists = True

    def load_collection(self, **kwargs):
        self.load_collection_calls += 1

    def get_collection_stats(self, collection_name):
        return {"row_count": 1}


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


@pytest.mark.parametrize(
    "checker_class",
    (
        checker_module.FlushChecker,
        checker_module.AddFieldChecker,
        checker_module.AddVectorFieldChecker,
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
