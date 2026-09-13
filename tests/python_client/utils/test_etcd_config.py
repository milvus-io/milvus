from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from utils.etcd_config import MilvusEtcdConfigController


def committed_response(revision):
    from pyetcd.etcdrpc import PutResponse, ResponseHeader, ResponseOp

    return True, [ResponseOp(response_put=PutResponse(header=ResponseHeader(revision=revision)))]


def test_real_pyetcd_transaction_decoder_preserves_put_revision(monkeypatch):
    import pyetcd
    from pyetcd.etcdrpc import TxnResponse

    client = pyetcd.client(host="localhost")
    try:
        # Exercise the real SDK request/response adapter without contacting etcd.
        txn = Mock(return_value=TxnResponse(succeeded=True, responses=committed_response(7)[1]))
        monkeypatch.setattr(client.kvstub, "Txn", txn)
        controller = MilvusEtcdConfigController(host="localhost", client=client)
        monkeypatch.setattr(
            controller,
            "_read_path",
            Mock(
                side_effect=[
                    SimpleNamespace(exists=False, value=None, mod_revision=None),
                    RuntimeError("readback unavailable"),
                ]
            ),
        )
        with pytest.raises(RuntimeError, match="readback unavailable"):
            controller.set_config("storage.enableV3", "true")
        assert controller._owned_revisions[controller.config_path("storage.enableV3")] == 7
        request = txn.call_args.args[0]
        assert request.success[0].request_put.value == b"true"
        assert not request.failure
    finally:
        client.close()


@pytest.mark.parametrize("settle_after_write", [False, True])
def test_set_config_optionally_waits_after_verified_write(monkeypatch, settle_after_write):
    client = Mock()
    client.get.side_effect = [
        (None, None),
        (b"true", SimpleNamespace(mod_revision=7)),
    ]
    client.transaction.return_value = committed_response(7)
    controller = MilvusEtcdConfigController(host="localhost", client=client)
    path = controller.config_path("storage.enableV3")

    def check_settled_after_commit(seconds):
        assert seconds == 10
        assert controller._owned_revisions[path] == 7

    sleep = Mock(side_effect=check_settled_after_commit)
    monkeypatch.setattr("utils.etcd_config.time.sleep", sleep)

    result = controller.set_config("storage.enableV3", "true", settle_after_write=settle_after_write)

    assert result.value == b"true"
    assert result.mod_revision == 7
    assert sleep.call_count == int(settle_after_write)


def test_set_config_does_not_wait_if_read_back_fails(monkeypatch):
    client = Mock()
    client.get.side_effect = [
        (None, None),
        (b"false", SimpleNamespace(mod_revision=7)),
    ]
    client.transaction.return_value = committed_response(7)
    controller = MilvusEtcdConfigController(host="localhost", client=client)
    sleep = Mock()
    monkeypatch.setattr("utils.etcd_config.time.sleep", sleep)

    with pytest.raises(RuntimeError, match="not visible after commit"):
        controller.set_config("storage.enableV3", "true", settle_after_write=True)

    sleep.assert_not_called()


@pytest.mark.parametrize("original", [None, b"false"])
@pytest.mark.parametrize("repeated", [False, True])
def test_committed_write_is_restored_after_readback_failure(original, repeated):
    client = Mock()
    initial = (original, None if original is None else SimpleNamespace(mod_revision=1))
    committed = (b"true", SimpleNamespace(mod_revision=7))
    latest = (b"false", SimpleNamespace(mod_revision=8)) if repeated else committed
    reads = [initial, initial]
    transactions = [committed_response(7)]
    if repeated:
        reads += [committed, committed]
        transactions.append(committed_response(8))
    reads += [RuntimeError("readback unavailable"), latest, initial]
    client.get.side_effect = reads
    client.transaction.side_effect = transactions + [(True, [])]
    controller = MilvusEtcdConfigController(host="localhost", client=client)

    with pytest.raises(RuntimeError, match="readback unavailable"):
        with controller.preserve_config("storage.enableV3"):
            controller.set_config("storage.enableV3", "true")
            if repeated:
                controller.set_config("storage.enableV3", "false")

    assert client.transaction.call_count == 2 + int(repeated)
    path = controller.config_path("storage.enableV3")
    client.transactions.mod.assert_called_with(path)
    if original is None:
        client.transactions.delete.assert_called_once_with(path)
    else:
        assert client.transactions.put.call_args.args == (path, original)
    assert not controller._owned_revisions


def test_readback_does_not_claim_external_same_value_revision():
    client = Mock()
    client.get.side_effect = [(None, None), (b"true", SimpleNamespace(mod_revision=8))]
    client.transaction.return_value = committed_response(7)
    controller = MilvusEtcdConfigController(host="localhost", client=client)
    with pytest.raises(RuntimeError, match="not visible after commit"):
        controller.set_config("storage.enableV3", "true")
    assert controller._owned_revisions[controller.config_path("storage.enableV3")] == 7


def test_restore_refuses_external_write_after_readback_failure():
    client = Mock()
    client.get.side_effect = [
        (None, None),
        (None, None),
        RuntimeError("readback unavailable"),
        (b"true", SimpleNamespace(mod_revision=8)),
    ]
    client.transaction.return_value = committed_response(7)
    controller = MilvusEtcdConfigController(host="localhost", client=client)
    with pytest.raises(RuntimeError, match="Refusing to overwrite"):
        with controller.preserve_config("storage.enableV3"):
            controller.set_config("storage.enableV3", "true")
    assert client.transaction.call_count == 1


@pytest.mark.parametrize("transaction_error", [False, True])
def test_unconfirmed_write_does_not_claim_revision(transaction_error):
    client = Mock()
    client.get.return_value = (None, None)
    if transaction_error:
        client.transaction.side_effect = RuntimeError("transaction timeout")
    else:
        client.transaction.return_value = (False, [])
    controller = MilvusEtcdConfigController(host="localhost", client=client)
    with pytest.raises(RuntimeError):
        controller.set_config("storage.enableV3", "true")
    assert not controller._owned_revisions
